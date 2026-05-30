package colossus.custodian;

import colossus.bigtable.BigTableClient;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.PgId;
import colossus.common.RowKey;
import colossus.common.WireReader;
import colossus.common.WireWriter;
import colossus.placement.DserverStatusTable;
import colossus.placement.PgPlacement;
import colossus.placement.PgTable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The background reconciler. It is stateless and operates on <em>observed</em> cluster state read
 * directly from BigTable — the PG table and the D-server status table — never on directives from the
 * Curator (ADR 0004). It runs four loops: repair (re-replicate under-replicated PGs, escalating to
 * the REPAIR lane on a durability-floor breach), scrub (deep-verify CRCs on a cadence), rebalance
 * (migrate extents off hot D-servers), and tier-down (v1 logs the decision). Each loop is a discrete
 * tick so reconciliation is deterministically testable.
 */
public final class Custodian {

    private final BigTableClient bt;
    private final PgTable pgTable;
    private final DserverStatusTable status;
    private final List<DserverId> dserverPool;
    private final WorkDispatcher dispatcher;
    private final int replicationFactor;
    private final long staleThresholdSeconds;
    private final long scrubCadenceSeconds;
    private final int rebalanceExtentThreshold;

    private long ts = 0;

    public Custodian(BigTableClient bt, PgTable pgTable, DserverStatusTable status,
                     List<DserverId> dserverPool, WorkDispatcher dispatcher,
                     int replicationFactor, long staleThresholdSeconds,
                     long scrubCadenceSeconds, int rebalanceExtentThreshold) {
        this.bt = bt;
        this.pgTable = pgTable;
        this.status = status;
        this.dserverPool = List.copyOf(dserverPool);
        this.dispatcher = dispatcher;
        this.replicationFactor = replicationFactor;
        this.staleThresholdSeconds = staleThresholdSeconds;
        this.scrubCadenceSeconds = scrubCadenceSeconds;
        this.rebalanceExtentThreshold = rebalanceExtentThreshold;
    }

    private List<PgId> allPgs() {
        List<PgId> out = new ArrayList<>();
        for (var e : bt.scan("pg/").entrySet()) {
            // "pg/00001234" → 0x1234
            String hex = e.getKey().asString().substring("pg/".length());
            out.add(PgId.of((int) Long.parseLong(hex, 16)));
        }
        return out;
    }

    // ---- CP23 / CP24 repair loop with durability escalation ----

    /**
     * Find under-replicated PGs and dispatch repair. A PG with only one surviving replica is a
     * durability-floor breach and gets REPAIR priority (which escalates the target's dmClock lane
     * above foreground); routine under-replication gets BACKGROUND.
     */
    public List<WorkItem> repairTick(Instant now) {
        List<WorkItem> dispatched = new ArrayList<>();
        for (PgId pg : allPgs()) {
            Optional<PgPlacement> placementOpt = pgTable.lookup(pg);
            if (placementOpt.isEmpty()) {
                continue;
            }
            PgPlacement placement = placementOpt.get();
            List<DserverId> replicas = placement.dservers();
            List<DserverId> surviving = new ArrayList<>();
            for (DserverId d : replicas) {
                if (status.isUp(d, now, staleThresholdSeconds)) {
                    surviving.add(d);
                }
            }
            if (surviving.size() >= replicationFactor || surviving.isEmpty()) {
                continue; // healthy, or total loss (operator-only — out of v1 scope)
            }
            Optional<DserverId> target = pickTarget(replicas, now);
            if (target.isEmpty()) {
                continue; // no healthy spare to repair onto
            }
            WorkItem.Priority priority = surviving.size() == 1
                    ? WorkItem.Priority.REPAIR        // last replica → escalate
                    : WorkItem.Priority.BACKGROUND;   // routine under-replication
            WorkItem item = WorkItem.repair(pg, surviving.get(0), target.get(), priority);
            WorkDispatcher.WorkResult r = dispatcher.dispatch(item);
            if (r.ok()) {
                // Replace the down replica(s) with the target and bump the epoch.
                List<DserverId> newSet = new ArrayList<>(surviving);
                newSet.add(target.get());
                pgTable.assign(pg, newSet, placement.policy());
                dispatched.add(item);
            }
        }
        return dispatched;
    }

    private Optional<DserverId> pickTarget(List<DserverId> exclude, Instant now) {
        for (DserverId d : dserverPool) {
            if (!exclude.contains(d) && status.isUp(d, now, staleThresholdSeconds)) {
                return Optional.of(d);
            }
        }
        return Optional.empty();
    }

    // ---- CP23 scrub loop ----

    private ColumnKey scrubCol() {
        return ColumnKey.of("scrub", "last_completed");
    }

    public List<WorkItem> scrubTick(Instant now) {
        List<WorkItem> dispatched = new ArrayList<>();
        for (PgId pg : allPgs()) {
            Optional<PgPlacement> placement = pgTable.lookup(pg);
            if (placement.isEmpty()) {
                continue;
            }
            RowKey row = RowKey.of(pg.rowKey());
            Optional<Instant> last = bt.get(row, scrubCol())
                    .map(b -> Instant.ofEpochMilli(new WireReader(b).i64()));
            boolean due = last.isEmpty() || now.isAfter(last.get().plusSeconds(scrubCadenceSeconds));
            if (!due) {
                continue;
            }
            WorkItem item = WorkItem.scrub(pg, placement.get().primary());
            WorkDispatcher.WorkResult r = dispatcher.dispatch(item);
            if (r.corruptionFound()) {
                // A mismatch schedules a repair from a healthy peer.
                pickTarget(placement.get().dservers(), now).ifPresent(target ->
                        dispatcher.dispatch(WorkItem.repair(pg, placement.get().primary(), target,
                                WorkItem.Priority.REPAIR)));
            }
            bt.put(row, scrubCol(), ++ts, new WireWriter().i64(now.toEpochMilli()).toByteArray());
            dispatched.add(item);
        }
        return dispatched;
    }

    // ---- CP23 rebalance loop ----

    public List<WorkItem> rebalanceTick(Instant now) {
        List<WorkItem> dispatched = new ArrayList<>();
        for (DserverId d : dserverPool) {
            Optional<DserverStatusTable.DserverStatus> s = status.get(d);
            if (s.isEmpty() || s.get().numExtents() <= rebalanceExtentThreshold) {
                continue;
            }
            Optional<DserverId> target = pickLeastLoadedOther(d, now);
            if (target.isPresent()) {
                WorkItem item = WorkItem.rebalance(PgId.of(0), d, target.get());
                dispatcher.dispatch(item);
                dispatched.add(item);
            }
        }
        return dispatched;
    }

    private Optional<DserverId> pickLeastLoadedOther(DserverId hot, Instant now) {
        DserverId best = null;
        int bestExtents = Integer.MAX_VALUE;
        for (DserverId d : dserverPool) {
            if (d.equals(hot) || !status.isUp(d, now, staleThresholdSeconds)) {
                continue;
            }
            int extents = status.get(d).map(DserverStatusTable.DserverStatus::numExtents).orElse(0);
            if (extents < bestExtents) {
                bestExtents = extents;
                best = d;
            }
        }
        return Optional.ofNullable(best);
    }
}
