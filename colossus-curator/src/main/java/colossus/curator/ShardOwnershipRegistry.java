package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Condition;
import colossus.bigtable.Mutation;
import colossus.common.ColumnKey;
import colossus.common.CuratorId;
import colossus.common.RowKey;
import colossus.common.WireReader;
import colossus.common.WireWriter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The mapping (namespace prefix → owning Curator) stored in BigTable as {@code curator.shards.*}
 * rows. This is the load-bearing failover primitive: a Curator dying does not stall the namespace
 * because ownership is reassigned by a single row CAS, not by log replay. The owner heartbeats every
 * few seconds; an idle Curator that sees a stale heartbeat claims the shard by CAS, and the loser of
 * a race simply fails the CAS.
 */
public final class ShardOwnershipRegistry {

    private static final String ROW_PREFIX = "curator.shards.";
    private static final ColumnKey OWNER = ColumnKey.of("owner", "id");
    private static final ColumnKey HEARTBEAT = ColumnKey.of("hb", "ts");

    private final BigTableClient bt;
    private long ts = 0;

    public ShardOwnershipRegistry(BigTableClient bt) {
        this.bt = bt;
    }

    private RowKey rowKey(String prefix) {
        return RowKey.of(ROW_PREFIX + prefix);
    }

    private static byte[] millis(Instant t) {
        return new WireWriter().i64(t.toEpochMilli()).toByteArray();
    }

    private static Instant toInstant(byte[] b) {
        return Instant.ofEpochMilli(new WireReader(b).i64());
    }

    public Optional<CuratorId> ownerOf(String prefix) {
        return bt.get(rowKey(prefix), OWNER)
                .map(b -> new CuratorId(new String(b, StandardCharsets.UTF_8)));
    }

    public Optional<Instant> lastHeartbeat(String prefix) {
        return bt.get(rowKey(prefix), HEARTBEAT).map(ShardOwnershipRegistry::toInstant);
    }

    /** Explicit bootstrap claim (unconditional) — used when a shard is first assigned. */
    public synchronized void assumeOwnership(String prefix, CuratorId curator, Instant now) {
        RowKey row = rowKey(prefix);
        long t = ++ts;
        bt.checkAndMutate(row, List.of(), List.of(
                Mutation.put(OWNER, t, curator.hostPort().getBytes(StandardCharsets.UTF_8)),
                Mutation.put(HEARTBEAT, t, millis(now))));
    }

    /** Owner refreshes its heartbeat; returns false if it has lost ownership (a CAS miss). */
    public synchronized boolean heartbeat(String prefix, CuratorId curator, Instant now) {
        RowKey row = rowKey(prefix);
        long t = ++ts;
        return bt.checkAndMutate(row,
                List.of(Condition.expectValue(OWNER, curator.hostPort().getBytes(StandardCharsets.UTF_8))),
                List.of(Mutation.put(HEARTBEAT, t, millis(now))));
    }

    /**
     * Claim a shard if it is unowned or its heartbeat is older than {@code staleThresholdSeconds}.
     * The claim is a CAS on the current (owner, heartbeat) pair, so a concurrent claimer loses.
     */
    public synchronized boolean claimIfStale(String prefix, CuratorId claimant, Instant now,
                                             long staleThresholdSeconds) {
        RowKey row = rowKey(prefix);
        Optional<CuratorId> owner = ownerOf(prefix);
        long t = ++ts;
        byte[] claimantBytes = claimant.hostPort().getBytes(StandardCharsets.UTF_8);

        if (owner.isEmpty()) {
            return bt.checkAndMutate(row,
                    List.of(Condition.expectAbsent(OWNER)),
                    List.of(Mutation.put(OWNER, t, claimantBytes), Mutation.put(HEARTBEAT, t, millis(now))));
        }
        Optional<Instant> hb = lastHeartbeat(prefix);
        boolean stale = hb.isEmpty()
                || now.isAfter(hb.get().plusSeconds(staleThresholdSeconds));
        if (!stale) {
            return false;
        }
        // CAS against the exact stale state we observed.
        List<Condition> conds = new ArrayList<>();
        conds.add(Condition.expectValue(OWNER, owner.get().hostPort().getBytes(StandardCharsets.UTF_8)));
        hb.ifPresent(instant -> conds.add(Condition.expectValue(HEARTBEAT, millis(instant))));
        return bt.checkAndMutate(row, conds,
                List.of(Mutation.put(OWNER, t, claimantBytes), Mutation.put(HEARTBEAT, t, millis(now))));
    }

    /** Prefixes whose heartbeat is stale (candidates for reassignment). */
    public List<String> staleShards(Instant now, long staleThresholdSeconds) {
        List<String> out = new ArrayList<>();
        for (var e : bt.scan(ROW_PREFIX).entrySet()) {
            String prefix = e.getKey().asString().substring(ROW_PREFIX.length());
            Optional<Instant> hb = lastHeartbeat(prefix);
            if (hb.isEmpty() || now.isAfter(hb.get().plusSeconds(staleThresholdSeconds))) {
                out.add(prefix);
            }
        }
        return out;
    }
}
