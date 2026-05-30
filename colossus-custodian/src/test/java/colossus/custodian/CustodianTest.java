package colossus.custodian;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.DserverId;
import colossus.common.PgId;
import colossus.placement.DserverStatusTable;
import colossus.placement.PgTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CustodianTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

    private static List<DserverId> pool(int n) {
        List<DserverId> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add(DserverId.of("d" + i, 7100));
        return out;
    }

    /** Records dispatched work; can be told to report corruption for SCRUB on chosen PGs. */
    static final class RecordingDispatcher implements WorkDispatcher {
        final List<WorkItem> items = new ArrayList<>();
        final Set<Integer> corruptPgs;

        RecordingDispatcher(Set<Integer> corruptPgs) {
            this.corruptPgs = corruptPgs;
        }

        @Override
        public WorkResult dispatch(WorkItem item) {
            items.add(item);
            if (item.op() == WorkItem.Op.SCRUB) {
                return WorkResult.scrub(corruptPgs.contains(item.pg().id()));
            }
            return WorkResult.success();
        }
    }

    private record Fixture(BigTableClient bt, PgTable pgTable, DserverStatusTable status,
                           List<DserverId> pool) {}

    private Fixture fixture(Path dir) {
        BigTableClient bt = new BigTableClient(new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false));
        return new Fixture(bt, new PgTable(bt), new DserverStatusTable(bt), pool(5));
    }

    private void heartbeat(DserverStatusTable status, List<DserverId> live, Instant when, int extents) {
        for (DserverId d : live) {
            status.writeStatus(d, when, 1_000_000, extents);
        }
    }

    @Test
    void underReplicatedPgDetectedAndRepairedOnBackgroundLane(@TempDir Path dir) {
        Fixture f = fixture(dir);
        f.pgTable.assign(PgId.of(0x89), List.of(f.pool.get(0), f.pool.get(1), f.pool.get(2)), "p");

        // d0,d1 fresh; d2 stale (heartbeat 20 s old). Two survivors → routine under-replication.
        heartbeat(f.status, List.of(f.pool.get(0), f.pool.get(1), f.pool.get(3), f.pool.get(4)), t0, 5);
        heartbeat(f.status, List.of(f.pool.get(2)), t0.minusSeconds(20), 5);

        RecordingDispatcher disp = new RecordingDispatcher(Set.of());
        Custodian c = new Custodian(f.bt, f.pgTable, f.status, f.pool, disp, 3, 15, 7 * 86400, 1000);

        List<WorkItem> dispatched = c.repairTick(t0);

        assertThat(dispatched).hasSize(1);
        WorkItem item = dispatched.get(0);
        assertThat(item.op()).isEqualTo(WorkItem.Op.REPAIR_COPY);
        assertThat(item.priority()).isEqualTo(WorkItem.Priority.BACKGROUND);
        assertThat(item.target()).contains(f.pool.get(3)); // first healthy spare not in the set
        // PG table now reflects the repaired replica set with a bumped epoch.
        assertThat(f.pgTable.lookup(PgId.of(0x89)).orElseThrow().dservers()).contains(f.pool.get(3));
        assertThat(f.pgTable.lookup(PgId.of(0x89)).orElseThrow().epoch()).isEqualTo(2);
    }

    @Test
    void durabilityFloorBreachEscalatesToRepairLane(@TempDir Path dir) {
        Fixture f = fixture(dir);
        f.pgTable.assign(PgId.of(1), List.of(f.pool.get(0), f.pool.get(1), f.pool.get(2)), "p");

        // Only d0 survives (d1, d2 stale) → one replica left → durability-floor breach.
        heartbeat(f.status, List.of(f.pool.get(0), f.pool.get(3), f.pool.get(4)), t0, 5);
        heartbeat(f.status, List.of(f.pool.get(1), f.pool.get(2)), t0.minusSeconds(20), 5);

        RecordingDispatcher disp = new RecordingDispatcher(Set.of());
        Custodian c = new Custodian(f.bt, f.pgTable, f.status, f.pool, disp, 3, 15, 7 * 86400, 1000);

        List<WorkItem> dispatched = c.repairTick(t0);
        assertThat(dispatched).hasSize(1);
        assertThat(dispatched.get(0).priority()).isEqualTo(WorkItem.Priority.REPAIR); // escalated
    }

    @Test
    void healthyPgNeedsNoRepair(@TempDir Path dir) {
        Fixture f = fixture(dir);
        f.pgTable.assign(PgId.of(2), List.of(f.pool.get(0), f.pool.get(1), f.pool.get(2)), "p");
        heartbeat(f.status, f.pool, t0, 5); // everyone fresh
        RecordingDispatcher disp = new RecordingDispatcher(Set.of());
        Custodian c = new Custodian(f.bt, f.pgTable, f.status, f.pool, disp, 3, 15, 7 * 86400, 1000);
        assertThat(c.repairTick(t0)).isEmpty();
    }

    @Test
    void scrubRunsWhenOverdueAndCorruptionSchedulesRepair(@TempDir Path dir) {
        Fixture f = fixture(dir);
        f.pgTable.assign(PgId.of(3), List.of(f.pool.get(0), f.pool.get(1), f.pool.get(2)), "p");
        heartbeat(f.status, f.pool, t0, 5);

        RecordingDispatcher disp = new RecordingDispatcher(Set.of(3)); // PG 3 reports corruption
        Custodian c = new Custodian(f.bt, f.pgTable, f.status, f.pool, disp, 3, 15, 7 * 86400, 1000);

        List<WorkItem> dispatched = c.scrubTick(t0);
        assertThat(dispatched).extracting(WorkItem::op).contains(WorkItem.Op.SCRUB);
        // Corruption on PG 3 triggered a follow-up repair dispatch.
        assertThat(disp.items).anyMatch(i -> i.op() == WorkItem.Op.REPAIR_COPY && i.pg().id() == 3);

        // Second tick within the cadence window does nothing (scrub timestamp recorded).
        disp.items.clear();
        assertThat(c.scrubTick(t0.plusSeconds(60))).isEmpty();
    }

    @Test
    void rebalanceMovesExtentsOffHotDserver(@TempDir Path dir) {
        Fixture f = fixture(dir);
        // d0 is hot (1000 extents), others light (5). Threshold 100.
        f.status.writeStatus(f.pool.get(0), t0, 1_000_000, 1000);
        heartbeat(f.status, f.pool.subList(1, 5), t0, 5);

        RecordingDispatcher disp = new RecordingDispatcher(Set.of());
        Custodian c = new Custodian(f.bt, f.pgTable, f.status, f.pool, disp, 3, 15, 7 * 86400, 100);

        List<WorkItem> dispatched = c.rebalanceTick(t0);
        assertThat(dispatched).hasSize(1);
        assertThat(dispatched.get(0).op()).isEqualTo(WorkItem.Op.REBALANCE_MOVE);
        assertThat(dispatched.get(0).source()).contains(f.pool.get(0));
    }
}
