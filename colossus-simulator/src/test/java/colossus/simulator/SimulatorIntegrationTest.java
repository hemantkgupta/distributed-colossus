package colossus.simulator;

import colossus.client.ColossusClient;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.common.PgId;
import colossus.curator.Curator;
import colossus.custodian.Custodian;
import colossus.custodian.WorkItem;
import colossus.bigtable.Faults;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimulatorIntegrationTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final int CHUNK = 64; // tiny chunks so multi-chunk files stay test-sized

    @Test
    void basicWriteThenReadAcrossChunks(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/big.dat");
        client.create(path, "alice");
        byte[] payload = new byte[200]; // spans 4 x 64-byte chunks (the "200 MB / 4 chunks" shape)
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 7 + 1);
        client.write(path, payload);

        assertThat(client.stat(path).chunkCount()).isEqualTo(4);
        assertThat(client.read(path, 0, 200)).isEqualTo(payload);
        // A read straddling a chunk boundary reassembles correctly.
        assertThat(client.read(path, 60, 10))
                .isEqualTo(java.util.Arrays.copyOfRange(payload, 60, 70));
    }

    @Test
    void concurrentAppendersGetDistinctChunks(@TempDir Path dir) throws Exception {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();
        FilePath path = FilePath.of("/data/log");
        client.create(path, "alice");

        int appenders = 8;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(appenders);
        var indices = java.util.Collections.synchronizedList(new java.util.ArrayList<Integer>());
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < appenders; i++) {
                final byte[] b = {(byte) i};
                futures.add(pool.submit(() -> indices.add(client.append(path, b))));
            }
            for (var f : futures) f.get();
        } finally {
            pool.shutdownNow();
        }
        // The chunk-index CAS guarantees every appender got a distinct index.
        assertThat(indices).doesNotHaveDuplicates().hasSize(appenders);
        assertThat(client.stat(path).chunkCount()).isEqualTo(appenders);
    }

    @Test
    void curatorDeathDoesNotPauseNamespace(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        Curator c1 = cluster.addCurator("c1");
        Curator c2 = cluster.addCurator("c2");
        c1.assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/f");
        client.create(path, "alice");
        client.write(path, "before".getBytes());

        // c1 dies mid-stream. Another Curator picks up the shard via a single row CAS — no replay.
        cluster.killCurator(c1);
        clock.advanceSeconds(20);
        cluster.heartbeatDservers(clock.now());
        List<String> claimed = c2.claimStaleTick();
        assertThat(claimed).containsExactly("/");

        // The client transparently continues against the new owner.
        assertThat(client.read(path, 0, 6)).isEqualTo("before".getBytes());
        FilePath g = FilePath.of("/data/g");
        client.create(g, "alice");
        client.write(g, "after".getBytes());
        assertThat(client.stat(g).exists()).isTrue();
    }

    @Test
    void dserverDeathTriggersCustodianRepair(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/f");
        client.create(path, "alice");
        client.append(path, "chunk-bytes".getBytes());

        // Find the chunk's replica set and kill a secondary (not the primary the client reads from).
        var loc = cluster.curators().get(0).shardFor(path).orElseThrow().getChunkLocations(path, 0).orElseThrow();
        DserverId secondary = loc.dservers().get(2);
        PgId pg = cluster.router().placementGroupFor(loc.handle().hex());

        cluster.killDserver(secondary);
        clock.advanceSeconds(20);
        cluster.heartbeatDservers(clock.now()); // survivors + spares refresh; killed one stays stale

        Custodian custodian = cluster.custodian(1_000_000);
        List<WorkItem> dispatched = custodian.repairTick(clock.now());

        assertThat(dispatched).anyMatch(w -> w.op() == WorkItem.Op.REPAIR_COPY && w.pg().equals(pg));
        // The repair target now hosts the extent, and the data is still readable from a survivor.
        DserverId target = dispatched.stream().filter(w -> w.pg().equals(pg)).findFirst()
                .orElseThrow().target().orElseThrow();
        assertThat(cluster.dserver(target).hostedHandles()).contains(loc.handle());
        assertThat(cluster.pgTable().lookup(pg).orElseThrow().dservers()).contains(target);
        assertThat(client.read(path, 0, 11)).isEqualTo("chunk-bytes".getBytes());
    }

    @Test
    void durabilityFloorBreachEscalatesRepairLane(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/f");
        client.create(path, "alice");
        client.append(path, "x".getBytes());
        var loc = cluster.curators().get(0).shardFor(path).orElseThrow().getChunkLocations(path, 0).orElseThrow();

        // Kill TWO of three replicas — one survivor left → durability-floor breach.
        cluster.killDserver(loc.dservers().get(1));
        cluster.killDserver(loc.dservers().get(2));
        clock.advanceSeconds(20);
        cluster.heartbeatDservers(clock.now());

        Custodian custodian = cluster.custodian(1_000_000);
        List<WorkItem> dispatched = custodian.repairTick(clock.now());
        WorkItem repair = dispatched.stream()
                .filter(w -> w.op() == WorkItem.Op.REPAIR_COPY).findFirst().orElseThrow();
        assertThat(repair.priority()).isEqualTo(WorkItem.Priority.REPAIR);
        // The target's REPAIR lane was escalated above FOREGROUND so bytes outrun a third loss.
        DserverId target = repair.target().orElseThrow();
        assertThat(cluster.escalator(target).repairOutranksForeground()).isTrue();
    }

    @Test
    void tabletServerQuorumLossFailsWritesButReadsContinue(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/f");
        client.create(path, "alice");
        client.write(path, "durable".getBytes());

        // Partition 2 of 3 BigTable tablet-servers — metadata writes lose quorum.
        FaultInjector faults = new FaultInjector(cluster);
        faults.partitionTabletServers(2);

        assertThatThrownBy(() -> client.create(FilePath.of("/data/new"), "bob"))
                .isInstanceOf(Faults.QuorumLostException.class);
        // Reads keep working off the surviving replica (possibly stale, but available).
        assertThat(client.read(path, 0, 7)).isEqualTo("durable".getBytes());

        // Healing restores write availability.
        faults.healTabletServers();
        assertThat(client.create(FilePath.of("/data/new2"), "bob")).isTrue();
    }

    @Test
    void deleteThenGcReclaimsTheExtents(@TempDir Path dir) {
        SimClock clock = new SimClock(t0);
        Cluster cluster = new Cluster(dir, clock, 5, 3, CHUNK, 15);
        cluster.addCurator("c1").assumeShard("/");
        cluster.heartbeatDservers(t0);
        ColossusClient client = cluster.client();

        FilePath path = FilePath.of("/data/doomed");
        client.create(path, "alice");
        client.append(path, "to-be-deleted".getBytes());

        // Find the chunk's replica set; its extents are now hosted on those D-servers.
        var loc = cluster.curators().get(0).shardFor(path).orElseThrow().getChunkLocations(path, 0).orElseThrow();
        var handle = loc.handle();
        for (DserverId d : loc.dservers()) {
            assertThat(cluster.dserver(d).hostedHandles()).contains(handle);
        }

        // Delete is a metadata tombstone — the bytes are still on disk right after.
        assertThat(client.delete(path)).isTrue();
        for (DserverId d : loc.dservers()) {
            assertThat(cluster.dserver(d).hostedHandles()).contains(handle);
        }

        // The Custodian's GC sweep reclaims the now-orphaned extents.
        List<colossus.common.ChunkHandle> reclaimed = cluster.gcTick();
        assertThat(reclaimed).contains(handle);
        for (DserverId d : loc.dservers()) {
            assertThat(cluster.dserver(d).hostedHandles()).doesNotContain(handle);
        }
        // A second sweep finds nothing left to do.
        assertThat(cluster.gcTick()).isEmpty();
    }
}
