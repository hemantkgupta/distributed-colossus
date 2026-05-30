package colossus.dserver;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.LeaseToken;
import colossus.dmclock.DmClockScheduler;
import colossus.dmclock.Lane;
import colossus.dmclock.Request;
import colossus.placement.DserverStatusTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DserverSchedulingTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private final ChunkHandle h = ChunkHandle.of(1);

    @Test
    void foregroundWriteReadCorrectAndTagged(@TempDir Path dir) {
        Dserver d = new Dserver(DserverId.of("d0", 7100), new ExtentStore(dir, true),
                Dserver.defaultScheduler(), null, 1_000_000_000L);
        d.leaseHolder().grant(new LeaseToken(h, d.id(), t0, t0.plusSeconds(60)));
        d.pushBytes(h, 0, "fg".getBytes());
        d.commitWrite(h, t0);
        assertThat(d.read(h, 0, 2)).isEqualTo("fg".getBytes());
        // The IO was routed onto the FOREGROUND lane (push + apply + read submissions queued there).
        assertThat(d.scheduler().pollNext()).hasValueSatisfying(tr ->
                assertThat(tr.request().lane()).isEqualTo(Lane.FOREGROUND));
    }

    @Test
    void foregroundDispatchesAheadOfBackgroundFlood(@TempDir Path dir) {
        DmClockScheduler s = Dserver.defaultScheduler();
        Dserver d = new Dserver(DserverId.of("d0", 7100), new ExtentStore(dir, true), s, null, 1_000_000_000L);
        d.leaseHolder().grant(new LeaseToken(h, d.id(), t0, t0.plusSeconds(60)));

        // A storm of background work is already queued...
        for (int i = 0; i < 500; i++) {
            s.submit(new Request(Lane.BACKGROUND, 65536, 1000 + i));
        }
        // ...then foreground writes/reads arrive (each enqueues FOREGROUND IO).
        d.pushBytes(h, 0, new byte[]{1});
        d.commitWrite(h, t0);
        d.read(h, 0, 1);

        // dmClock's reservation phase serves the foreground IO first despite the background backlog.
        for (int i = 0; i < 3; i++) {
            assertThat(s.pollNext()).hasValueSatisfying(tr ->
                    assertThat(tr.request().lane()).isEqualTo(Lane.FOREGROUND));
        }
        // Functional correctness is unaffected by the background load.
        assertThat(d.store().read(h, 0, 1)).containsExactly(1);
    }

    @Test
    void heartbeatWritesStatusAndStalenessIsDetected(@TempDir Path dir) {
        BigTableClient bt = new BigTableClient(new BigTableCluster(dir.resolve("bt"), 1, 1_000_000, Long.MAX_VALUE, false));
        DserverStatusTable status = new DserverStatusTable(bt);
        Dserver d = new Dserver(DserverId.of("d0", 7100), new ExtentStore(dir.resolve("ext"), true),
                Dserver.defaultScheduler(), status, 500_000_000L);
        d.leaseHolder().grant(new LeaseToken(h, d.id(), t0, t0.plusSeconds(60)));
        d.pushBytes(h, 0, new byte[]{1});
        d.commitWrite(h, t0);

        d.heartbeat(t0);
        assertThat(status.isUp(d.id(), t0.plusSeconds(10), 15)).isTrue();   // fresh
        assertThat(status.isUp(d.id(), t0.plusSeconds(20), 15)).isFalse();  // stale (>15 s)
        assertThat(status.get(d.id()).orElseThrow().numExtents()).isEqualTo(1);
        assertThat(status.liveDservers(t0.plusSeconds(5), 15)).containsExactly(d.id());
        assertThat(status.downDservers(t0.plusSeconds(20), 15)).containsExactly(d.id());
    }
}
