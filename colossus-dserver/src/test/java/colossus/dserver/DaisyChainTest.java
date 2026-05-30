package colossus.dserver;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.LeaseToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DaisyChainTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private final ChunkHandle h = ChunkHandle.of(0x4242);

    /** Build a 3-node chain primary -> secondary -> tertiary, each with its own extent store. */
    private List<Dserver> chain(Path dir) {
        List<Dserver> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Dserver d = new Dserver(DserverId.of("d" + i, 7100),
                    new ExtentStore(dir.resolve("d" + i), true),
                    Dserver.defaultScheduler(), null, 1_000_000_000L);
            nodes.add(d);
        }
        nodes.get(0).setDownstream(nodes.get(1));
        nodes.get(1).setDownstream(nodes.get(2));
        return nodes;
    }

    @Test
    void pushThenCommitReplicatesToAllThree(@TempDir Path dir) {
        List<Dserver> nodes = chain(dir);
        Dserver primary = nodes.get(0);
        primary.leaseHolder().grant(new LeaseToken(h, primary.id(), t0, t0.plusSeconds(60)));

        byte[] payload = "daisy-chain-bytes".getBytes();
        primary.pushBytes(h, 0, payload);          // buffers down the whole chain
        long serial = primary.commitWrite(h, t0);  // applies + fsyncs at each node

        assertThat(serial).isEqualTo(1);
        for (Dserver d : nodes) {
            assertThat(d.store().read(h, 0, payload.length)).isEqualTo(payload);
        }
        // Durability: a fresh store over the tertiary's dir reads the bytes (fsync happened).
        assertThat(new ExtentStore(dir.resolve("d2"), true).read(h, 0, payload.length)).isEqualTo(payload);
    }

    @Test
    void commitsGetMonotonicSerialsFromPrimaryLease(@TempDir Path dir) {
        List<Dserver> nodes = chain(dir);
        Dserver primary = nodes.get(0);
        primary.leaseHolder().grant(new LeaseToken(h, primary.id(), t0, t0.plusSeconds(60)));

        primary.pushBytes(h, 0, new byte[]{1});
        assertThat(primary.commitWrite(h, t0)).isEqualTo(1);
        primary.pushBytes(h, 0, new byte[]{2});
        assertThat(primary.commitWrite(h, t0)).isEqualTo(2);
    }

    @Test
    void commitWithoutLeaseRejectedAtPrimary(@TempDir Path dir) {
        List<Dserver> nodes = chain(dir);
        Dserver primary = nodes.get(0);
        primary.pushBytes(h, 0, new byte[]{1});
        assertThatThrownBy(() -> primary.commitWrite(h, t0))
                .isInstanceOf(LeaseHolder.NotLeaseHolderException.class);
    }

    @Test
    void pendingBuffersAtEveryNodeBeforeCommit(@TempDir Path dir) {
        List<Dserver> nodes = chain(dir);
        Dserver primary = nodes.get(0);
        primary.leaseHolder().grant(new LeaseToken(h, primary.id(), t0, t0.plusSeconds(60)));
        primary.pushBytes(h, 0, new byte[]{1, 2, 3});
        // Bytes are buffered everywhere but committed nowhere yet.
        for (Dserver d : nodes) {
            assertThat(d.peekPending(h)).isPresent();
            assertThat(d.store().exists(h)).isFalse();
        }
    }
}
