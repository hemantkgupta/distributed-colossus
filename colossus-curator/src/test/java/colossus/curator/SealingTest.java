package colossus.curator;

import colossus.common.ChunkHandle;
import colossus.common.FilePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SealingTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void sealMakesChunkImmutableAndRevokesLease(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        FilePath p = FilePath.of("/photos/big.dat");
        shard.createFile(p, "alice");
        ChunkHandle h = shard.allocateChunk(p).handle();

        assertThat(shard.isSealed(p, h)).isFalse();
        assertThat(f.leases.get(p, h)).isPresent();        // a fresh chunk holds a lease

        assertThat(shard.seal(p, h)).isTrue();             // seal it
        assertThat(shard.isSealed(p, h)).isTrue();
        assertThat(f.leases.get(p, h)).isEmpty();          // lease released
        assertThat(shard.seal(p, h)).isFalse();            // idempotent
    }

    @Test
    void renewingALeaseOnASealedChunkIsRejected(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        FilePath p = FilePath.of("/photos/big.dat");
        shard.createFile(p, "alice");
        ChunkHandle h = shard.allocateChunk(p).handle();

        assertThat(shard.renewLease(p, h)).isPresent();    // fine while open
        shard.seal(p, h);
        assertThatThrownBy(() -> shard.renewLease(p, h))
                .isInstanceOf(CuratorShard.SealedExtentException.class);
    }
}
