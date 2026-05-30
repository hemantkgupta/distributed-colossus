package colossus.curator;

import colossus.common.FilePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CuratorNamespaceTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void statColdFetchThenCacheHit(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        Curator c = f.curator("c1", clock);
        CuratorShard shard = c.assumeShard("/photos/");

        shard.createFile(FilePath.of("/photos/a.jpg"), "alice");
        Results.FileStat s1 = shard.stat(FilePath.of("/photos/a.jpg")); // cold
        Results.FileStat s2 = shard.stat(FilePath.of("/photos/a.jpg")); // cached

        assertThat(s1.exists()).isTrue();
        assertThat(s1.size()).isZero();
        assertThat(s2).isEqualTo(s1);
        assertThat(shard.coldFetches()).isEqualTo(1);
        assertThat(shard.cacheHits()).isEqualTo(1);
    }

    @Test
    void statAbsentForUnknownFile(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        assertThat(shard.stat(FilePath.of("/photos/missing")).exists()).isFalse();
    }

    @Test
    void createIsIdempotentCreateIfAbsent(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        assertThat(shard.createFile(FilePath.of("/photos/a"), "alice")).isTrue();
        assertThat(shard.createFile(FilePath.of("/photos/a"), "bob")).isFalse(); // already exists
    }

    @Test
    void allocateChunkProducesReplicaSetLeaseAndEpoch(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        CuratorShard shard = f.curator("c1", clock).assumeShard("/photos/");
        FilePath p = FilePath.of("/photos/big.dat");
        shard.createFile(p, "alice");

        Results.AllocateChunkResult c0 = shard.allocateChunk(p);
        Results.AllocateChunkResult c1 = shard.allocateChunk(p);

        assertThat(c0.chunkIndex()).isEqualTo(0);
        assertThat(c1.chunkIndex()).isEqualTo(1);
        assertThat(c0.dservers()).hasSize(3);
        assertThat(c0.primary()).isEqualTo(c0.dservers().get(0));
        assertThat(c0.lease().isValid(t0.plusSeconds(30))).isTrue();
        assertThat(c0.placementEpoch()).isGreaterThan(0);
        assertThat(c0.handle()).isNotEqualTo(c1.handle());
        assertThat(shard.stat(p).chunkCount()).isEqualTo(2);
    }

    @Test
    void getChunkLocationsResolvesHandleAndReplicas(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        FilePath p = FilePath.of("/photos/big.dat");
        shard.createFile(p, "alice");
        Results.AllocateChunkResult alloc = shard.allocateChunk(p);

        Results.ChunkLocation loc = shard.getChunkLocations(p, 0).orElseThrow();
        assertThat(loc.handle()).isEqualTo(alloc.handle());
        assertThat(loc.dservers()).isEqualTo(alloc.dservers());
        assertThat(loc.lease()).isPresent();
        assertThat(shard.getChunkLocations(p, 5)).isEmpty(); // no such chunk index
    }

    @Test
    void allocateOnMissingFileThrows(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        assertThatThrownBy(() -> shard.allocateChunk(FilePath.of("/photos/ghost")))
                .isInstanceOf(CuratorShard.FileNotFoundException.class);
    }

    @Test
    void deleteRemovesFileAndChunks(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = f.curator("c1", new MutableClock(t0)).assumeShard("/photos/");
        FilePath p = FilePath.of("/photos/x");
        shard.createFile(p, "alice");
        shard.allocateChunk(p);
        assertThat(shard.delete(p)).isTrue();
        assertThat(shard.stat(p).exists()).isFalse();
        assertThat(shard.delete(p)).isFalse(); // already gone
    }
}
