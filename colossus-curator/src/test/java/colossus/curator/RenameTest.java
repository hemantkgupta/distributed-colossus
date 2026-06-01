package colossus.curator;

import colossus.bigtable.Mutation;
import colossus.common.FilePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenameTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

    private CuratorShard shard(CuratorFixture f) {
        return f.curator("c1", new MutableClock(t0)).assumeShard("/");
    }

    @Test
    void renameMovesFileAndPreservesChunks(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = shard(f);
        FilePath src = FilePath.of("/a/x");
        shard.createFile(src, "alice");
        var alloc = shard.allocateChunk(src);

        RenameCoordinator rc = new RenameCoordinator(f.bt, new MutableClock(t0));
        FilePath dst = FilePath.of("/b/y");
        assertThat(rc.rename(src, dst)).isTrue();

        assertThat(shard.stat(dst).exists()).isTrue();
        assertThat(shard.stat(src).exists()).isFalse();
        // the chunk moved with the file
        assertThat(shard.getChunkLocations(dst, 0)).hasValueSatisfying(loc ->
                assertThat(loc.handle()).isEqualTo(alloc.handle()));
    }

    @Test
    void renameToExistingDestinationFails(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = shard(f);
        shard.createFile(FilePath.of("/a/x"), "alice");
        shard.createFile(FilePath.of("/b/y"), "bob");
        RenameCoordinator rc = new RenameCoordinator(f.bt, new MutableClock(t0));
        assertThat(rc.rename(FilePath.of("/a/x"), FilePath.of("/b/y"))).isFalse();
        assertThat(shard.stat(FilePath.of("/a/x")).exists()).isTrue(); // source untouched
    }

    @Test
    void renameMissingSourceThrows(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        shard(f);
        RenameCoordinator rc = new RenameCoordinator(f.bt, new MutableClock(t0));
        assertThatThrownBy(() -> rc.rename(FilePath.of("/a/ghost"), FilePath.of("/b/y")))
                .isInstanceOf(CuratorShard.FileNotFoundException.class);
    }

    @Test
    void recoverFinalizesACommittedButUnfinishedRename(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = shard(f);
        FilePath src = FilePath.of("/a/x");
        FilePath dst = FilePath.of("/b/y");
        shard.createFile(src, "alice");
        // Simulate a crash AFTER commit, BEFORE finalize: source marked + destination already created.
        f.bt.checkAndMutate(src.toRowKey(), List.of(),
                List.of(Mutation.put(NamespaceCodec.RENAME_TARGET, 1, dst.path().getBytes(StandardCharsets.UTF_8))));
        shard.createFile(dst, "alice");

        RenameCoordinator rc = new RenameCoordinator(f.bt, new MutableClock(t0));
        assertThat(rc.recover("/")).contains(src);
        assertThat(shard.stat(src).exists()).isFalse(); // finished: source removed
        assertThat(shard.stat(dst).exists()).isTrue();
    }

    @Test
    void recoverRevertsAnUncommittedRename(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        CuratorShard shard = shard(f);
        FilePath src = FilePath.of("/a/x");
        shard.createFile(src, "alice");
        // Simulate a crash AFTER prepare, BEFORE the destination was created.
        f.bt.checkAndMutate(src.toRowKey(), List.of(),
                List.of(Mutation.put(NamespaceCodec.RENAME_TARGET, 1, "/b/y".getBytes(StandardCharsets.UTF_8))));

        RenameCoordinator rc = new RenameCoordinator(f.bt, new MutableClock(t0));
        assertThat(rc.recover("/")).contains(src);
        assertThat(shard.stat(src).exists()).isTrue(); // reverted: source intact
        assertThat(f.bt.get(src.toRowKey(), NamespaceCodec.RENAME_TARGET)).isEmpty(); // marker cleared
    }
}
