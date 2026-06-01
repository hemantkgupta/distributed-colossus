package colossus.custodian;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.ChunkHandle;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.common.WireWriter;
import colossus.placement.HashRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class GarbageCollectorTest {

    private final DserverId target = DserverId.of("d0", 7100);

    /** Records dispatched work; mirrors CustodianTest.RecordingDispatcher. */
    static final class RecordingDispatcher implements WorkDispatcher {
        final List<WorkItem> items = new ArrayList<>();

        @Override
        public WorkResult dispatch(WorkItem item) {
            items.add(item);
            return WorkResult.success();
        }
    }

    private BigTableClient bt(Path dir) {
        return new BigTableClient(new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false));
    }

    /** Write one live file row whose chunks: family references the given handles (index = position). */
    private void writeFile(BigTableClient bt, long ts, String path, ChunkHandle... handles) {
        FilePath fp = FilePath.of(path);
        for (int i = 0; i < handles.length; i++) {
            byte[] value = new WireWriter().i64(handles[i].id()).toByteArray();
            bt.put(fp.toRowKey(), ColumnKey.of("chunks", Integer.toString(i)), ts, value);
        }
    }

    @Test
    void liveHandlesCollectsEveryReferencedHandle(@TempDir Path dir) {
        BigTableClient bt = bt(dir);
        ChunkHandle h1 = ChunkHandle.of(0x1111);
        ChunkHandle h2 = ChunkHandle.of(0x2222);
        ChunkHandle h3 = ChunkHandle.of(0x3333);
        writeFile(bt, 1, "/a/file1", h1, h2);
        writeFile(bt, 1, "/a/file2", h3);

        GarbageCollector gc = new GarbageCollector(bt, target, new HashRouter(256));

        assertThat(gc.liveHandles("/")).containsExactlyInAnyOrder(h1, h2, h3);
    }

    @Test
    void findOrphansReturnsExactlyTheUnreferencedHosted(@TempDir Path dir) {
        BigTableClient bt = bt(dir);
        ChunkHandle live1 = ChunkHandle.of(0x1111);
        ChunkHandle live2 = ChunkHandle.of(0x2222);
        writeFile(bt, 1, "/a/file1", live1, live2);

        ChunkHandle orphanA = ChunkHandle.of(0xAAAA); // file deleted / never-referenced straggler
        ChunkHandle orphanB = ChunkHandle.of(0xBBBB);

        GarbageCollector gc = new GarbageCollector(bt, target, new HashRouter(256));

        // Hosted set = the two live extents plus two unreferenced ones.
        List<ChunkHandle> hosted = List.of(live1, live2, orphanA, orphanB);

        assertThat(gc.findOrphans(hosted, "/")).containsExactlyInAnyOrder(orphanA, orphanB);
    }

    @Test
    void reclaimDispatchesOneDeleteExtentPerOrphan(@TempDir Path dir) {
        BigTableClient bt = bt(dir);
        ChunkHandle live1 = ChunkHandle.of(0x1111);
        writeFile(bt, 1, "/a/file1", live1);

        ChunkHandle orphanA = ChunkHandle.of(0xAAAA);
        ChunkHandle orphanB = ChunkHandle.of(0xBBBB);

        GarbageCollector gc = new GarbageCollector(bt, target, new HashRouter(256));
        RecordingDispatcher disp = new RecordingDispatcher();

        List<ChunkHandle> hosted = List.of(live1, orphanA, orphanB);
        List<WorkItem> dispatched = gc.reclaim(hosted, "/", disp);

        // One DELETE_EXTENT per orphan; the live extent is left alone.
        assertThat(dispatched).hasSize(2);
        assertThat(dispatched).allMatch(i -> i.op() == WorkItem.Op.DELETE_EXTENT);
        assertThat(dispatched).allMatch(i -> i.target().equals(java.util.Optional.of(target)));
        assertThat(disp.items).hasSize(2);
        assertThat(disp.items).extracting(WorkItem::op)
                .containsOnly(WorkItem.Op.DELETE_EXTENT);
    }

    @Test
    void deletedFileLeavesNoLiveHandlesSoExtentBecomesOrphan(@TempDir Path dir) {
        BigTableClient bt = bt(dir);
        ChunkHandle h = ChunkHandle.of(0xC0FFEE);
        FilePath fp = FilePath.of("/a/doomed");
        writeFile(bt, 1, fp.path(), h);

        // Simulate CuratorShard.delete: tombstone the chunks: cell at a later timestamp.
        bt.delete(fp.toRowKey(), ColumnKey.of("chunks", "0"), 2);

        GarbageCollector gc = new GarbageCollector(bt, target, new HashRouter(256));

        // No live file row references the handle any more.
        assertThat(gc.liveHandles("/")).doesNotContain(h);
        // The extent the D-server still hosts is now an orphan.
        assertThat(gc.findOrphans(Set.of(h), "/")).containsExactly(h);
    }
}
