package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TabletSplitterTest {

    private static final ColumnKey SIZE = ColumnKey.of("meta", "size");

    @Test
    void splitsAtMedianAndPartitionsRowsCleanly(@TempDir Path dir) {
        Tablet src = new Tablet(KeyRange.all(), dir.resolve("src.log"), false);
        for (int i = 0; i < 100; i++) {
            src.put(RowKey.of(String.format("/k%03d", i)), SIZE, 1, new byte[]{(byte) i});
        }
        assertThat(TabletSplitter.shouldSplit(src, 50, Long.MAX_VALUE)).isTrue();

        TabletSplitter.SplitResult r = TabletSplitter.split(src, dir.resolve("low.log"), dir.resolve("high.log"));

        // Every key lands in exactly one half, on the correct side of the split key.
        assertThat(r.low().owns(RowKey.of("/k000"))).isTrue();
        assertThat(r.high().owns(r.splitKey())).isTrue();
        assertThat(r.low().rowCount() + r.high().rowCount()).isEqualTo(100);
        assertThat(r.low().get(RowKey.of("/k000"), SIZE)).isPresent();
        assertThat(r.high().get(RowKey.of("/k099"), SIZE)).isPresent();

        // The boundary key itself belongs to the high half (inclusive start).
        assertThat(r.high().owns(r.splitKey())).isTrue();
        assertThat(r.low().owns(r.splitKey())).isFalse();
        r.low().close();
        r.high().close();
    }

    @Test
    void splitHalvesSurviveRestart(@TempDir Path dir) {
        Tablet src = new Tablet(KeyRange.all(), dir.resolve("src.log"), true);
        for (int i = 0; i < 10; i++) {
            src.put(RowKey.of("/k" + i), SIZE, 1, new byte[]{(byte) i});
        }
        Path lowWal = dir.resolve("low.log");
        Path highWal = dir.resolve("high.log");
        TabletSplitter.SplitResult r = TabletSplitter.split(src, lowWal, highWal);
        RowKey split = r.splitKey();
        r.low().close();
        r.high().close();

        try (Tablet low2 = new Tablet(KeyRange.of(KeyRange.MIN, split), lowWal, true)) {
            assertThat(low2.rowCount()).isGreaterThan(0);
            assertThat(low2.get(RowKey.of("/k0"), SIZE)).isPresent();
        }
    }

    @Test
    void belowThresholdDoesNotSplit(@TempDir Path dir) {
        Tablet src = new Tablet(KeyRange.all(), dir.resolve("src.log"), false);
        src.put(RowKey.of("/a"), SIZE, 1, new byte[]{1});
        assertThat(TabletSplitter.shouldSplit(src, 100, Long.MAX_VALUE)).isFalse();
        src.close();
    }
}
