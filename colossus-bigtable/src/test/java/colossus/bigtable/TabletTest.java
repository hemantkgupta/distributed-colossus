package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TabletTest {

    private static final ColumnKey SIZE = ColumnKey.of("meta", "size");

    @Test
    void tabletSurvivesRestartViaWalReplay(@TempDir Path dir) {
        Path wal = dir.resolve("commit.log");
        try (Tablet t = new Tablet(KeyRange.all(), wal, true)) {
            t.put(RowKey.of("/a"), SIZE, 1, new byte[]{1});
            t.put(RowKey.of("/b"), SIZE, 1, new byte[]{2});
            t.checkAndMutate(RowKey.of("/a"),
                    List.of(Condition.expectValue(SIZE, new byte[]{1})),
                    List.of(Mutation.put(SIZE, 2, new byte[]{9})));
        }
        // Reopen with the same WAL — state must come back exactly.
        try (Tablet t2 = new Tablet(KeyRange.all(), wal, true)) {
            assertThat(t2.get(RowKey.of("/a"), SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(9));
            assertThat(t2.get(RowKey.of("/b"), SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(2));
            assertThat(t2.rowCount()).isEqualTo(2);
        }
    }

    @Test
    void deletesReplayAsTombstones(@TempDir Path dir) {
        Path wal = dir.resolve("commit.log");
        try (Tablet t = new Tablet(KeyRange.all(), wal, true)) {
            t.put(RowKey.of("/a"), SIZE, 1, new byte[]{1});
            t.checkAndMutate(RowKey.of("/a"), List.of(), List.of(Mutation.delete(SIZE, 2)));
        }
        try (Tablet t2 = new Tablet(KeyRange.all(), wal, true)) {
            assertThat(t2.get(RowKey.of("/a"), SIZE)).isEmpty();
        }
    }

    @Test
    void tornTailIsIgnoredOnReplay(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("commit.log");
        try (Tablet t = new Tablet(KeyRange.all(), wal, true)) {
            t.put(RowKey.of("/a"), SIZE, 1, new byte[]{1});
            t.put(RowKey.of("/b"), SIZE, 1, new byte[]{2});
        }
        // Simulate a crash mid-append: chop the last few bytes off the log.
        byte[] full = Files.readAllBytes(wal);
        Files.write(wal, java.util.Arrays.copyOf(full, full.length - 3));
        try (Tablet t2 = new Tablet(KeyRange.all(), wal, true)) {
            assertThat(t2.get(RowKey.of("/a"), SIZE)).isPresent(); // first intact record survives
            assertThat(t2.rowCount()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void writeOutsideRangeRejected(@TempDir Path dir) {
        try (Tablet t = new Tablet(KeyRange.of(RowKey.of("/m"), RowKey.of("/z")), dir.resolve("w.log"), false)) {
            assertThatThrownBy(() -> t.put(RowKey.of("/a"), SIZE, 1, new byte[]{1}))
                    .isInstanceOf(NotOwnerException.class);
            t.put(RowKey.of("/photos"), SIZE, 1, new byte[]{1}); // in range
            assertThat(t.get(RowKey.of("/photos"), SIZE)).isPresent();
        }
    }

    @Test
    void scanPrefixReturnsMatchingRowsInOrder(@TempDir Path dir) {
        try (Tablet t = new Tablet(KeyRange.all(), dir.resolve("w.log"), false)) {
            t.put(RowKey.of("/photos/b"), SIZE, 1, new byte[]{1});
            t.put(RowKey.of("/photos/a"), SIZE, 1, new byte[]{1});
            t.put(RowKey.of("/docs/x"), SIZE, 1, new byte[]{1});
            assertThat(t.scanPrefix("/photos/").keySet().stream().map(RowKey::asString))
                    .containsExactly("/photos/a", "/photos/b");
        }
    }
}
