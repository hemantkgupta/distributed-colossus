package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RowTest {

    private static final ColumnKey SIZE = ColumnKey.of("meta", "size");
    private static final ColumnKey OWNER = ColumnKey.of("meta", "owner");

    @Test
    void putAndGetLatestVersion() {
        Row row = new Row(RowKey.of("/a"));
        row.put(SIZE, 1, new byte[]{1});
        row.put(SIZE, 2, new byte[]{2});
        assertThat(row.get(SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(2));
    }

    @Test
    void deleteTombstonesColumn() {
        Row row = new Row(RowKey.of("/a"));
        row.put(SIZE, 1, new byte[]{9});
        row.delete(SIZE, 2);
        assertThat(row.get(SIZE)).isEmpty();
        assertThat(row.isEmpty()).isTrue();
    }

    @Test
    void casExpectAbsentSucceedsOnceThenFails() {
        Row row = new Row(RowKey.of("/foo"));
        boolean first = row.checkAndMutate(
                List.of(Condition.expectAbsent(SIZE)),
                List.of(Mutation.put(SIZE, 1, new byte[]{1})));
        boolean second = row.checkAndMutate(
                List.of(Condition.expectAbsent(SIZE)),
                List.of(Mutation.put(SIZE, 2, new byte[]{2})));
        assertThat(first).isTrue();
        assertThat(second).isFalse(); // column now present → create-if-absent fails
        assertThat(row.get(SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(1));
    }

    @Test
    void casExpectValueGuardsUpdate() {
        Row row = new Row(RowKey.of("/foo"));
        row.put(SIZE, 1, new byte[]{1});
        boolean wrong = row.checkAndMutate(
                List.of(Condition.expectValue(SIZE, new byte[]{9})),
                List.of(Mutation.put(SIZE, 2, new byte[]{2})));
        boolean right = row.checkAndMutate(
                List.of(Condition.expectValue(SIZE, new byte[]{1})),
                List.of(Mutation.put(SIZE, 2, new byte[]{2})));
        assertThat(wrong).isFalse();
        assertThat(right).isTrue();
        assertThat(row.get(SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(2));
    }

    @Test
    void multiColumnMutationIsAllOrNothing() {
        Row row = new Row(RowKey.of("/foo"));
        boolean ok = row.checkAndMutate(
                List.of(Condition.expectAbsent(SIZE)),
                List.of(Mutation.put(SIZE, 1, new byte[]{1}),
                        Mutation.put(OWNER, 1, "alice".getBytes())));
        assertThat(ok).isTrue();
        assertThat(row.get(SIZE)).isPresent();
        assertThat(row.get(OWNER)).hasValueSatisfying(v -> assertThat(new String(v)).isEqualTo("alice"));
        assertThat(row.liveColumns()).containsExactlyInAnyOrder(SIZE, OWNER);
    }

    @Test
    void familyScanReturnsLatestLiveValues() {
        Row row = new Row(RowKey.of("/foo"));
        row.put(ColumnKey.of("chunks", "0"), 1, new byte[]{0});
        row.put(ColumnKey.of("chunks", "1"), 1, new byte[]{1});
        row.put(ColumnKey.of("meta", "size"), 1, new byte[]{99});
        assertThat(row.family("chunks").keySet()).containsExactly("0", "1");
    }

    @Test
    void concurrentCasHasExactlyOneWinner() throws Exception {
        Row row = new Row(RowKey.of("/foo"));
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger winners = new AtomicInteger();
        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final int id = i;
                futures.add(pool.submit(() -> {
                    boolean won = row.checkAndMutate(
                            List.of(Condition.expectAbsent(OWNER)),
                            List.of(Mutation.put(OWNER, id, new byte[]{(byte) id})));
                    if (won) winners.incrementAndGet();
                }));
            }
            for (Future<?> f : futures) f.get();
        } finally {
            pool.shutdownNow();
        }
        assertThat(winners.get()).isEqualTo(1); // create-if-absent: exactly one winner
        assertThat(row.get(OWNER)).isPresent();
    }
}
