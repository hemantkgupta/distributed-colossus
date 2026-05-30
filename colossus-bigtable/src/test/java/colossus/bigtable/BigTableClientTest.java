package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BigTableClientTest {

    private static final ColumnKey SIZE = ColumnKey.of("meta", "size");

    @Test
    void putGetScanRoundtrip(@TempDir Path dir) {
        try (BigTableCluster c = new BigTableCluster(dir, 3, 1_000_000, Long.MAX_VALUE, false)) {
            BigTableClient client = new BigTableClient(c);
            client.put(RowKey.of("/photos/a"), SIZE, 1, new byte[]{1});
            client.put(RowKey.of("/photos/b"), SIZE, 1, new byte[]{2});
            client.put(RowKey.of("/docs/x"), SIZE, 1, new byte[]{3});

            assertThat(client.get(RowKey.of("/photos/a"), SIZE))
                    .hasValueSatisfying(v -> assertThat(v).containsExactly(1));
            assertThat(client.scan("/photos/").keySet().stream().map(RowKey::asString))
                    .containsExactly("/photos/a", "/photos/b");
        }
    }

    @Test
    void casUnderContentionViaClient(@TempDir Path dir) {
        try (BigTableCluster c = new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false)) {
            BigTableClient client = new BigTableClient(c);
            RowKey k = RowKey.of("/foo");
            boolean first = client.checkAndMutate(k,
                    List.of(Condition.expectAbsent(SIZE)), List.of(Mutation.put(SIZE, 1, new byte[]{1})));
            boolean second = client.checkAndMutate(k,
                    List.of(Condition.expectAbsent(SIZE)), List.of(Mutation.put(SIZE, 2, new byte[]{2})));
            assertThat(first).isTrue();
            assertThat(second).isFalse();
        }
    }

    @Test
    void readLatencyDominatedByCacheNoRouteChurn(@TempDir Path dir) {
        try (BigTableCluster c = new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false)) {
            BigTableClient client = new BigTableClient(c);
            RowKey k = RowKey.of("/foo");
            client.put(k, SIZE, 1, new byte[]{1});
            for (int i = 0; i < 100; i++) {
                client.get(k, SIZE);
            }
            assertThat(client.cachedRoutes()).isEqualTo(1); // one route, cached and reused
        }
    }

    @Test
    void splitIsTransparentToClientAndRoutesUpdate(@TempDir Path dir) {
        // Low split threshold forces a split partway through the write burst.
        try (BigTableCluster c = new BigTableCluster(dir, 3, 8, Long.MAX_VALUE, false)) {
            BigTableClient client = new BigTableClient(c);
            for (int i = 0; i < 40; i++) {
                client.put(RowKey.of(String.format("/k%03d", i)), SIZE, 1, new byte[]{(byte) i});
            }
            // The keyspace has been split into multiple tablets...
            assertThat(c.groupCount()).isGreaterThan(1);
            // ...yet every key is still readable through the client (stale routes refetched on NotOwner).
            for (int i = 0; i < 40; i++) {
                assertThat(client.get(RowKey.of(String.format("/k%03d", i)), SIZE))
                        .as("key %03d after split", i)
                        .isPresent();
            }
            assertThat(client.scan("/k").size()).isEqualTo(40);
        }
    }
}
