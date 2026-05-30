package colossus.placement;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.DserverId;
import colossus.common.PgId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PgTableCacheTest {

    private static final DserverId A = DserverId.of("h", 1);
    private static final DserverId B = DserverId.of("h", 2);
    private static final DserverId C = DserverId.of("h", 3);
    private static final DserverId D = DserverId.of("h", 4);

    private PgTable table(Path dir) {
        return new PgTable(new BigTableClient(new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false)));
    }

    @Test
    void readsAreCacheDominated(@TempDir Path dir) {
        PgTable table = table(dir);
        table.assign(PgId.of(7), List.of(A, B, C), "p");
        PgTableCache cache = new PgTableCache(table);

        for (int i = 0; i < 100; i++) {
            assertThat(cache.get(PgId.of(7))).isPresent();
        }
        assertThat(cache.backingReads()).isEqualTo(1); // 1 load, 99 cache hits
        assertThat(cache.cachedEntries()).isEqualTo(1);
    }

    @Test
    void epochMismatchTriggersRefresh(@TempDir Path dir) {
        PgTable table = table(dir);
        table.assign(PgId.of(7), List.of(A, B, C), "p");        // epoch 1
        PgTableCache cache = new PgTableCache(table);
        assertThat(cache.get(PgId.of(7)).orElseThrow().epoch()).isEqualTo(1);

        // A repair changes the replica set behind the cache's back → epoch 2.
        table.assign(PgId.of(7), List.of(A, B, D), "p");

        // A D-server rejects the client with the newer epoch. The cache reconciles and refetches.
        boolean stale = cache.reconcileEpoch(PgId.of(7), 2);
        assertThat(stale).isTrue();
        PgPlacement refreshed = cache.get(PgId.of(7)).orElseThrow();
        assertThat(refreshed.epoch()).isEqualTo(2);
        assertThat(refreshed.dservers()).containsExactly(A, B, D);
        assertThat(cache.backingReads()).isEqualTo(2); // initial load + post-invalidation reload
    }

    @Test
    void reconcileWithSameOrOlderEpochKeepsCache(@TempDir Path dir) {
        PgTable table = table(dir);
        table.assign(PgId.of(7), List.of(A, B, C), "p");
        PgTableCache cache = new PgTableCache(table);
        cache.get(PgId.of(7));
        assertThat(cache.reconcileEpoch(PgId.of(7), 1)).isFalse(); // not newer
        assertThat(cache.backingReads()).isEqualTo(1);
    }

    @Test
    void placementFacadeResolvesBlobToDservers(@TempDir Path dir) {
        PgTable table = table(dir);
        HashRouter router = new HashRouter(4096);
        PgId pg = router.placementGroupFor("/photos/a.jpg");
        table.assign(pg, List.of(A, B, C), "rack-distinct-3x");

        Placement placement = new Placement(router, new PgTableCache(table));
        assertThat(placement.placementGroupFor("/photos/a.jpg")).isEqualTo(pg);
        assertThat(placement.dserversFor(pg)).hasValueSatisfying(ds -> assertThat(ds).containsExactly(A, B, C));
        assertThat(placement.placementFor("/photos/a.jpg").orElseThrow().primary()).isEqualTo(A);
    }
}
