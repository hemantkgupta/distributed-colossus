package colossus.placement;

import colossus.common.PgId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HashRouterTest {

    @Test
    void deterministicAcrossInstancesAndRuns() {
        HashRouter a = new HashRouter(4096);
        HashRouter b = new HashRouter(4096);
        for (String key : new String[]{"/photos/2024/a.jpg", "0000000000004242", "/x"}) {
            assertThat(a.placementGroupFor(key)).isEqualTo(b.placementGroupFor(key));
        }
        // Spot-check stability against a recomputed hash.
        assertThat(a.placementGroupFor("/photos/2024/a.jpg"))
                .isEqualTo(PgId.of(Math.floorMod(HashRouter.hash64("/photos/2024/a.jpg"), 4096)));
    }

    @Test
    void distributionIsApproximatelyUniform() {
        int pgs = 256;
        HashRouter router = new HashRouter(pgs);
        Map<PgId, Integer> counts = new HashMap<>();
        int n = 256_000; // ~1000 per PG ideal
        for (int i = 0; i < n; i++) {
            counts.merge(router.placementGroupFor("/file/" + i + ".dat"), 1, Integer::sum);
        }
        // Every PG used, and no PG wildly over/under the ideal of n/pgs = 1000.
        assertThat(counts.keySet()).hasSize(pgs);
        int ideal = n / pgs;
        int min = counts.values().stream().mapToInt(Integer::intValue).min().orElseThrow();
        int max = counts.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
        assertThat(min).isGreaterThan((int) (ideal * 0.6));
        assertThat(max).isLessThan((int) (ideal * 1.4));
    }

    @Test
    void pgIdAlwaysInRange() {
        HashRouter router = new HashRouter(4096);
        for (int i = 0; i < 10_000; i++) {
            int id = router.placementGroupFor("k" + i).id();
            assertThat(id).isBetween(0, 4095);
        }
    }
}
