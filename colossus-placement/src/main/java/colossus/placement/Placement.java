package colossus.placement;

import colossus.common.DserverId;
import colossus.common.PgId;

import java.util.List;
import java.util.Optional;

/**
 * The placement façade combining the two halves of hybrid placement: a pure {@link HashRouter}
 * (blob key → PG) and a cached {@link PgTable} lookup (PG → replica set). Hash bounds per-object
 * metadata to zero; the small lookup table bounds map-propagation cost. This is what the client and
 * Curator use to turn a chunk handle into a set of D-servers to talk to.
 */
public final class Placement {

    private final HashRouter router;
    private final PgTableCache cache;

    public Placement(HashRouter router, PgTableCache cache) {
        this.router = router;
        this.cache = cache;
    }

    public PgId placementGroupFor(String blobKey) {
        return router.placementGroupFor(blobKey);
    }

    public Optional<List<DserverId>> dserversFor(PgId pg) {
        return cache.get(pg).map(PgPlacement::dservers);
    }

    public Optional<PgPlacement> placementFor(String blobKey) {
        return cache.get(router.placementGroupFor(blobKey));
    }

    public PgTableCache cache() {
        return cache;
    }

    public HashRouter router() {
        return router;
    }
}
