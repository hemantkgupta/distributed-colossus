package colossus.placement;

import colossus.common.PgId;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client-side cache of PG placements. Steady-state reads are served from the cache (no BigTable
 * round-trip), so addressing a D-server costs nothing once warm. The cache is refreshed lazily when
 * a D-server rejects an RPC with a newer epoch (STALE_PLACEMENT_REPLY): the caller calls
 * {@link #invalidate} and the next {@link #get} reloads the row. {@code backingReads} counts how
 * often the cache had to consult the PgTable — used to demonstrate cache-dominated reads.
 */
public final class PgTableCache {

    private final PgTable table;
    private final ConcurrentHashMap<PgId, PgPlacement> cache = new ConcurrentHashMap<>();
    private final AtomicLong backingReads = new AtomicLong();

    public PgTableCache(PgTable table) {
        this.table = table;
    }

    public Optional<PgPlacement> get(PgId pg) {
        PgPlacement cached = cache.get(pg);
        if (cached != null) {
            return Optional.of(cached);
        }
        backingReads.incrementAndGet();
        Optional<PgPlacement> loaded = table.lookup(pg);
        loaded.ifPresent(p -> cache.put(pg, p));
        return loaded;
    }

    /** Force a reload of this PG on the next {@link #get} (called on epoch-mismatch rejection). */
    public void invalidate(PgId pg) {
        cache.remove(pg);
    }

    /**
     * Reconcile against an epoch observed from a D-server. If the server's epoch is newer than the
     * cached one, the cache entry is stale and is dropped so the next get refetches.
     *
     * @return true if the cache was found stale and invalidated
     */
    public boolean reconcileEpoch(PgId pg, int observedEpoch) {
        PgPlacement cached = cache.get(pg);
        if (cached != null && observedEpoch > cached.epoch()) {
            cache.remove(pg);
            return true;
        }
        return false;
    }

    public long backingReads() {
        return backingReads.get();
    }

    public int cachedEntries() {
        return cache.size();
    }
}
