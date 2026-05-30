package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The client API the Curator, Custodian, and placement library use to talk to BigTable:
 * {@code put}, {@code get}, {@code scan}, {@code checkAndMutate}. It caches the row→tablet routing
 * and, on a {@link NotOwnerException} (after a split or ownership change), evicts the stale entry,
 * refetches the route from the MasterTablet, and retries — exactly the BigTable client contract.
 */
public final class BigTableClient {

    private final BigTableCluster cluster;
    private final ConcurrentHashMap<RowKey, String> routeCache = new ConcurrentHashMap<>();
    private static final int MAX_RETRIES = 4;

    public BigTableClient(BigTableCluster cluster) {
        this.cluster = cluster;
    }

    private String resolve(RowKey key) {
        return routeCache.computeIfAbsent(key,
                k -> cluster.route(k).orElseThrow(() -> new NotOwnerException("no route for " + k)));
    }

    private interface Attempt<T> {
        T run(String tabletId);
    }

    private <T> T withRouting(RowKey key, Attempt<T> attempt) {
        NotOwnerException last = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            String tabletId = resolve(key);
            try {
                return attempt.run(tabletId);
            } catch (NotOwnerException e) {
                last = e;
                routeCache.remove(key); // stale route — refetch on next loop
            }
        }
        throw last != null ? last : new NotOwnerException("routing failed for " + key);
    }

    public boolean checkAndMutate(RowKey key, List<Condition> conditions, List<Mutation> mutations) {
        return withRouting(key, id -> cluster.checkAndMutate(id, key, conditions, mutations));
    }

    public void put(RowKey key, ColumnKey column, long ts, byte[] value) {
        checkAndMutate(key, List.of(), List.of(Mutation.put(column, ts, value)));
    }

    public void delete(RowKey key, ColumnKey column, long ts) {
        checkAndMutate(key, List.of(), List.of(Mutation.delete(column, ts)));
    }

    public Optional<byte[]> get(RowKey key, ColumnKey column) {
        return withRouting(key, id -> cluster.get(id, key, column));
    }

    public Optional<Row> getRow(RowKey key) {
        return withRouting(key, id -> cluster.getRow(id, key));
    }

    public NavigableMap<RowKey, Row> scan(String prefix) {
        return cluster.scan(prefix);
    }

    public int cachedRoutes() {
        return routeCache.size();
    }
}
