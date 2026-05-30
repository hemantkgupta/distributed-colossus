package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The bootstrap tablet that holds the routing table: a mapping from each range-start row key to the
 * tablet id that owns the range beginning there. {@code route(key)} is a floor lookup. The routing
 * table is itself persisted (in a backing {@link Tablet}) so it survives a restart — clients consult
 * the MasterTablet on cold start, then cache, exactly as in BigTable.
 */
public final class MasterTablet implements AutoCloseable {

    private static final ColumnKey ROUTE = ColumnKey.of("routing", "tablet");

    private final Tablet backing;
    private final NavigableMap<RowKey, String> routes = new ConcurrentSkipListMap<>();
    private long ts = 0;

    public MasterTablet(Path walPath, boolean fsyncOnPut) {
        this.backing = new Tablet(KeyRange.all(), walPath, fsyncOnPut);
        // Recover routing from the backing tablet's replayed rows.
        for (RowKey start : backing.rowKeys()) {
            backing.get(start, ROUTE).ifPresent(v ->
                    routes.put(start, new String(v, StandardCharsets.UTF_8)));
        }
    }

    /** Declare that the range beginning at {@code rangeStart} is owned by {@code tabletId}. */
    public synchronized void assignRange(RowKey rangeStart, String tabletId) {
        routes.put(rangeStart, tabletId);
        backing.put(rangeStart, ROUTE, ++ts, tabletId.getBytes(StandardCharsets.UTF_8));
    }

    /** Resolve which tablet owns the given row key (the greatest range-start <= key). */
    public Optional<String> route(RowKey key) {
        var e = routes.floorEntry(key);
        return e == null ? Optional.empty() : Optional.of(e.getValue());
    }

    public NavigableMap<RowKey, String> snapshot() {
        return new java.util.TreeMap<>(routes);
    }

    public int rangeCount() {
        return routes.size();
    }

    @Override
    public void close() {
        backing.close();
    }
}
