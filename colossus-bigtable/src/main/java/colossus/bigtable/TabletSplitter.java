package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Splits a hot tablet at its median row key into two tablets when row count or byte count crosses
 * a threshold. The two halves get fresh WALs seeded with the source rows; the routing table is then
 * updated (by the caller / {@link BigTableCluster}) so clients re-route. Clients hitting the old
 * range during the brief window get {@link NotOwnerException} and refetch — sub-second unavailability
 * for the split range only.
 */
public final class TabletSplitter {

    public record SplitResult(RowKey splitKey, Tablet low, Tablet high) {}

    private TabletSplitter() {}

    public static boolean shouldSplit(Tablet t, int rowThreshold, long byteThreshold) {
        return t.rowCount() > rowThreshold || t.approximateBytes() > byteThreshold;
    }

    /**
     * Split {@code source} at its median key into [start, median) and [median, end). The source
     * tablet is closed; callers must replace it with the two returned tablets and update routing.
     */
    public static SplitResult split(Tablet source, Path lowWal, Path highWal) {
        Optional<RowKey> median = source.medianKey();
        if (median.isEmpty()) {
            throw new IllegalStateException("cannot split a tablet with fewer than 2 rows");
        }
        RowKey splitKey = median.get();
        KeyRange srcRange = source.range();
        Tablet low = new Tablet(KeyRange.of(srcRange.start(), splitKey), lowWal, true);
        Tablet high = new Tablet(KeyRange.of(splitKey, srcRange.end().orElse(null)), highWal, true);

        for (var entry : source.rawRows().entrySet()) {
            RowKey key = entry.getKey();
            Row row = entry.getValue();
            Tablet target = key.compareTo(splitKey) < 0 ? low : high;
            long ts = 1;
            for (ColumnKey col : row.liveColumns()) {
                byte[] v = row.get(col).orElseThrow();
                target.put(key, col, ts++, v);
            }
        }
        source.close();
        return new SplitResult(splitKey, low, high);
    }
}
