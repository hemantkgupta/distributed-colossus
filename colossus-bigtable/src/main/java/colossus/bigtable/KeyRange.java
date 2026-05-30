package colossus.bigtable;

import colossus.common.RowKey;

import java.util.Optional;

/**
 * A half-open row-key range [start, end). {@code start} is inclusive (the empty key is the
 * smallest, i.e. -inf); {@code end} empty means unbounded (+inf). Tablets own a KeyRange.
 */
public record KeyRange(RowKey start, Optional<RowKey> end) {

    public static final RowKey MIN = RowKey.of("");

    public static KeyRange all() {
        return new KeyRange(MIN, Optional.empty());
    }

    public static KeyRange of(RowKey start, RowKey endExclusive) {
        return new KeyRange(start, Optional.ofNullable(endExclusive));
    }

    public boolean contains(RowKey key) {
        if (key.compareTo(start) < 0) {
            return false;
        }
        return end.isEmpty() || key.compareTo(end.get()) < 0;
    }

    public boolean prefixOverlaps(String prefix) {
        // Whether some key with this prefix could fall in range — used by scans.
        RowKey p = RowKey.of(prefix);
        return end.isEmpty() || p.compareTo(end.get()) < 0;
    }
}
