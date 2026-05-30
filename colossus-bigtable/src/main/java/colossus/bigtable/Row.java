package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The unit of atomicity in BigTable. One file = one row in the namespace schema. Columns are
 * versioned by timestamp; the latest non-tombstone version is the live value. A {@link ReentrantLock}
 * makes {@link #checkAndMutate} an atomic compare-and-set across multiple columns of the same row —
 * the single primitive that gives Colossus its metadata atomicity without cross-row transactions.
 */
public final class Row {

    private final RowKey key;
    private final NavigableMap<ColumnKey, NavigableMap<Long, VersionedCell>> cells =
            new ConcurrentSkipListMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public Row(RowKey key) {
        this.key = key;
    }

    public RowKey key() {
        return key;
    }

    /** Latest live value for a column, or empty if absent / tombstoned. */
    public Optional<byte[]> get(ColumnKey column) {
        lock.lock();
        try {
            return latest(column);
        } finally {
            lock.unlock();
        }
    }

    private Optional<byte[]> latest(ColumnKey column) {
        NavigableMap<Long, VersionedCell> versions = cells.get(column);
        if (versions == null || versions.isEmpty()) {
            return Optional.empty();
        }
        VersionedCell newest = versions.lastEntry().getValue();
        return newest.isTombstone() ? Optional.empty() : Optional.of(newest.value());
    }

    /** Apply a single put outside a CAS (still atomic at the cell level). */
    public void put(ColumnKey column, long timestamp, byte[] value) {
        checkAndMutate(List.of(), List.of(Mutation.put(column, timestamp, value)));
    }

    public void delete(ColumnKey column, long timestamp) {
        checkAndMutate(List.of(), List.of(Mutation.delete(column, timestamp)));
    }

    /**
     * Atomically: test every condition, and only if all hold, apply every mutation. Returns
     * whether the mutations were applied. The whole test-and-apply runs under the row lock.
     */
    public boolean checkAndMutate(List<Condition> conditions, List<Mutation> mutations) {
        lock.lock();
        try {
            for (Condition c : conditions) {
                if (!c.matches(latest(c.column()).orElse(null))) {
                    return false;
                }
            }
            for (Mutation m : mutations) {
                applyLocked(m);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Used by WAL replay / replication — applies an already-decided mutation, no conditions. */
    void applyLocked(Mutation m) {
        cells.computeIfAbsent(m.column(), c -> new TreeMap<>())
                .put(m.timestamp(), new VersionedCell(m.timestamp(), m.value()));
    }

    public List<ColumnKey> liveColumns() {
        lock.lock();
        try {
            List<ColumnKey> out = new ArrayList<>();
            for (var e : cells.entrySet()) {
                VersionedCell newest = e.getValue().lastEntry().getValue();
                if (!newest.isTombstone()) {
                    out.add(e.getKey());
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** Scan columns within a family (qualifier → latest live value). */
    public NavigableMap<String, byte[]> family(String family) {
        lock.lock();
        try {
            NavigableMap<String, byte[]> out = new TreeMap<>();
            for (var e : cells.entrySet()) {
                if (e.getKey().family().equals(family)) {
                    latest(e.getKey()).ifPresent(v -> out.put(e.getKey().qualifier(), v));
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        return liveColumns().isEmpty();
    }
}
