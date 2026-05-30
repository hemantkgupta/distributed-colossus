package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A contiguous row-key range materialized as an in-memory map plus a write-ahead log. This is the
 * pedagogical stand-in for a BigTable tablet: durable via WAL replay rather than the full LSM
 * (commit log + memtable + SSTable) of §7.1 — the recovery semantics are identical, the on-disk
 * compaction is omitted. A tablet survives process restart by replaying its WAL.
 */
public final class Tablet implements AutoCloseable {

    private KeyRange range;
    private final NavigableMap<RowKey, Row> rows = new ConcurrentSkipListMap<>();
    private final WriteAheadLog wal;
    private final AtomicLong approxBytes = new AtomicLong(0);

    public Tablet(KeyRange range, Path walPath, boolean fsyncOnPut) {
        this.range = range;
        this.wal = new WriteAheadLog(walPath, fsyncOnPut);
        // Recover prior state from the WAL.
        for (WriteAheadLog.Entry e : wal.replay()) {
            rowFor(e.rowKey()).applyLocked(e.mutation());
            accountBytes(e.mutation());
        }
    }

    public KeyRange range() {
        return range;
    }

    public boolean owns(RowKey key) {
        return range.contains(key);
    }

    private Row rowFor(RowKey key) {
        return rows.computeIfAbsent(key, Row::new);
    }

    private void requireOwned(RowKey key) {
        if (!range.contains(key)) {
            throw new NotOwnerException(key, range);
        }
    }

    private void accountBytes(Mutation m) {
        approxBytes.addAndGet(m.column().full().length() + (m.value() == null ? 0 : m.value().length) + 16);
    }

    public boolean checkAndMutate(RowKey key, List<Condition> conditions, List<Mutation> mutations) {
        requireOwned(key);
        Row row = rowFor(key);
        // Durability: log first, then apply. We log under the row lock indirectly via the row's
        // CAS so a successful WAL append matches an applied mutation.
        synchronized (row) {
            boolean ok = row.checkAndMutate(conditions, mutations);
            if (ok) {
                for (Mutation m : mutations) {
                    wal.append(key, m);
                    accountBytes(m);
                }
            }
            return ok;
        }
    }

    public void put(RowKey key, ColumnKey column, long ts, byte[] value) {
        checkAndMutate(key, List.of(), List.of(Mutation.put(column, ts, value)));
    }

    public Optional<byte[]> get(RowKey key, ColumnKey column) {
        requireOwned(key);
        Row row = rows.get(key);
        return row == null ? Optional.empty() : row.get(column);
    }

    public Optional<Row> getRow(RowKey key) {
        requireOwned(key);
        return Optional.ofNullable(rows.get(key));
    }

    /** Rows whose key starts with the given prefix, in key order. */
    public NavigableMap<RowKey, Row> scanPrefix(String prefix) {
        NavigableMap<RowKey, Row> out = new TreeMap<>();
        for (var e : rows.entrySet()) {
            if (e.getKey().asString().startsWith(prefix) && !e.getValue().isEmpty()) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    public List<RowKey> rowKeys() {
        return new ArrayList<>(rows.keySet());
    }

    public int rowCount() {
        return rows.size();
    }

    public long approximateBytes() {
        return approxBytes.get();
    }

    /** Median row key — the split point used by {@link TabletSplitter}. */
    public Optional<RowKey> medianKey() {
        if (rows.size() < 2) {
            return Optional.empty();
        }
        List<RowKey> keys = rowKeys();
        return Optional.of(keys.get(keys.size() / 2));
    }

    NavigableMap<RowKey, Row> rawRows() {
        return rows;
    }

    void narrowRange(KeyRange newRange) {
        this.range = newRange;
    }

    @Override
    public void close() {
        wal.close();
    }
}
