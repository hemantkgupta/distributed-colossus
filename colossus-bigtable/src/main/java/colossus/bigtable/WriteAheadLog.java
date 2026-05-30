package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import colossus.common.WireReader;
import colossus.common.WireWriter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only commit log for a tablet. Each record is a length-prefixed serialization of a
 * (rowKey, mutation) pair; writes are fsync'd per append when {@code fsyncOnPut} so a crash
 * loses at most an un-acked write. Replay tolerates a torn tail: a truncated final record stops
 * recovery cleanly rather than corrupting the tablet.
 */
public final class WriteAheadLog implements AutoCloseable {

    /** One recovered entry: the row it targets plus the mutation to re-apply. */
    public record Entry(RowKey rowKey, Mutation mutation) {}

    private final Path path;
    private final boolean fsyncOnPut;
    private FileOutputStream fos;
    private BufferedOutputStream out;

    public WriteAheadLog(Path path, boolean fsyncOnPut) {
        this.path = path;
        this.fsyncOnPut = fsyncOnPut;
        try {
            Files.createDirectories(path.getParent());
            this.fos = new FileOutputStream(path.toFile(), true); // append
            this.out = new BufferedOutputStream(fos);
        } catch (IOException e) {
            throw new UncheckedIOException("open WAL " + path, e);
        }
    }

    public synchronized void append(RowKey rowKey, Mutation m) {
        WireWriter w = new WireWriter()
                .rowKey(rowKey)
                .str(m.column().family())
                .str(m.column().qualifier())
                .i64(m.timestamp())
                .bool(m.delete());
        if (!m.delete()) {
            w.bytes(m.value());
        }
        byte[] body = w.toByteArray();
        byte[] framed = new WireWriter().i32(body.length).toByteArray();
        try {
            out.write(framed);
            out.write(body);
            out.flush();
            if (fsyncOnPut) {
                fos.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("append WAL " + path, e);
        }
    }

    /** Read every intact record from the log in order. */
    public List<Entry> replay() {
        List<Entry> entries = new ArrayList<>();
        byte[] all;
        try {
            all = Files.exists(path) ? Files.readAllBytes(path) : new byte[0];
        } catch (IOException e) {
            throw new UncheckedIOException("read WAL " + path, e);
        }
        int pos = 0;
        while (pos + 4 <= all.length) {
            WireReader lenR = new WireReader(slice(all, pos, 4));
            int recLen = lenR.i32();
            if (pos + 4 + recLen > all.length) {
                break; // torn tail — stop cleanly
            }
            WireReader r = new WireReader(slice(all, pos + 4, recLen));
            RowKey rowKey = r.rowKey();
            ColumnKey col = ColumnKey.of(r.str(), r.str());
            long ts = r.i64();
            boolean delete = r.bool();
            Mutation m = delete ? Mutation.delete(col, ts) : Mutation.put(col, ts, r.bytes());
            entries.add(new Entry(rowKey, m));
            pos += 4 + recLen;
        }
        return entries;
    }

    private static byte[] slice(byte[] src, int off, int len) {
        byte[] b = new byte[len];
        System.arraycopy(src, off, b, 0, len);
        return b;
    }

    @Override
    public synchronized void close() {
        try {
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException("close WAL " + path, e);
        }
    }
}
