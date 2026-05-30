package colossus.common;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Append-only writer for the hand-rolled binary payloads. All multi-byte integers are
 * big-endian; strings are 2-byte-length-prefixed UTF-8; byte arrays are 4-byte-length-prefixed.
 * Binary-safe (does not use modified UTF-8).
 */
public final class WireWriter {
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream(64);

    public WireWriter u8(int v) {
        buf.write(v & 0xFF);
        return this;
    }

    public WireWriter i32(int v) {
        buf.write((v >>> 24) & 0xFF);
        buf.write((v >>> 16) & 0xFF);
        buf.write((v >>> 8) & 0xFF);
        buf.write(v & 0xFF);
        return this;
    }

    public WireWriter i64(long v) {
        for (int s = 56; s >= 0; s -= 8) {
            buf.write((int) ((v >>> s) & 0xFF));
        }
        return this;
    }

    /** 2-byte length-prefixed UTF-8. Max length 65535 bytes. */
    public WireWriter str(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > 0xFFFF) {
            throw new IllegalArgumentException("string too long for 2-byte length prefix: " + b.length);
        }
        buf.write((b.length >>> 8) & 0xFF);
        buf.write(b.length & 0xFF);
        buf.write(b, 0, b.length);
        return this;
    }

    /** 4-byte length-prefixed raw bytes. */
    public WireWriter bytes(byte[] b) {
        i32(b.length);
        buf.write(b, 0, b.length);
        return this;
    }

    /** 2-byte length-prefixed raw bytes (used for RowKey / ColumnKey qualifiers). */
    public WireWriter shortBytes(byte[] b) {
        if (b.length > 0xFFFF) {
            throw new IllegalArgumentException("byte[] too long for 2-byte length prefix: " + b.length);
        }
        buf.write((b.length >>> 8) & 0xFF);
        buf.write(b.length & 0xFF);
        buf.write(b, 0, b.length);
        return this;
    }

    public WireWriter chunkHandle(ChunkHandle h) {
        return i64(h.id());
    }

    public WireWriter rowKey(RowKey k) {
        return shortBytes(k.bytes());
    }

    public WireWriter columnKey(ColumnKey c) {
        str(c.family());
        return str(c.qualifier());
    }

    public WireWriter timestamp(Instant t) {
        return i64(t.toEpochMilli());
    }

    public WireWriter bool(boolean b) {
        return u8(b ? 1 : 0);
    }

    public byte[] toByteArray() {
        return buf.toByteArray();
    }
}
