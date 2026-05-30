package colossus.common;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** Cursor-based reader, the dual of {@link WireWriter}. Throws on truncated input. */
public final class WireReader {
    private final byte[] data;
    private int pos;

    public WireReader(byte[] data) {
        this.data = data;
    }

    private void need(int n) {
        if (pos + n > data.length) {
            throw new IllegalStateException("truncated wire payload: need " + n
                    + " bytes at offset " + pos + " of " + data.length);
        }
    }

    public int u8() {
        need(1);
        return data[pos++] & 0xFF;
    }

    public int i32() {
        need(4);
        int v = ((data[pos] & 0xFF) << 24) | ((data[pos + 1] & 0xFF) << 16)
                | ((data[pos + 2] & 0xFF) << 8) | (data[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    public long i64() {
        need(8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (data[pos + i] & 0xFF);
        }
        pos += 8;
        return v;
    }

    public String str() {
        need(2);
        int len = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
        pos += 2;
        need(len);
        String s = new String(data, pos, len, StandardCharsets.UTF_8);
        pos += len;
        return s;
    }

    public byte[] bytes() {
        int len = i32();
        need(len);
        byte[] b = new byte[len];
        System.arraycopy(data, pos, b, 0, len);
        pos += len;
        return b;
    }

    public byte[] shortBytes() {
        need(2);
        int len = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
        pos += 2;
        need(len);
        byte[] b = new byte[len];
        System.arraycopy(data, pos, b, 0, len);
        pos += len;
        return b;
    }

    public ChunkHandle chunkHandle() {
        return ChunkHandle.of(i64());
    }

    public RowKey rowKey() {
        return new RowKey(shortBytes());
    }

    public ColumnKey columnKey() {
        return ColumnKey.of(str(), str());
    }

    public Instant timestamp() {
        return Instant.ofEpochMilli(i64());
    }

    public boolean bool() {
        return u8() != 0;
    }

    public boolean hasRemaining() {
        return pos < data.length;
    }

    public int remaining() {
        return data.length - pos;
    }
}
