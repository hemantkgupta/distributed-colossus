package colossus.curator;

import colossus.common.ChunkHandle;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.LeaseToken;
import colossus.common.WireReader;
import colossus.common.WireWriter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes/decodes the cell values of the namespace row schema (§5.2). One file = one row; the
 * column families are meta:, chunks:, locations:, lease:, scrub:. Keeping the byte layout here means
 * the Curator never special-cases serialization — it just CASes opaque cell values on the file row.
 */
public final class NamespaceCodec {

    private NamespaceCodec() {}

    // ---- column keys ----
    public static final ColumnKey META_SIZE = ColumnKey.of("meta", "size");
    public static final ColumnKey META_CTIME = ColumnKey.of("meta", "ctime");
    public static final ColumnKey META_MTIME = ColumnKey.of("meta", "mtime");
    public static final ColumnKey META_OWNER = ColumnKey.of("meta", "owner");

    public static ColumnKey chunkAt(int index) {
        return ColumnKey.of("chunks", Integer.toString(index));
    }

    public static ColumnKey locationsOf(ChunkHandle h) {
        return ColumnKey.of("locations", h.hex());
    }

    public static ColumnKey leaseOf(ChunkHandle h) {
        return ColumnKey.of("lease", h.hex());
    }

    public static final ColumnKey SCRUB_LAST = ColumnKey.of("scrub", "last_completed");

    // ---- value codecs ----
    public static byte[] longBytes(long v) {
        return new WireWriter().i64(v).toByteArray();
    }

    public static long toLong(byte[] b) {
        return new WireReader(b).i64();
    }

    public static byte[] handleBytes(ChunkHandle h) {
        return new WireWriter().i64(h.id()).toByteArray();
    }

    public static ChunkHandle toHandle(byte[] b) {
        return ChunkHandle.of(new WireReader(b).i64());
    }

    public static byte[] dserverListBytes(List<DserverId> dservers) {
        WireWriter w = new WireWriter().i32(dservers.size());
        for (DserverId d : dservers) {
            w.str(d.hostPort());
        }
        return w.toByteArray();
    }

    public static List<DserverId> toDserverList(byte[] b) {
        WireReader r = new WireReader(b);
        int n = r.i32();
        List<DserverId> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new DserverId(r.str()));
        }
        return out;
    }

    public static byte[] leaseBytes(LeaseToken lease) {
        return new WireWriter()
                .i64(lease.chunk().id())
                .str(lease.primary().hostPort())
                .i64(lease.grantedAt().toEpochMilli())
                .i64(lease.expiresAt().toEpochMilli())
                .toByteArray();
    }

    public static LeaseToken toLease(byte[] b) {
        WireReader r = new WireReader(b);
        ChunkHandle chunk = ChunkHandle.of(r.i64());
        DserverId primary = new DserverId(r.str());
        Instant granted = Instant.ofEpochMilli(r.i64());
        Instant expires = Instant.ofEpochMilli(r.i64());
        return new LeaseToken(chunk, primary, granted, expires);
    }
}
