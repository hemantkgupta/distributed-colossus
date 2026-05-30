package colossus.placement;

import colossus.common.PgId;

import java.nio.charset.StandardCharsets;

/**
 * Maps a blob key (or chunk handle string) to a Placement Group via a stable hash modulo the PG
 * count. Pure and deterministic — the same key always lands in the same PG across processes and
 * runs, so no per-object location metadata is needed; only the small PG→D-server table is looked up.
 * This is the "hash" half of hybrid placement (ADR 0005). Uses 64-bit FNV-1a (no JDK
 * String.hashCode dependence, which is only 32-bit and weaker).
 */
public final class HashRouter {

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final int pgCount;

    public HashRouter(int pgCount) {
        if (pgCount <= 0) {
            throw new IllegalArgumentException("pgCount must be > 0");
        }
        this.pgCount = pgCount;
    }

    public int pgCount() {
        return pgCount;
    }

    public static long hash64(String key) {
        long h = FNV_OFFSET;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xff);
            h *= FNV_PRIME;
        }
        return h;
    }

    public PgId placementGroupFor(String blobKey) {
        return PgId.of(Math.floorMod(hash64(blobKey), pgCount));
    }
}
