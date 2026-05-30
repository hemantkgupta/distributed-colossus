package colossus.common;

/**
 * The logical unit of file allocation. A chunk maps 1:1 to an extent on each replica
 * D-server. Handles are opaque 64-bit ids minted by the Curator on AllocateChunk.
 */
public record ChunkHandle(long id) implements Comparable<ChunkHandle> {
    public static ChunkHandle of(long id) {
        return new ChunkHandle(id);
    }

    /** Zero-padded 16-hex-char rendering, used as a qualifier in {@code locations:} columns. */
    public String hex() {
        return String.format("%016x", id);
    }

    @Override
    public int compareTo(ChunkHandle o) {
        return Long.compare(id, o.id);
    }
}
