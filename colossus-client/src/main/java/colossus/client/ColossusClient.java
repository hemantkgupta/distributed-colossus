package colossus.client;

import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.curator.CuratorShard;
import colossus.curator.Results;
import colossus.dserver.Dserver;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The public Colossus API. It asks a Curator for metadata (one cached RPC per file) and streams
 * bytes directly to and from D-servers — the Curator never sees payload bytes. Writes lay a file out
 * across fixed-size chunks; each chunk is allocated by the Curator (one row CAS), its lease is handed
 * to the primary, and the bytes are driven down the daisy chain. Reads resolve a chunk's replicas via
 * the Curator and pull from the nearest one. A path whose Curator is mid-failover surfaces as a
 * brief WRONG_SHARD condition; the client retries.
 */
public final class ColossusClient {

    private final ClusterView view;
    private final int chunkSize;
    private static final int MAX_SHARD_RETRIES = 5;

    public ColossusClient(ClusterView view, int chunkSize) {
        this.view = view;
        this.chunkSize = chunkSize;
    }

    private CuratorShard shard(FilePath path) {
        for (int i = 0; i < MAX_SHARD_RETRIES; i++) {
            Optional<CuratorShard> s = view.curatorFor(path);
            if (s.isPresent()) {
                return s.get();
            }
        }
        throw new WrongShardException(path);
    }

    public boolean create(FilePath path, String owner) {
        return shard(path).createFile(path, owner);
    }

    public Results.FileStat stat(FilePath path) {
        return shard(path).stat(path);
    }

    public boolean delete(FilePath path) {
        return shard(path).delete(path);
    }

    /** Write a whole file: lay {@code data} across as many chunks as needed. */
    public void write(FilePath path, byte[] data) {
        CuratorShard shard = shard(path);
        int offset = 0;
        while (offset < data.length) {
            int len = Math.min(chunkSize, data.length - offset);
            byte[] piece = new byte[len];
            System.arraycopy(data, offset, piece, 0, len);
            writeOneChunk(shard, path, piece);
            offset += len;
        }
    }

    /** Append one chunk's worth of bytes and return the new chunk index. */
    public int append(FilePath path, byte[] bytes) {
        return writeOneChunk(shard(path), path, bytes).chunkIndex();
    }

    private Results.AllocateChunkResult writeOneChunk(CuratorShard shard, FilePath path, byte[] piece) {
        Results.AllocateChunkResult alloc = shard.allocateChunk(path);
        Dserver primary = view.dserver(alloc.primary());
        // The Curator delegated the lease to this primary; make the primary aware of it (GRANT_LEASE).
        primary.leaseHolder().grant(alloc.lease());
        List<Dserver> tail = new ArrayList<>();
        List<DserverId> replicas = alloc.dservers();
        for (int i = 1; i < replicas.size(); i++) {
            tail.add(view.dserver(replicas.get(i)));
        }
        primary.pushBytes(alloc.handle(), 0, piece, tail);
        primary.commitWrite(alloc.handle(), alloc.lease().grantedAt(), tail);
        return alloc;
    }

    /** Read {@code size} bytes starting at {@code offset}, spanning chunks as needed. */
    public byte[] read(FilePath path, long offset, int size) {
        CuratorShard shard = shard(path);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long pos = offset;
        int remaining = size;
        while (remaining > 0) {
            int chunkIndex = (int) (pos / chunkSize);
            long within = pos % chunkSize;
            Optional<Results.ChunkLocation> loc = shard.getChunkLocations(path, chunkIndex);
            if (loc.isEmpty() || loc.get().dservers().isEmpty()) {
                break; // EOF — no such chunk
            }
            Dserver d = view.dserver(loc.get().dservers().get(0)); // nearest replica
            int want = (int) Math.min(remaining, chunkSize - within);
            byte[] part = d.read(loc.get().handle(), within, want);
            if (part.length == 0) {
                break;
            }
            out.writeBytes(part);
            pos += part.length;
            remaining -= part.length;
            if (part.length < want) {
                break; // short read → EOF within this chunk
            }
        }
        return out.toByteArray();
    }

    /** Thrown when the owning Curator could not be resolved after retries (mid-failover). */
    public static final class WrongShardException extends RuntimeException {
        public WrongShardException(FilePath path) {
            super("could not resolve owning Curator for " + path.path());
        }
    }
}
