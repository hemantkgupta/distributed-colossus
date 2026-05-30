package colossus.curator;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.LeaseToken;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Reply value types returned by Curator namespace operations. */
public final class Results {
    private Results() {}

    public record FileStat(boolean exists, long size, Instant mtime, int chunkCount) {
        public static FileStat absent() {
            return new FileStat(false, 0, Instant.EPOCH, 0);
        }
    }

    /** Result of AllocateChunk: the new handle, its replica set, the leased primary, and the epoch. */
    public record AllocateChunkResult(
            ChunkHandle handle,
            int chunkIndex,
            List<DserverId> dservers,
            LeaseToken lease,
            int placementEpoch) {
        public DserverId primary() {
            return lease.primary();
        }
    }

    /** Result of GetChunkLocations for one chunk index. */
    public record ChunkLocation(
            ChunkHandle handle,
            List<DserverId> dservers,
            int placementEpoch,
            Optional<LeaseToken> lease) {}
}
