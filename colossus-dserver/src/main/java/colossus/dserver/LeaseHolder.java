package colossus.dserver;

import colossus.common.ChunkHandle;
import colossus.common.LeaseToken;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the leases this D-server holds as primary, and serializes mutations on each leased extent
 * by assigning a monotonically increasing serial number. Only the lease holder may commit a write;
 * a commit attempted without a valid (unexpired) lease is rejected, which is how Colossus inherits
 * GFS's single-writer-per-chunk discipline.
 */
public final class LeaseHolder {

    private final ConcurrentHashMap<ChunkHandle, LeaseToken> held = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChunkHandle, Long> serials = new ConcurrentHashMap<>();

    public void grant(LeaseToken token) {
        held.put(token.chunk(), token);
        serials.putIfAbsent(token.chunk(), 0L);
    }

    public void renew(LeaseToken token) {
        held.put(token.chunk(), token);
    }

    public void revoke(ChunkHandle handle) {
        held.remove(handle);
    }

    public Optional<LeaseToken> lease(ChunkHandle handle) {
        return Optional.ofNullable(held.get(handle));
    }

    public boolean holdsValid(ChunkHandle handle, Instant now) {
        LeaseToken t = held.get(handle);
        return t != null && t.isValid(now);
    }

    /**
     * Allocate the next mutation serial for a leased extent. Throws if no valid lease is held —
     * the D-server is not the primary and must not order writes on this chunk.
     */
    public synchronized long nextSerial(ChunkHandle handle, Instant now) {
        if (!holdsValid(handle, now)) {
            throw new NotLeaseHolderException(handle);
        }
        return serials.merge(handle, 1L, Long::sum);
    }

    public long currentSerial(ChunkHandle handle) {
        return serials.getOrDefault(handle, 0L);
    }

    /** Thrown when a commit is attempted on a chunk this D-server does not hold a valid lease for. */
    public static final class NotLeaseHolderException extends RuntimeException {
        public NotLeaseHolderException(ChunkHandle handle) {
            super("no valid lease held for chunk " + handle.hex());
        }
    }
}
