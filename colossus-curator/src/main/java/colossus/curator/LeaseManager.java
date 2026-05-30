package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.common.LeaseToken;

import java.time.Instant;
import java.util.Optional;

/**
 * Issues short (default 60 s) leases that delegate the right to serialize mutations on a chunk to
 * one primary D-server. Lease records live in the {@code lease:} column family of the file row, so a
 * lease survives a Curator restart — the new Curator reads the lease from BigTable rather than
 * re-granting blindly. Expiry is wall-clock; a lapsed lease lets the Curator grant to a new primary.
 */
public final class LeaseManager {

    private final BigTableClient bt;
    private final long durationSeconds;
    private long ts = 0;

    public LeaseManager(BigTableClient bt, long durationSeconds) {
        this.bt = bt;
        this.durationSeconds = durationSeconds;
    }

    public synchronized LeaseToken grant(FilePath path, ChunkHandle handle, DserverId primary, Instant now) {
        LeaseToken token = new LeaseToken(handle, primary, now, now.plusSeconds(durationSeconds));
        bt.put(path.toRowKey(), NamespaceCodec.leaseOf(handle), ++ts, NamespaceCodec.leaseBytes(token));
        return token;
    }

    public Optional<LeaseToken> get(FilePath path, ChunkHandle handle) {
        return bt.get(path.toRowKey(), NamespaceCodec.leaseOf(handle)).map(NamespaceCodec::toLease);
    }

    public boolean isValid(FilePath path, ChunkHandle handle, Instant now) {
        return get(path, handle).map(t -> t.isValid(now)).orElse(false);
    }

    public synchronized Optional<LeaseToken> renew(FilePath path, ChunkHandle handle, Instant now) {
        Optional<LeaseToken> existing = get(path, handle);
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        LeaseToken renewed = existing.get().renew(now, durationSeconds);
        bt.put(path.toRowKey(), NamespaceCodec.leaseOf(handle), ++ts, NamespaceCodec.leaseBytes(renewed));
        return Optional.of(renewed);
    }

    public synchronized void revoke(FilePath path, ChunkHandle handle) {
        bt.delete(path.toRowKey(), NamespaceCodec.leaseOf(handle), ++ts);
    }
}
