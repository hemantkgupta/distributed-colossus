package colossus.common;

import java.time.Instant;
import java.util.Objects;

/**
 * A short delegation from the Curator to one D-server granting it the right to serialize
 * mutations on a specific chunk's extent. Lease records live in the {@code lease:} column
 * family of the file row; the in-memory copy is advisory and re-derived from BigTable.
 */
public record LeaseToken(
        ChunkHandle chunk,
        DserverId primary,
        Instant grantedAt,
        Instant expiresAt) {

    public LeaseToken {
        Objects.requireNonNull(chunk, "chunk");
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(grantedAt, "grantedAt");
        Objects.requireNonNull(expiresAt, "expiresAt");
    }

    public boolean isValid(Instant now) {
        return now.isBefore(expiresAt);
    }

    public LeaseToken renew(Instant now, long durationSeconds) {
        return new LeaseToken(chunk, primary, now, now.plusSeconds(durationSeconds));
    }
}
