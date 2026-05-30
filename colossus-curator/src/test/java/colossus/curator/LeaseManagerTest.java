package colossus.curator;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.common.LeaseToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LeaseManagerTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private final FilePath path = FilePath.of("/photos/a");
    private final ChunkHandle handle = ChunkHandle.of(4242);
    private final DserverId primary = DserverId.of("d0", 7100);

    @Test
    void grantThenExpire(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        LeaseToken lease = f.leases.grant(path, handle, primary, t0);
        assertThat(lease.primary()).isEqualTo(primary);
        assertThat(f.leases.isValid(path, handle, t0.plusSeconds(59))).isTrue();
        assertThat(f.leases.isValid(path, handle, t0.plusSeconds(61))).isFalse(); // 60 s lease lapsed
    }

    @Test
    void renewExtendsExpiry(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        f.leases.grant(path, handle, primary, t0);
        Optional<LeaseToken> renewed = f.leases.renew(path, handle, t0.plusSeconds(30));
        assertThat(renewed).isPresent();
        assertThat(renewed.get().expiresAt()).isEqualTo(t0.plusSeconds(90));
        assertThat(f.leases.isValid(path, handle, t0.plusSeconds(89))).isTrue();
    }

    @Test
    void revokeClearsLease(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        f.leases.grant(path, handle, primary, t0);
        f.leases.revoke(path, handle);
        assertThat(f.leases.get(path, handle)).isEmpty();
        assertThat(f.leases.isValid(path, handle, t0)).isFalse();
    }

    @Test
    void leaseSurvivesReadThroughBigTable(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        f.leases.grant(path, handle, primary, t0);
        // A fresh LeaseManager over the same BigTable sees the lease (durable, not in-memory).
        LeaseManager other = new LeaseManager(f.bt, 60);
        assertThat(other.get(path, handle)).hasValueSatisfying(l ->
                assertThat(l.primary()).isEqualTo(primary));
    }

    @Test
    void renewMissingLeaseReturnsEmpty(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        assertThat(f.leases.renew(path, ChunkHandle.of(999), t0)).isEmpty();
    }
}
