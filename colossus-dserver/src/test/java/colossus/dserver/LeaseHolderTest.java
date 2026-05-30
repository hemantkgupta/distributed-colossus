package colossus.dserver;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.LeaseToken;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeaseHolderTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private final ChunkHandle h = ChunkHandle.of(7);

    private LeaseToken lease(Instant granted, long secs) {
        return new LeaseToken(h, DserverId.of("d0", 7100), granted, granted.plusSeconds(secs));
    }

    @Test
    void primarySerializesMutationsMonotonically() {
        LeaseHolder holder = new LeaseHolder();
        holder.grant(lease(t0, 60));
        assertThat(holder.nextSerial(h, t0)).isEqualTo(1);
        assertThat(holder.nextSerial(h, t0)).isEqualTo(2);
        assertThat(holder.nextSerial(h, t0)).isEqualTo(3);
        assertThat(holder.currentSerial(h)).isEqualTo(3);
    }

    @Test
    void commitWithoutLeaseRejected() {
        LeaseHolder holder = new LeaseHolder();
        assertThatThrownBy(() -> holder.nextSerial(h, t0))
                .isInstanceOf(LeaseHolder.NotLeaseHolderException.class);
    }

    @Test
    void expiredLeaseRejectsSerial() {
        LeaseHolder holder = new LeaseHolder();
        holder.grant(lease(t0, 60));
        assertThat(holder.holdsValid(h, t0.plusSeconds(30))).isTrue();
        assertThatThrownBy(() -> holder.nextSerial(h, t0.plusSeconds(61)))
                .isInstanceOf(LeaseHolder.NotLeaseHolderException.class);
    }

    @Test
    void renewExtendsValidity() {
        LeaseHolder holder = new LeaseHolder();
        holder.grant(lease(t0, 60));
        holder.renew(lease(t0.plusSeconds(40), 60));
        assertThat(holder.holdsValid(h, t0.plusSeconds(90))).isTrue();
    }

    @Test
    void revokeDropsLease() {
        LeaseHolder holder = new LeaseHolder();
        holder.grant(lease(t0, 60));
        holder.revoke(h);
        assertThat(holder.holdsValid(h, t0)).isFalse();
    }
}
