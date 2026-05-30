package colossus.curator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/** A hand-advanced clock for deterministic lease-expiry and heartbeat-staleness tests. */
final class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant start) {
        this.now = start;
    }

    void advanceSeconds(long s) {
        now = now.plusSeconds(s);
    }

    @Override
    public Instant instant() {
        return now;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
