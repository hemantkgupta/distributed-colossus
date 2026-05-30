package colossus.simulator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/** A hand-advanced clock shared by the whole simulated cell so failover timing is deterministic. */
public final class SimClock extends Clock {
    private volatile Instant now;

    public SimClock(Instant start) {
        this.now = start;
    }

    public void advanceSeconds(long s) {
        now = now.plusSeconds(s);
    }

    public Instant now() {
        return now;
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
