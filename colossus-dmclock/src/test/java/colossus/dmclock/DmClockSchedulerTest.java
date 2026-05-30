package colossus.dmclock;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class DmClockSchedulerTest {

    private static Map<Lane, LaneConfig> cfg(LaneConfig fg, LaneConfig bg, LaneConfig repair) {
        EnumMap<Lane, LaneConfig> m = new EnumMap<>(Lane.class);
        m.put(Lane.FOREGROUND, fg);
        m.put(Lane.BACKGROUND, bg);
        m.put(Lane.REPAIR, repair);
        return m;
    }

    private static void flood(DmClockScheduler s, Lane lane, int n) {
        for (int i = 0; i < n; i++) {
            s.submit(Request.of(lane, 4096, i));
        }
    }

    private static void drainN(DmClockScheduler s, int n) {
        for (int i = 0; i < n; i++) {
            if (s.pollNext().isEmpty()) {
                return;
            }
        }
    }

    @Test
    void threeLaneDispatchDrainsEverything() {
        DmClockScheduler s = new DmClockScheduler(
                cfg(new LaneConfig(1000, 4, 5000),
                        new LaneConfig(100, 1, 1000),
                        new LaneConfig(200, 1, 1500)),
                6000);
        flood(s, Lane.FOREGROUND, 50);
        flood(s, Lane.BACKGROUND, 30);
        flood(s, Lane.REPAIR, 20);

        int seen = 0;
        while (true) {
            Optional<TaggedRequest> r = s.pollNext();
            if (r.isEmpty()) break;
            seen++;
        }
        assertThat(seen).isEqualTo(100);
        assertThat(s.isEmpty()).isTrue();
        assertThat(s.dispatchedOn(Lane.FOREGROUND)
                + s.dispatchedOn(Lane.BACKGROUND)
                + s.dispatchedOn(Lane.REPAIR)).isEqualTo(100L);
    }

    @Test
    void weightProportionalityUnderSaturation() {
        // Reservations 0, limits non-binding → surplus splits by weight (FG:BG = 4:1).
        DmClockScheduler s = new DmClockScheduler(
                cfg(new LaneConfig(0, 4, 1_000_000),
                        new LaneConfig(0, 1, 1_000_000),
                        new LaneConfig(0, 1, 1_000_000)),
                1000);
        flood(s, Lane.FOREGROUND, 10_000);
        flood(s, Lane.BACKGROUND, 10_000);
        drainN(s, 1000);

        double ratio = (double) s.dispatchedOn(Lane.FOREGROUND) / s.dispatchedOn(Lane.BACKGROUND);
        assertThat(ratio).isBetween(3.5, 4.5); // ~4:1 by weight
    }

    @Test
    void reservationCannotBeStarvedByHugeWeight() {
        // FG reserves 300 iops; BG has astronomically higher weight but zero reservation.
        // Reservation isolation means FG still gets ~its reserved share within one virtual second.
        DmClockScheduler s = new DmClockScheduler(
                cfg(new LaneConfig(300, 1, 1000),
                        new LaneConfig(0, 1_000_000, 1000),
                        new LaneConfig(0, 1, 1000)),
                1000);
        flood(s, Lane.FOREGROUND, 10_000);
        flood(s, Lane.BACKGROUND, 10_000);
        drainN(s, 1000); // ~1 virtual second

        assertThat(s.dispatchedOn(Lane.FOREGROUND)).isBetween(250L, 360L);
        assertThat(s.dispatchedOn(Lane.BACKGROUND)).isGreaterThan(600L);
    }

    @Test
    void limitCapsALane() {
        // FG limited to 100 iops; over ~1 virtual second it cannot exceed ~100 dispatches.
        DmClockScheduler s = new DmClockScheduler(
                cfg(new LaneConfig(0, 1, 100),
                        new LaneConfig(0, 1, 1000),
                        new LaneConfig(0, 1, 1000)),
                1000);
        flood(s, Lane.FOREGROUND, 10_000);
        flood(s, Lane.BACKGROUND, 10_000);
        drainN(s, 1000);

        assertThat(s.dispatchedOn(Lane.FOREGROUND)).isLessThanOrEqualTo(105L);
        assertThat(s.dispatchedOn(Lane.BACKGROUND)).isGreaterThan(850L);
    }

    @Test
    void emptySchedulerPollsEmpty() {
        DmClockScheduler s = new DmClockScheduler(
                cfg(new LaneConfig(1000, 4, 5000),
                        new LaneConfig(100, 1, 1000),
                        new LaneConfig(200, 1, 1500)),
                6000);
        assertThat(s.pollNext()).isEmpty();
        assertThat(s.totalDispatched()).isZero();
    }
}
