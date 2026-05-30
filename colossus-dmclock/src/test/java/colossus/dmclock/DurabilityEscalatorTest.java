package colossus.dmclock;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DurabilityEscalatorTest {

    private static final LaneConfig FG = new LaneConfig(1000, 4, 5000);
    private static final LaneConfig BG = new LaneConfig(100, 1, 1000);
    private static final LaneConfig REPAIR_NORMAL = new LaneConfig(200, 1, 1500);
    private static final LaneConfig REPAIR_ESCALATED = new LaneConfig(2000, 4, 5000);

    private static Map<Lane, LaneConfig> baseConfig() {
        EnumMap<Lane, LaneConfig> m = new EnumMap<>(Lane.class);
        m.put(Lane.FOREGROUND, FG);
        m.put(Lane.BACKGROUND, BG);
        m.put(Lane.REPAIR, REPAIR_NORMAL);
        return m;
    }

    @Test
    void breachFlipsRepairAboveForeground() {
        DmClockScheduler s = new DmClockScheduler(baseConfig(), 6000);
        DurabilityEscalator esc = new DurabilityEscalator(s, REPAIR_NORMAL, REPAIR_ESCALATED);

        assertThat(esc.isEscalated()).isFalse();
        assertThat(esc.repairOutranksForeground()).isFalse();
        assertThat(s.configOf(Lane.REPAIR).reservationIops()).isEqualTo(200);

        esc.onBreach();
        assertThat(esc.isEscalated()).isTrue();
        assertThat(esc.repairOutranksForeground()).isTrue();
        assertThat(s.configOf(Lane.REPAIR).reservationIops()).isEqualTo(2000);

        esc.onCleared();
        assertThat(esc.isEscalated()).isFalse();
        assertThat(esc.repairOutranksForeground()).isFalse();
        assertThat(s.configOf(Lane.REPAIR).reservationIops()).isEqualTo(200);
    }

    @Test
    void breachIsIdempotent() {
        DmClockScheduler s = new DmClockScheduler(baseConfig(), 6000);
        DurabilityEscalator esc = new DurabilityEscalator(s, REPAIR_NORMAL, REPAIR_ESCALATED);
        esc.onBreach();
        esc.onBreach();
        assertThat(s.configOf(Lane.REPAIR).reservationIops()).isEqualTo(2000);
        esc.onCleared();
        esc.onCleared();
        assertThat(s.configOf(Lane.REPAIR).reservationIops()).isEqualTo(200);
    }

    @Test
    void escalationShiftsDispatchTowardRepair() {
        long repairNormal = repairCountUnder(REPAIR_NORMAL, false);
        long repairEscalated = repairCountUnder(REPAIR_ESCALATED, true);

        // Escalation must materially increase REPAIR's share of dispatches.
        assertThat(repairEscalated).isGreaterThan(repairNormal);
    }

    @Test
    void underEscalationRepairReservationFloorIsHonoredAndReachesForeground() {
        // FG floods. Escalated REPAIR reserves 2000 iops and matches FG's weight, so over ~1
        // virtual second REPAIR is guaranteed its ~2000 floor and is no longer starved below FG
        // (equal weights drive the surplus to parity — that is mClock-correct).
        DmClockScheduler s = new DmClockScheduler(baseConfig(), 6000);
        DurabilityEscalator esc = new DurabilityEscalator(s, REPAIR_NORMAL, REPAIR_ESCALATED);
        esc.onBreach();
        for (int i = 0; i < 100_000; i++) {
            s.submit(Request.of(Lane.FOREGROUND, 4096, i));
            s.submit(Request.of(Lane.REPAIR, 4096, i));
        }
        for (int i = 0; i < 6000; i++) {
            s.pollNext();
        }
        assertThat(s.dispatchedOn(Lane.REPAIR)).isGreaterThanOrEqualTo(1900L);   // reservation floor honored
        assertThat(s.dispatchedOn(Lane.REPAIR))
                .isGreaterThanOrEqualTo(s.dispatchedOn(Lane.FOREGROUND));        // no longer below FG
    }

    @Test
    void withoutEscalationRepairStaysBelowForegroundUnderFlood() {
        // The contrast case: normal REPAIR (reservation 200, weight 1) loses to FG (weight 4).
        DmClockScheduler s = new DmClockScheduler(baseConfig(), 6000);
        new DurabilityEscalator(s, REPAIR_NORMAL, REPAIR_ESCALATED); // no breach
        for (int i = 0; i < 100_000; i++) {
            s.submit(Request.of(Lane.FOREGROUND, 4096, i));
            s.submit(Request.of(Lane.REPAIR, 4096, i));
        }
        for (int i = 0; i < 6000; i++) {
            s.pollNext();
        }
        assertThat(s.dispatchedOn(Lane.REPAIR)).isLessThan(s.dispatchedOn(Lane.FOREGROUND));
    }

    private static long repairCountUnder(LaneConfig repairCfg, boolean breach) {
        DmClockScheduler s = new DmClockScheduler(baseConfig(), 6000);
        DurabilityEscalator esc = new DurabilityEscalator(s, REPAIR_NORMAL, REPAIR_ESCALATED);
        if (breach) {
            esc.onBreach();
        }
        for (int i = 0; i < 100_000; i++) {
            s.submit(Request.of(Lane.FOREGROUND, 4096, i));
            s.submit(Request.of(Lane.REPAIR, 4096, i));
        }
        for (int i = 0; i < 6000; i++) {
            s.pollNext();
        }
        return s.dispatchedOn(Lane.REPAIR);
    }
}
