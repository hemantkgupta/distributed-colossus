package colossus.dmclock;

/**
 * Observes durability state and reconfigures the REPAIR lane on a durability-floor breach.
 *
 * <p>Under normal operation REPAIR carries a modest reservation below FOREGROUND's, so routine
 * re-replication runs in the background without disturbing client latency. When a PG drops to its
 * last surviving replica (a breach), {@link #onBreach()} swaps in the escalated config whose
 * reservation exceeds FOREGROUND's — the scheduler then consistently picks REPAIR in the
 * reservation phase, client traffic slows, and the bytes get re-replicated before a final loss.
 * {@link #onCleared()} restores the normal config once the floor is back above the threshold.
 */
public final class DurabilityEscalator {

    private final DmClockScheduler scheduler;
    private final LaneConfig normalRepair;
    private final LaneConfig escalatedRepair;
    private boolean escalated = false;

    public DurabilityEscalator(DmClockScheduler scheduler,
                               LaneConfig normalRepair,
                               LaneConfig escalatedRepair) {
        this.scheduler = scheduler;
        this.normalRepair = normalRepair;
        this.escalatedRepair = escalatedRepair;
        scheduler.reconfigure(Lane.REPAIR, normalRepair);
    }

    public synchronized void onBreach() {
        if (!escalated) {
            scheduler.reconfigure(Lane.REPAIR, escalatedRepair);
            escalated = true;
        }
    }

    public synchronized void onCleared() {
        if (escalated) {
            scheduler.reconfigure(Lane.REPAIR, normalRepair);
            escalated = false;
        }
    }

    public synchronized boolean isEscalated() {
        return escalated;
    }

    /** True once REPAIR's reservation has been lifted above FOREGROUND's. */
    public synchronized boolean repairOutranksForeground() {
        return scheduler.configOf(Lane.REPAIR).reservationIops()
                > scheduler.configOf(Lane.FOREGROUND).reservationIops();
    }
}
