package colossus.dmclock;

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * A centralized dmClock (distributed mClock) scheduler for one D-server's IO.
 *
 * <p>Semantics: R/W/L are rates in IOs per <em>virtual second</em>. The virtual clock advances by
 * {@code 1/capacityIops} per dispatched IO, so {@code capacityIops} dispatches fill one virtual
 * second. A lane reserved for {@code R} thus receives ~{@code R/capacity} of dispatches as a floor;
 * its limit caps it at {@code L/capacity}; surplus beyond reservations is split by weight.
 *
 * <p>Tag assignment at submission (the mClock construction):
 * <pre>
 *   reservation = max(lastReservation + 1/R, vt)
 *   proportion  = max(lastProportion  + 1/W, vt)   // 1/weight spacing → proportional sharing
 *   limit       = max(lastLimit       + 1/L, vt)
 * </pre>
 *
 * <p>Dispatch is two-phase over the lanes whose head is <em>eligible</em> ({@code limit <= vt}):
 * the reservation phase serves the smallest reservation tag that is due ({@code <= vt}); otherwise
 * the weight phase serves the smallest proportion tag. If nothing is eligible, the clock idles
 * forward to the earliest limit tag. Within a lane, tags are monotonic, so the FIFO head is the
 * lane's minimum — no per-lane priority queue is needed.
 */
public final class DmClockScheduler {

    private final EnumMap<Lane, LaneConfig> configs = new EnumMap<>(Lane.class);
    private final EnumMap<Lane, ArrayDeque<TaggedRequest>> queues = new EnumMap<>(Lane.class);
    private final EnumMap<Lane, Tag> lastTag = new EnumMap<>(Lane.class);

    private final double capacityIops;
    private double vt = 0.0;
    private long dispatched = 0;
    private final EnumMap<Lane, Long> dispatchCount = new EnumMap<>(Lane.class);

    public DmClockScheduler(Map<Lane, LaneConfig> laneConfigs, double capacityIops) {
        if (capacityIops <= 0) {
            throw new IllegalArgumentException("capacityIops must be > 0");
        }
        this.capacityIops = capacityIops;
        for (Lane lane : Lane.values()) {
            LaneConfig cfg = laneConfigs.get(lane);
            if (cfg == null) {
                throw new IllegalArgumentException("missing config for lane " + lane);
            }
            configs.put(lane, cfg);
            queues.put(lane, new ArrayDeque<>());
            lastTag.put(lane, new Tag(0, 0, 0));
            dispatchCount.put(lane, 0L);
        }
    }

    /** Replace a lane's budget at runtime (used by {@link DurabilityEscalator}). */
    public synchronized void reconfigure(Lane lane, LaneConfig cfg) {
        configs.put(lane, cfg);
    }

    public synchronized LaneConfig configOf(Lane lane) {
        return configs.get(lane);
    }

    public synchronized void submit(Request req) {
        Lane lane = req.lane();
        LaneConfig cfg = configs.get(lane);
        Tag prev = lastTag.get(lane);
        Tag tag = new Tag(
                Math.max(prev.reservation() + 1.0 / cfg.reservationIops(), vt),
                Math.max(prev.proportion() + 1.0 / cfg.weight(), vt),
                Math.max(prev.limit() + 1.0 / cfg.limitIops(), vt));
        // reservationIops can be 0 → 1/0 = +Inf, meaning "never due in reservation phase".
        queues.get(lane).addLast(new TaggedRequest(req, tag));
        lastTag.put(lane, tag);
    }

    public synchronized boolean isEmpty() {
        return queues.values().stream().allMatch(ArrayDeque::isEmpty);
    }

    public synchronized long totalDispatched() {
        return dispatched;
    }

    public synchronized long dispatchedOn(Lane lane) {
        return dispatchCount.get(lane);
    }

    /**
     * Pick and remove the next request to service, advancing the virtual clock. Returns empty
     * only when every lane is empty.
     */
    public synchronized Optional<TaggedRequest> pollNext() {
        if (isEmpty()) {
            return Optional.empty();
        }

        // Eligibility: a lane's head is eligible once its limit tag is due. If none is eligible,
        // idle the clock forward to the earliest limit tag (rate limiting forces a gap).
        double minLimit = Double.POSITIVE_INFINITY;
        boolean anyEligible = false;
        for (Lane lane : Lane.values()) {
            TaggedRequest head = queues.get(lane).peek();
            if (head == null) {
                continue;
            }
            if (head.tag().limit() <= vt) {
                anyEligible = true;
            } else {
                minLimit = Math.min(minLimit, head.tag().limit());
            }
        }
        if (!anyEligible) {
            vt = minLimit; // advance to the earliest moment something becomes eligible
        }

        // Reservation phase: among eligible heads with reservation tag due, take the smallest.
        Lane chosen = null;
        double best = Double.POSITIVE_INFINITY;
        for (Lane lane : Lane.values()) {
            TaggedRequest head = queues.get(lane).peek();
            if (head == null || head.tag().limit() > vt) {
                continue;
            }
            if (head.tag().reservation() <= vt && head.tag().reservation() < best) {
                best = head.tag().reservation();
                chosen = lane;
            }
        }

        // Weight phase: no reservation due → smallest proportion tag among eligible heads.
        if (chosen == null) {
            best = Double.POSITIVE_INFINITY;
            for (Lane lane : Lane.values()) {
                TaggedRequest head = queues.get(lane).peek();
                if (head == null || head.tag().limit() > vt) {
                    continue;
                }
                if (head.tag().proportion() < best) {
                    best = head.tag().proportion();
                    chosen = lane;
                }
            }
        }

        if (chosen == null) {
            // Everything still limited even after idling (shouldn't happen since we advanced vt).
            return Optional.empty();
        }

        TaggedRequest out = queues.get(chosen).pollFirst();
        vt += 1.0 / capacityIops;
        dispatched++;
        dispatchCount.merge(chosen, 1L, Long::sum);
        return Optional.of(out);
    }

    public synchronized double virtualClock() {
        return vt;
    }
}
