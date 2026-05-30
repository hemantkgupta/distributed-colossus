package colossus.dmclock;

/**
 * The dmClock R/W/L budget for a lane.
 *
 * @param reservationIops a hard floor — IOs per virtual second the lane is guaranteed
 *                        regardless of contention (served in the reservation phase).
 * @param weight          relative share of surplus capacity (proportion phase). A lane with
 *                        weight 4 receives ~4x the surplus of a weight-1 lane.
 * @param limitIops       a hard ceiling — the lane is never served faster than this rate.
 */
public record LaneConfig(int reservationIops, int weight, int limitIops) {
    public LaneConfig {
        if (reservationIops < 0 || limitIops <= 0 || weight <= 0) {
            throw new IllegalArgumentException("reservation>=0, weight>0, limit>0 required");
        }
        if (limitIops < reservationIops) {
            throw new IllegalArgumentException("limit must be >= reservation");
        }
    }
}
