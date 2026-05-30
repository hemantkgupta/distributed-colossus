package colossus.dmclock;

/**
 * The three virtual-time tags computed for a request at submission. A request becomes eligible
 * once {@code limit <= virtualClock}; within eligible requests, the reservation phase picks the
 * smallest {@code reservation} tag that is due, else the weight phase picks the smallest
 * {@code proportion} tag.
 */
public record Tag(double reservation, double proportion, double limit) {}
