package colossus.dmclock;

/** A unit of IO submitted to the scheduler. {@code seq} is a submission ordinal for tracing. */
public record Request(Lane lane, int ioBytes, long seq) {
    public static Request of(Lane lane, int ioBytes, long seq) {
        return new Request(lane, ioBytes, seq);
    }
}
