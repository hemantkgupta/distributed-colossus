package colossus.bigtable;

/** Failure signals surfaced by the replicated metadata layer. */
public final class Faults {
    private Faults() {}

    /** A tablet-server replica is down / partitioned and cannot serve. */
    public static final class UnreachableException extends RuntimeException {
        public UnreachableException(TabletServerId id) {
            super("tablet-server unreachable: " + id);
        }
    }

    /** Fewer than a quorum of replicas are reachable, so the write cannot be made durable. */
    public static final class QuorumLostException extends RuntimeException {
        public QuorumLostException(String tabletId, int up, int quorum) {
            super("tablet " + tabletId + ": only " + up + " replica(s) up, need " + quorum);
        }
    }
}
