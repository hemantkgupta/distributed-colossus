package colossus.custodian;

import colossus.common.DserverId;
import colossus.common.PgId;

import java.util.Optional;

/**
 * A unit of background work the Custodian dispatches to a D-server. The Custodian decides <em>what</em>
 * to do and at what priority; an external {@link WorkDispatcher} (wired by the simulator to real
 * D-servers) executes it. The priority maps to a dmClock lane at the target: BACKGROUND for routine
 * work, REPAIR for a durability-floor breach.
 */
public record WorkItem(Op op, PgId pg, Optional<DserverId> source, Optional<DserverId> target, Priority priority) {

    public enum Op {
        REPAIR_COPY,    // re-replicate a PG's extents onto a fresh target
        SCRUB,          // deep-verify CRCs across replicas
        REBALANCE_MOVE, // migrate extents off a hot D-server
        TIER_DOWN,      // (v1: logs the decision only)
        DELETE_EXTENT   // reclaim an orphaned extent from a D-server (no live file row references it)
    }

    /** Maps to a dmClock lane at the target D-server. */
    public enum Priority {
        BACKGROUND,
        REPAIR
    }

    public static WorkItem repair(PgId pg, DserverId source, DserverId target, Priority priority) {
        return new WorkItem(Op.REPAIR_COPY, pg, Optional.of(source), Optional.of(target), priority);
    }

    public static WorkItem scrub(PgId pg, DserverId primary) {
        return new WorkItem(Op.SCRUB, pg, Optional.of(primary), Optional.empty(), Priority.BACKGROUND);
    }

    public static WorkItem rebalance(PgId pg, DserverId source, DserverId target) {
        return new WorkItem(Op.REBALANCE_MOVE, pg, Optional.of(source), Optional.of(target), Priority.BACKGROUND);
    }

    public static WorkItem tierDown(PgId pg) {
        return new WorkItem(Op.TIER_DOWN, pg, Optional.empty(), Optional.empty(), Priority.BACKGROUND);
    }

    public static WorkItem deleteExtent(PgId pg, DserverId target) {
        return new WorkItem(Op.DELETE_EXTENT, pg, Optional.empty(), Optional.of(target), Priority.BACKGROUND);
    }
}
