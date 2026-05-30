package colossus.custodian;

/**
 * Executes a {@link WorkItem} against the data plane. The Custodian depends on this interface, not on
 * the D-server module, which keeps the control plane decoupled from the data plane (ADR 0004). The
 * simulator provides the implementation that maps D-server ids to live D-servers and performs the
 * copy / scrub / move, escalating the target's dmClock lane when the priority is REPAIR.
 */
public interface WorkDispatcher {

    /** Result of executing a work item. {@code corruptionFound} is meaningful only for SCRUB. */
    record WorkResult(boolean ok, boolean corruptionFound) {
        public static WorkResult success() {
            return new WorkResult(true, false);
        }

        public static WorkResult scrub(boolean corruptionFound) {
            return new WorkResult(true, corruptionFound);
        }
    }

    WorkResult dispatch(WorkItem item);
}
