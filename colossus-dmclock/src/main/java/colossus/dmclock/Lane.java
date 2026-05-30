package colossus.dmclock;

/**
 * The three QoS lanes at a D-server. FOREGROUND is client read/write traffic; BACKGROUND is
 * routine Custodian work (rebalance, routine repair, scrub); REPAIR is escalated repair that,
 * under a durability-floor breach, is reconfigured to outrank FOREGROUND.
 */
public enum Lane {
    FOREGROUND,
    BACKGROUND,
    REPAIR
}
