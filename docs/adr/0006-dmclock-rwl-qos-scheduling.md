# ADR 0006 — dmClock R/W/L QoS scheduling at the D-server

**Status:** Accepted

## Context
Foreground client IO and background repair/scrub/rebalance share the same disks; without isolation a repair storm starves clients.

## Decision
Each D-server schedules IO with dmClock (distributed mClock): three lanes (FOREGROUND, BACKGROUND, REPAIR), each with Reservation / Weight / Limit. Dispatch is virtual-time ordered: reservation phase first, then weighted-proportion phase respecting limits.

## Consequences
+ Explicit isolation budgets; foreground p99 stays within its reservation under background load. − Parameters are static config knobs here (real systems auto-tune).
