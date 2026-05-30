# ADR 0008 — Daisy-chain replication inherited from GFS

**Status:** Accepted

## Context
Colossus must remain a tractable migration target for the twenty internal teams already on GFS.

## Decision
Keep GFS's replication topology unchanged: bytes flow client → primary → secondary → tertiary, each NIC saturated independently; primary assigns a mutation serial. Only the placement function differs (hybrid vs master-driven).

## Consequences
+ Client and operator muscle memory transfers; write path is familiar. − Inherits GFS's chain latency and primary-failure window semantics.
