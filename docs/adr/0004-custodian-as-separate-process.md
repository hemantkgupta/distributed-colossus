# ADR 0004 — Custodian as a separate process

**Status:** Accepted

## Context
In GFS the Master also did background work (re-replication, GC), so recovery storms competed with foreground metadata serving on the same CPU.

## Decision
Run background reconciliation in a separate stateless Custodian process with its own deployment, CPU budget, and dmClock QoS lane. The Custodian reads cluster state directly from BigTable; it is never directed by the Curator.

## Consequences
+ Foreground metadata latency is decoupled from recovery storms. + Custodian and Curator scale independently. − Two control-plane services to operate instead of one.
