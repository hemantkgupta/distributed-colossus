# ADR 0007 — Durability-threshold escalation

**Status:** Accepted

## Context
Routine under-replication can wait, but a PG down to its last replica is a data-loss emergency that must not queue behind foreground traffic.

## Decision
On a durability-floor breach (e.g. 1 of 3 replicas surviving) the Custodian escalates repair from the BACKGROUND lane to the REPAIR lane, whose escalated reservation exceeds FOREGROUND's. Client traffic slows; bytes survive.

## Consequences
+ Bytes-survive guarantee under critical loss. − Foreground latency degrades during escalation; this is the intended trade.
