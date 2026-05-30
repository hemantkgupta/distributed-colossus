# ADR 0003 — Stateless Curators with KV-backed state

**Status:** Accepted

## Context
GFS Master recovery loaded a checkpoint and replayed the op-log tail — minute-class for large namespaces and a stop-the-world event.

## Decision
Curators hold only an in-memory LRU cache; all durable state is in BigTable. A Curator restart is a cold-cache fetch, not a checkpoint replay. Shard ownership is a BigTable row CAS.

## Consequences
+ Any Curator can die and be replaced in seconds with no namespace-wide pause. − A cold Curator's first request per file pays a BigTable round-trip; mitigated by the LRU cache.
