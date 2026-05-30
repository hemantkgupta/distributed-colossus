# ADR 0005 — Hybrid hash + lookup placement

**Status:** Accepted

## Context
Per-object location metadata explodes at exabyte scale; pure deterministic placement (CRUSH) makes map propagation the bottleneck.

## Decision
Object hash → Placement Group (small fixed set, ~10^4–10^6 PGs). PG → D-servers is a small KV lookup in a placement table (itself stored in BigTable). A monotonic epoch detects stale client caches.

## Consequences
+ Hash bounds per-object metadata; lookup bounds map-propagation cost. − An extra indirection vs CRUSH; the placement table is another BigTable-backed structure to scale.
