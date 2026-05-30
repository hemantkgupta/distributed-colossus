# ADR 0009 — Pedagogical pure-Java BigTable

**Status:** Accepted

## Context
Real Colossus depends on production Bigtable (on Spanner + Chubby + Borg). A faithful clone needs the same surface without the production substrate.

## Decision
Ship an in-tree row + column-family KV with tablets, per-tablet WAL + LSM flush, synchronous within-tablet replication, single-elected owner per range, and tablet split. No rocksdbjni; the sharding mechanism is explicit and inspectable.

## Consequences
+ Every architectural claim traces to readable source. − Not production-grade; the tablet scheduler and Paxos-class quorum are simplified.
