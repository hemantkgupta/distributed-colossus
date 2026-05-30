# ADR 0001 — Sharded master over a KV store

**Status:** Accepted

## Context
GFS hit a hard ceiling: the entire namespace lived in one in-RAM Master (~50 PB, ~10^9 files), with multi-hundred-ms lock pauses and multi-second GC stalls. Ceph answered by removing the master entirely (CRUSH + monitors). Google chose the opposite.

## Decision
Keep the master abstraction but shard it across stateless Curator processes whose durable state lives in a BigTable-like transactional KV store. Correctness bottoms out in BigTable row-level atomicity.

## Consequences
+ Exabyte scale; seconds-class Curator failover (no checkpoint/log replay). − Accepts BigTable as a load-bearing dependency; a clone needs an equivalent (FoundationDB/TiKV/Cockroach) or, as here, an in-tree pedagogical version.
