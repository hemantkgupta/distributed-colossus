# ADR 0014 — RS(6,3) erasure coding for sealed cold extents

**Status:** Accepted

## Context
3× replication costs 200% storage overhead. For the cold majority of an exabyte that is the dominant cost. But open, mutable extents cannot be cheaply erasure-coded — an append would force recomputing all parity.

## Decision
Keep replication as the default and the only path for open/hot extents. Once an extent is sealed (immutable) and has cooled, the Custodian's tier loop re-encodes it from 3× replication to Reed–Solomon RS(6,3): 6 data + 3 parity fragments over GF(2^8) (Cauchy generator → every 6-of-9 subset reconstructs), scattered across distinct fault domains, then the surplus replicas are reclaimed. The codec lives in a dependency-free colossus-erasure module.

## Consequences
+ Storage drops 3.0× → 1.5× for cold data; tolerates any 3 simultaneous losses. − A lost fragment is repaired by reading k=6 fragments (6× repair amplification vs 1× for replication) — paid on the background lane. − EC is sealed-only; the replicated→RS transition and degraded-read reconstruction add machinery (sealing, tier loop, fragment storage).
