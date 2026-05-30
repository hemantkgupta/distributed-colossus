# Peering and ownership

## Curator shard ownership (the failover primitive)

Ownership of a namespace prefix lives in the `curator.shards.<prefix>` BigTable row (`owner`, `hb`).
The owner heartbeats every few seconds. An idle Curator that scans and finds a heartbeat older than
the stale threshold (default 15 s) claims the shard with a single row CAS against the observed
(owner, heartbeat) pair — concurrent claimants lose the CAS, so exactly one wins. A deposed Curator
discovers it lost ownership when its next heartbeat CAS misses, and drops the shard from memory.

There is **no log replay and no checkpoint reload** — the new owner serves from a cold cache backed
by BigTable. Recovery is bounded by `staleThreshold + one CAS round-trip + the client retry`.

## BigTable tablet ownership

A tablet is replicated synchronously across N tablet-servers; the first reachable member coordinates
writes and replicates the decided mutations to the others. A write needs a quorum of reachable
members; below quorum, writes fail (`QuorumLostException`) while reads continue from a survivor.
Routing (range → tablet) lives in the MasterTablet and is a floor lookup; a split updates it
atomically and clients re-route on `NotOwnerException`.
