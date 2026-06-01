# ADR 0015 — Lazy garbage collection of orphaned extents

**Status:** Accepted

## Context
A delete must be cheap and must not stall the namespace, but the extents it orphans have to be reclaimed eventually. A partitioned D-server can also return holding extents no live file references.

## Decision
Make delete a single metadata CAS that tombstones the file row — instant, moves zero bytes. Reclamation is a separate, lazy Custodian GC loop (the GFS-reaper lineage): it scans for extents that no live file row references and reclaims them on the background QoS lane (DELETE_EXTENT dispatch / Dserver.dropExtent). Orphan-detection is a property of the current metadata, not of any event ordering.

## Consequences
+ No foreground latency spike on delete; an undelete window; the same sweep cleans both deleted-file extents and partition stragglers, so it is race- and partition-safe. − Space is reclaimed with a delay (eventual, not immediate); a crash mid-sweep just leaves orphans for the next pass.
