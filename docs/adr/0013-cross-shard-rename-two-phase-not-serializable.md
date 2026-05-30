# ADR 0013 — Cross-shard rename: two-phase, not strictly serializable

**Status:** Accepted

## Context
A rename whose source and destination fall in different Curator shards spans two BigTable rows in (potentially) different tablets. Strict serializability needs Spanner-class cross-row transactions.

## Decision
Single-shard rename is one CAS-pair. Cross-shard rename ships as a client-driven two-phase protocol (PREPARE on source → COMMIT on dest → FINALIZE delete on source) with a RENAMING marker; a recovery scan resolves dangling state. It is explicitly NOT strictly serializable under concurrent renames.

## Consequences
+ Sufficient for v1; the limitation is documented, not hidden. − Spanner is the real answer; concurrent renames of overlapping paths can interleave incorrectly.
