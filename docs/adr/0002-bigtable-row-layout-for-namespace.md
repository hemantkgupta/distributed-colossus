# ADR 0002 — BigTable row layout for the namespace

**Status:** Accepted

## Context
A file's metadata (size, chunk list, replica locations, lease) must be openable, readable, writable atomically without cross-row transactions in the common case.

## Decision
One file = one row; row key = file path. Column families: meta:, chunks:, locations:, lease:, scrub:. Row atomicity = file atomicity. Lease grant + chunk allocation are a single CAS on one row.

## Consequences
+ GetChunkLocations is one row read, no joins. − Cross-file operations (subtree rename) need cross-row transactions; degraded to a documented non-serializable two-phase path (see ADR 0013).
