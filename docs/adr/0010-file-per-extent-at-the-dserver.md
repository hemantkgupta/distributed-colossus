# ADR 0010 — File-per-extent at the D-server

**Status:** Accepted

## Context
Real D-servers run on raw block devices with O_DIRECT and custom kernel modules.

## Decision
Model each extent as a local file (<handle>.ext) plus a CRC32C sidecar (per 64 KB block) and a meta sidecar. fsync before ack is real; O_DIRECT and raw block I/O are modeled, not enforced via syscall flags.

## Consequences
+ Durability invariant (fsync-before-ack) is preserved and testable. − Page-cache effects differ from production; not a performance reference.
