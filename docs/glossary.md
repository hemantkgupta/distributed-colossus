# Glossary

- **BigTable** — pedagogical in-tree row + column-family KV underlying the metadata (ADR 0009).
- **Curator** — stateless namespace-shard owner; durable state in BigTable. Replaces the GFS Master.
- **Curator shard** — a namespace prefix range owned by one Curator; ownership is a BigTable row CAS.
- **Custodian** — stateless background reconciler: repair / scrub / rebalance / tier loops.
- **D-server** — data-plane daemon hosting extent files; daisy-chain replication; dmClock-scheduled IO.
- **Extent** — physical unit on a D-server: `<handle>.ext` + `.crc` (per-64KB-block CRC32C).
- **Chunk** — logical 64 MB allocation unit; maps 1:1 to an extent per replica.
- **Placement Group (PG)** — indirection between chunks and D-servers; chunk hashes to a PG, PG maps to a replica set.
- **PG table** — `placement.pg_table` BigTable table: PG → replica set, epoch, policy.
- **Tablet** — a row-key range, owned by one tablet-server, synchronously replicated to N peers.
- **dmClock** — QoS scheduler with Reservation / Weight / Limit budgets per lane (FOREGROUND / BACKGROUND / REPAIR).
- **Durability-floor breach** — a PG below the critical survivor count (1 of 3); triggers REPAIR-lane escalation.
- **Lease** — 60 s delegation letting one D-server serialize mutations on a chunk.
- **Daisy chain** — replication topology: client → primary → secondary → tertiary; serial assigned at primary.
- **Placement epoch** — monotonic int on a PG row; carried on D-server RPCs for staleness detection.
- **CAS** — BigTable row-level conditional mutation; the atomicity primitive.
