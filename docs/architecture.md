# Architecture

## Module graph (acyclic)

```
colossus-common  (value types + wire format; no deps)
   ↑
   ├── colossus-dmclock        → common
   ├── colossus-erasure        → no deps
   ├── colossus-bigtable       → common
   ├── colossus-placement      → common, bigtable
   ├── colossus-dserver        → common, dmclock, bigtable        [DATA PLANE]
   ├── colossus-curator        → common, bigtable, placement      [CONTROL PLANE]
   ├── colossus-custodian      → common, bigtable, placement      [CONTROL PLANE]
   ├── colossus-client         → common, bigtable, placement, curator, dserver [API]
   └── colossus-simulator      → ALL                              [HARNESS]
```

Two structural invariants encoded in the build:

1. **Data plane ⟂ control plane.** `colossus-dserver` does not depend on `colossus-curator`
   or `colossus-custodian`. Disaggregation is a build-time constraint.
2. **Curator ⟂ Custodian.** Neither depends on the other. The Custodian reconciles against
   *observed* cluster state read directly from BigTable — it is never told what to do by the
   Curator.

## Plane mapping

- **Clients** → talk to Curators for metadata (one RPC, cached for lease duration) and to
  D-servers directly for bytes. The Curator never sees payload bytes.
- **Control plane** → Curators (namespace shards) + Custodians (background reconciliation).
- **Data plane** → D-servers hosting extent files, daisy-chain replication.
- **Metadata substrate** → BigTable (namespace rows + placement table + status rows +
  shard-ownership rows). The single source of durable truth.

## Phasing — 33 CPs across 7 phases

| Phase | CPs | Theme |
|---|---|---|
| 1 | 1–4 | Foundation: value types, wire format, dmClock scheduler + durability escalator |
| 2 | 5–10 | BigTable: Row/CAS, Tablet+WAL, TabletServer replication, MasterTablet routing, split, client |
| 3 | 11–12 | Placement: HashRouter, PgTable + epoch-invalidated cache |
| 4 | 13–17 | Curator: shard boot, ownership registry + heartbeat, CAS reassignment, namespace ops, leases |
| 5 | 18–22 | D-server: ExtentStore + CRC, heartbeat, lease holder, daisy chain, dmClock integration |
| 6 | 23–26 | Custodian loops, durability escalation, ColossusClient e2e, chaos simulator |
| 7 | 27–33 | Extent sealing, pure-Java RS(6,3), simulator-level re-encode and degraded reads |

Each CP is a green-build deliverable; each phase boundary is an auto-commit + push.
