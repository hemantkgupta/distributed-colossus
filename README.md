# distributed-colossus

A faithful, pedagogical Java implementation of **Google Colossus** — the GFS successor
that scaled to exabytes by *sharding the master rather than removing it*.

This repo is the companion code for the Colossus deep-dive and implementation plan in the
CSE wiki (`raw-blog/distributed-file-system/colossus.md` and
`colossus-implementation-plan.md`). The plan is the *what* and *how*; the deep-dive is the *why*.

## The one-sentence architecture

GFS hit a ceiling because the namespace lived in one in-RAM Master. Colossus keeps the
master abstraction but **shards it across stateless `Curator` processes whose durable state
lives in a BigTable-like KV store**, adds a separate background reconciler (`Custodian`) with
its own QoS lane, and addresses data through a **hybrid hash→PG→lookup placement** layer.
The data plane (`D-servers`, daisy-chain replication, 64 MB chunks, CRC blocks, leases) is
inherited from GFS almost unchanged.

## Module map

| Module | Plane | Purpose |
|---|---|---|
| `colossus-common` | — | Value-type records + hand-rolled binary wire format |
| `colossus-dmclock` | data-plane lib | R/W/L QoS scheduler (FOREGROUND / BACKGROUND / REPAIR lanes) |
| `colossus-erasure` | durability lib | Pure-Java Reed-Solomon RS(6,3) codec for sealed cold extents |
| `colossus-bigtable` | metadata substrate | Pedagogical row + column-family KV with tablets, WAL, replication, split |
| `colossus-placement` | metadata substrate | Hash→PG router + PG→D-server lookup table (stored in BigTable) |
| `colossus-curator` | control plane | Stateless namespace shard owner; durable state in BigTable |
| `colossus-custodian` | control plane | Background reconciler: repair / scrub / rebalance / tier loops |
| `colossus-dserver` | data plane | Extent store, CRC, daisy-chain replication, dmClock-scheduled IO |
| `colossus-client` | API | Public create/stat/delete/write/append/read API over Curators + D-servers |
| `colossus-simulator` | harness | Multi-process chaos harness, fault injector, and erasure-coding lifecycle |

The Gradle dependency edges enforce **disaggregation**: the data plane does not depend on the
control plane, and the Curator and Custodian do not depend on each other (the Custodian
operates on *observed* cluster state in BigTable, never on Curator directives).

## Build

```bash
./gradlew build --console=plain
```

Requires JDK 17 (pinned via `.java-version`). No JNI, no native dependencies — pure JDK + Gradle.

## What is faithful vs stubbed

Faithful: sharded stateless Curators, BigTable row-per-file layout with row-atomic CAS,
seconds-class Curator failover via row-CAS ownership transfer, hybrid placement, dmClock
R/W/L QoS, durability-threshold escalation, daisy-chain replication, CRC scrub, sealed-extent
RS(6,3) erasure coding with degraded reads.

Stubbed for pedagogy: BigTable is in-tree (not real Bigtable/Spanner/Chubby/Borg);
single-cell (no multi-DC); erasure-coded fragments are simulator-local files rather than a
production Custodian tiering service; cross-shard rename is two-phase but not strictly
serializable; extents are files on local FS (O_DIRECT modeled, not enforced). See `docs/adr/`
and the implementation plan §13 for the full list.

## Phasing

33 checkpoints across 7 phases — see `docs/architecture.md`. Each phase is a green-build,
auto-committed boundary.

---

*Co-authored with Claude. Part of the `distributed-file-system` paper arc:
GFS → Ceph RADOS → **Colossus** → BlueStore.*
