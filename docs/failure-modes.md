# Failure modes and recovery

Mirrors implementation-plan §9; each row is exercised by a test (mostly in `colossus-simulator`).

| Failure | Detection | Recovery |
|---|---|---|
| Curator process dies | `curator.shards.*` heartbeat stale | Idle Curator CAS-claims the shard (<~20 s); cold-cache serve; no namespace pause. `SimulatorIntegrationTest.curatorDeathDoesNotPauseNamespace` |
| D-server dies | `dserver.status.*` stale (15 s) | Custodian detects under-replicated PGs; repairs on BACKGROUND lane. `dserverDeathTriggersCustodianRepair` |
| Down to 1 replica | Custodian counts survivors | REPAIR lane escalated above FOREGROUND; bytes outrun a third loss. `durabilityFloorBreachEscalatesRepairLane` |
| Disk corruption | Scrub CRC mismatch | Repair from a healthy peer. `CustodianTest.scrubRunsWhenOverdueAndCorruptionSchedulesRepair` |
| Tablet-server quorum lost (2 of 3) | Quorum check fails on write | Writes fail; reads continue from a survivor; operator restores quorum. `tabletServerQuorumLossFailsWritesButReadsContinue` |
| Tablet hot-spot | Row/byte threshold crossed | Median-key split; brief NotOwner blip; client re-routes. `BigTableClientTest.splitIsTransparentToClient...` |
| Custodian dies | none (data plane unaffected) | Replacement rescans BigTable and resumes (stateless) |
| Concurrent chunk allocation | Index-slot CAS miss | Loser retries against the higher index. `concurrentAppendersGetDistinctChunks` |
| Total PG loss (0 survivors) | Custodian sees 0 up | Operator-only restore (out of v1 scope) |
