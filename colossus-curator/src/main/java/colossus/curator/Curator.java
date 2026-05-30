package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.common.CuratorId;
import colossus.common.FilePath;
import colossus.placement.HashRouter;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A stateless namespace-shard owner. The Curator holds no durable state of its own — its shards'
 * data lives in BigTable, and shard ownership lives in {@code curator.shards.*} rows. Heartbeating
 * and stale-shard claiming are exposed as explicit ticks so failover is deterministically testable;
 * a real deployment drives them on a timer. When a Curator loses a shard (its heartbeat CAS misses)
 * it drops the shard from memory — no data is lost because nothing was only in memory.
 */
public final class Curator {

    private final CuratorId id;
    private final ShardOwnershipRegistry registry;
    private final BigTableClient bt;
    private final PlacementProvider placement;
    private final HashRouter router;
    private final LeaseManager leases;
    private final Clock clock;
    private final long cacheTtlSeconds;
    private final long staleThresholdSeconds;
    private final AtomicLong handleSeq;

    private final ConcurrentHashMap<String, CuratorShard> ownedShards = new ConcurrentHashMap<>();

    public Curator(CuratorId id, BigTableClient bt, ShardOwnershipRegistry registry,
                   PlacementProvider placement, HashRouter router, LeaseManager leases,
                   Clock clock, long cacheTtlSeconds, long staleThresholdSeconds) {
        this.id = id;
        this.bt = bt;
        this.registry = registry;
        this.placement = placement;
        this.router = router;
        this.leases = leases;
        this.clock = clock;
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.staleThresholdSeconds = staleThresholdSeconds;
        // Per-Curator handle range to avoid cross-Curator handle collisions without coordination.
        long base = (long) (Math.abs(id.hostPort().hashCode()) % 1000) * 1_000_000_000L;
        this.handleSeq = new AtomicLong(base);
    }

    public CuratorId id() {
        return id;
    }

    private CuratorShard newShard(String prefix) {
        return new CuratorShard(prefix, bt, placement, router, leases, handleSeq, clock, cacheTtlSeconds);
    }

    /** Explicitly take ownership of a shard (bootstrap / assignment). */
    public CuratorShard assumeShard(String prefix) {
        registry.assumeOwnership(prefix, id, clock.instant());
        CuratorShard shard = newShard(prefix);
        ownedShards.put(prefix, shard);
        return shard;
    }

    /** Refresh heartbeats for owned shards; drop any shard whose ownership we have lost. */
    public void heartbeatTick() {
        Instant now = clock.instant();
        for (String prefix : List.copyOf(ownedShards.keySet())) {
            boolean stillOwner = registry.heartbeat(prefix, id, now);
            if (!stillOwner) {
                ownedShards.remove(prefix);
            }
        }
    }

    /** Scan for shards with stale heartbeats and claim them by CAS. Returns prefixes newly claimed. */
    public List<String> claimStaleTick() {
        Instant now = clock.instant();
        List<String> claimed = new java.util.ArrayList<>();
        for (String prefix : registry.staleShards(now, staleThresholdSeconds)) {
            if (ownedShards.containsKey(prefix)) {
                continue;
            }
            if (registry.claimIfStale(prefix, id, now, staleThresholdSeconds)) {
                ownedShards.put(prefix, newShard(prefix));
                claimed.add(prefix);
            }
        }
        return claimed;
    }

    /** The owned shard responsible for a path (longest matching prefix), if this Curator owns it. */
    public Optional<CuratorShard> shardFor(FilePath path) {
        CuratorShard best = null;
        for (CuratorShard shard : ownedShards.values()) {
            if (shard.owns(path) && (best == null || shard.prefix().length() > best.prefix().length())) {
                best = shard;
            }
        }
        return Optional.ofNullable(best);
    }

    public boolean owns(String prefix) {
        return ownedShards.containsKey(prefix);
    }

    public int ownedShardCount() {
        return ownedShards.size();
    }
}
