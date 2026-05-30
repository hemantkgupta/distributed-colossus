package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Condition;
import colossus.bigtable.Mutation;
import colossus.bigtable.Row;
import colossus.common.ChunkHandle;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.common.LeaseToken;
import colossus.common.PgId;
import colossus.placement.HashRouter;
import colossus.placement.PgPlacement;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns a namespace prefix range. Holds only an in-memory cache; the durable copy of everything it
 * touches lives in BigTable (ADR 0003), so a shard can be picked up by another Curator with a cold
 * cache and no replay. Every namespace operation is a single row CAS — that is what makes file-level
 * atomicity free (ADR 0002).
 */
public final class CuratorShard {

    /** Cached per-file metadata with the wall-clock time it was read, for TTL invalidation. */
    private record CachedStat(Results.FileStat stat, Instant cachedAt) {}

    private final String prefix;
    private final BigTableClient bt;
    private final PlacementProvider placement;
    private final HashRouter router;
    private final LeaseManager leases;
    private final AtomicLong handleSeq;
    private final Clock clock;
    private final long cacheTtlSeconds;

    private final java.util.Map<FilePath, CachedStat> statCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong coldFetches = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();

    public CuratorShard(String prefix, BigTableClient bt, PlacementProvider placement, HashRouter router,
                        LeaseManager leases, AtomicLong handleSeq, Clock clock, long cacheTtlSeconds) {
        this.prefix = prefix;
        this.bt = bt;
        this.placement = placement;
        this.router = router;
        this.leases = leases;
        this.handleSeq = handleSeq;
        this.clock = clock;
        this.cacheTtlSeconds = cacheTtlSeconds;
    }

    public String prefix() {
        return prefix;
    }

    public boolean owns(FilePath path) {
        return path.path().startsWith(prefix);
    }

    private void invalidate(FilePath path) {
        statCache.remove(path);
    }

    public long coldFetches() {
        return coldFetches.get();
    }

    public long cacheHits() {
        return cacheHits.get();
    }

    // ---- CP16 namespace operations: each is a single CAS on the file row ----

    /** Create a file if absent. Single create-if-absent CAS on meta:size. */
    public boolean createFile(FilePath path, String owner) {
        Instant now = clock.instant();
        long t = now.toEpochMilli();
        boolean created = bt.checkAndMutate(path.toRowKey(),
                List.of(Condition.expectAbsent(NamespaceCodec.META_SIZE)),
                List.of(
                        Mutation.put(NamespaceCodec.META_SIZE, t, NamespaceCodec.longBytes(0)),
                        Mutation.put(NamespaceCodec.META_CTIME, t, NamespaceCodec.longBytes(now.toEpochMilli())),
                        Mutation.put(NamespaceCodec.META_MTIME, t, NamespaceCodec.longBytes(now.toEpochMilli())),
                        Mutation.put(NamespaceCodec.META_OWNER, t, owner.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        invalidate(path);
        return created;
    }

    /** Stat a file — cold-cache fetch from BigTable, then cached for the TTL. */
    public Results.FileStat stat(FilePath path) {
        Instant now = clock.instant();
        CachedStat cached = statCache.get(path);
        if (cached != null && now.isBefore(cached.cachedAt().plusSeconds(cacheTtlSeconds))) {
            cacheHits.incrementAndGet();
            return cached.stat();
        }
        coldFetches.incrementAndGet();
        Results.FileStat stat = readStat(path);
        statCache.put(path, new CachedStat(stat, now));
        return stat;
    }

    private Results.FileStat readStat(FilePath path) {
        Optional<Row> rowOpt = bt.getRow(path.toRowKey());
        if (rowOpt.isEmpty() || rowOpt.get().get(NamespaceCodec.META_SIZE).isEmpty()) {
            return Results.FileStat.absent();
        }
        Row row = rowOpt.get();
        long size = row.get(NamespaceCodec.META_SIZE).map(NamespaceCodec::toLong).orElse(0L);
        long mtime = row.get(NamespaceCodec.META_MTIME).map(NamespaceCodec::toLong).orElse(0L);
        int chunkCount = row.family("chunks").size();
        return new Results.FileStat(true, size, Instant.ofEpochMilli(mtime), chunkCount);
    }

    /**
     * Allocate the next chunk: mint a handle, resolve its PG to a replica set, lease the primary,
     * and record the chunk + locations + lease atomically in one CAS on the file row.
     */
    public Results.AllocateChunkResult allocateChunk(FilePath path) {
        Row row = bt.getRow(path.toRowKey())
                .filter(r -> r.get(NamespaceCodec.META_SIZE).isPresent())
                .orElseThrow(() -> new FileNotFoundException(path));
        int index = row.family("chunks").size();

        ChunkHandle handle = ChunkHandle.of(handleSeq.getAndIncrement());
        PgId pg = router.placementGroupFor(handle.hex());
        PgPlacement placed = placement.ensure(pg);
        List<DserverId> dservers = placed.dservers();

        Instant now = clock.instant();
        long t = now.toEpochMilli();
        LeaseToken lease = new LeaseToken(handle, dservers.get(0), now,
                now.plusSeconds(java.time.Duration.ofMinutes(1).toSeconds()));

        boolean ok = bt.checkAndMutate(path.toRowKey(),
                List.of(Condition.expectAbsent(NamespaceCodec.chunkAt(index))),
                List.of(
                        Mutation.put(NamespaceCodec.chunkAt(index), t, NamespaceCodec.handleBytes(handle)),
                        Mutation.put(NamespaceCodec.locationsOf(handle), t, NamespaceCodec.dserverListBytes(dservers)),
                        Mutation.put(NamespaceCodec.leaseOf(handle), t, NamespaceCodec.leaseBytes(lease)),
                        Mutation.put(NamespaceCodec.META_MTIME, t, NamespaceCodec.longBytes(now.toEpochMilli()))));
        if (!ok) {
            throw new ConcurrentAllocationException(path, index);
        }
        invalidate(path);
        return new Results.AllocateChunkResult(handle, index, dservers, lease, placed.epoch());
    }

    /** Resolve a chunk index to its handle, replica set, placement epoch, and current lease. */
    public Optional<Results.ChunkLocation> getChunkLocations(FilePath path, int chunkIndex) {
        Optional<Row> rowOpt = bt.getRow(path.toRowKey());
        if (rowOpt.isEmpty()) {
            return Optional.empty();
        }
        Row row = rowOpt.get();
        Optional<byte[]> handleBytes = row.get(NamespaceCodec.chunkAt(chunkIndex));
        if (handleBytes.isEmpty()) {
            return Optional.empty();
        }
        ChunkHandle handle = NamespaceCodec.toHandle(handleBytes.get());
        List<DserverId> dservers = row.get(NamespaceCodec.locationsOf(handle))
                .map(NamespaceCodec::toDserverList).orElse(List.of());
        int epoch = placement.lookup(router.placementGroupFor(handle.hex()))
                .map(PgPlacement::epoch).orElse(0);
        Optional<LeaseToken> lease = row.get(NamespaceCodec.leaseOf(handle)).map(NamespaceCodec::toLease);
        return Optional.of(new Results.ChunkLocation(handle, dservers, epoch, lease));
    }

    /** Delete a file: tombstone every live column in the row, guarded by a CAS on existence. */
    public boolean delete(FilePath path) {
        Optional<Row> rowOpt = bt.getRow(path.toRowKey());
        if (rowOpt.isEmpty() || rowOpt.get().get(NamespaceCodec.META_SIZE).isEmpty()) {
            return false;
        }
        Row row = rowOpt.get();
        long sizeNow = row.get(NamespaceCodec.META_SIZE).map(NamespaceCodec::toLong).orElse(0L);
        long t = clock.instant().toEpochMilli();
        List<Mutation> deletes = new ArrayList<>();
        for (ColumnKey col : row.liveColumns()) {
            deletes.add(Mutation.delete(col, t));
        }
        boolean ok = bt.checkAndMutate(path.toRowKey(),
                List.of(Condition.expectValue(NamespaceCodec.META_SIZE, NamespaceCodec.longBytes(sizeNow))),
                deletes);
        invalidate(path);
        return ok;
    }

    public LeaseManager leases() {
        return leases;
    }

    /** Thrown when a chunk-index slot was taken by a concurrent allocation (CAS miss). */
    public static final class ConcurrentAllocationException extends RuntimeException {
        public ConcurrentAllocationException(FilePath path, int index) {
            super("concurrent allocation at index " + index + " for " + path.path());
        }
    }

    /** Thrown when an operation targets a file that does not exist. */
    public static final class FileNotFoundException extends RuntimeException {
        public FileNotFoundException(FilePath path) {
            super("no such file: " + path.path());
        }
    }
}
