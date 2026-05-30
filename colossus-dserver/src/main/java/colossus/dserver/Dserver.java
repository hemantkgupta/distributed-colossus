package colossus.dserver;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.dmclock.DmClockScheduler;
import colossus.dmclock.Lane;
import colossus.dmclock.LaneConfig;
import colossus.dmclock.Request;
import colossus.placement.DserverStatusTable;

import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The data-plane daemon. Hosts extent files, holds leases as primary, participates in one position
 * of the daisy chain, and schedules all of its IO through a {@link DmClockScheduler}. It depends on
 * neither Curator nor Custodian (ADR 0004 / the data ⟂ control invariant). The write path is
 * GFS-inherited: bytes are pushed down the chain (PushBytes), then a serialized commit is applied at
 * each replica with an fsync before the ack flows back.
 */
public final class Dserver {

    private record PendingWrite(long offset, byte[] bytes) {}

    private final DserverId id;
    private final ExtentStore store;
    private final LeaseHolder leaseHolder = new LeaseHolder();
    private final DmClockScheduler scheduler;
    private final DserverStatusTable statusTable; // nullable in unit tests
    private final long diskFreeBytes;

    private volatile Dserver downstream; // next node in the daisy chain
    private final ConcurrentHashMap<ChunkHandle, PendingWrite> pending = new ConcurrentHashMap<>();
    private final Set<ChunkHandle> hosted = ConcurrentHashMap.newKeySet();
    private final AtomicLong reqSeq = new AtomicLong();

    public Dserver(DserverId id, ExtentStore store, DmClockScheduler scheduler,
                   DserverStatusTable statusTable, long diskFreeBytes) {
        this.id = id;
        this.store = store;
        this.scheduler = scheduler;
        this.statusTable = statusTable;
        this.diskFreeBytes = diskFreeBytes;
    }

    public static DmClockScheduler defaultScheduler() {
        Map<Lane, LaneConfig> cfg = new EnumMap<>(Lane.class);
        cfg.put(Lane.FOREGROUND, new LaneConfig(1000, 4, 5000));
        cfg.put(Lane.BACKGROUND, new LaneConfig(100, 1, 1000));
        cfg.put(Lane.REPAIR, new LaneConfig(200, 1, 1500));
        return new DmClockScheduler(cfg, 6000);
    }

    public DserverId id() {
        return id;
    }

    public ExtentStore store() {
        return store;
    }

    public LeaseHolder leaseHolder() {
        return leaseHolder;
    }

    public DmClockScheduler scheduler() {
        return scheduler;
    }

    public void setDownstream(Dserver downstream) {
        this.downstream = downstream;
    }

    public int hostedExtentCount() {
        return hosted.size();
    }

    private void schedule(Lane lane, int ioBytes) {
        scheduler.submit(new Request(lane, ioBytes, reqSeq.getAndIncrement()));
    }

    // ---- CP21 daisy chain ----

    /** Stage bytes locally and forward down the chain. The whole chain buffers before any commit. */
    public void pushBytes(ChunkHandle handle, long offset, byte[] bytes) {
        pending.put(handle, new PendingWrite(offset, bytes));
        schedule(Lane.FOREGROUND, bytes.length);
        if (downstream != null) {
            downstream.pushBytes(handle, offset, bytes);
        }
    }

    /**
     * Primary commit: assign a serial under the lease, apply locally (fsync), and forward the commit
     * down the chain. Returns the assigned serial once every replica has applied.
     */
    public long commitWrite(ChunkHandle handle, Instant now) {
        long serial = leaseHolder.nextSerial(handle, now); // rejects if not the lease holder
        applyCommit(handle, serial);
        return serial;
    }

    /** Apply a committed write at this replica and forward to the next (secondary/tertiary path). */
    public void applyCommit(ChunkHandle handle, long serial) {
        PendingWrite w = pending.remove(handle);
        if (w != null) {
            store.writeAt(handle, w.offset(), w.bytes());
            schedule(Lane.FOREGROUND, w.bytes().length);
            hosted.add(handle);
        }
        if (downstream != null) {
            downstream.applyCommit(handle, serial);
        }
    }

    // ---- CP22 foreground read through dmClock ----

    public byte[] read(ChunkHandle handle, long offset, int size) {
        schedule(Lane.FOREGROUND, size);
        return store.read(handle, offset, size);
    }

    /** Background/repair copy of an extent from a source D-server (used by the Custodian). */
    public void copyExtentFrom(ChunkHandle handle, Dserver source, Lane lane) {
        long len = source.store.length(handle);
        byte[] all = source.store.read(handle, 0, (int) len);
        schedule(lane, all.length);
        store.writeAt(handle, 0, all);
        hosted.add(handle);
    }

    // ---- CP19 heartbeat ----

    public void heartbeat(Instant now) {
        if (statusTable != null) {
            statusTable.writeStatus(id, now, diskFreeBytes, hosted.size());
        }
    }

    public Optional<PendingWrite> peekPending(ChunkHandle handle) {
        return Optional.ofNullable(pending.get(handle));
    }
}
