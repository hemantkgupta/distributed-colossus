package colossus.placement;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Row;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.RowKey;
import colossus.common.WireReader;
import colossus.common.WireWriter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The {@code dserver.status.*} table in BigTable. Each D-server writes its status row every few
 * seconds; Curators read it to know which replicas are UP when answering GetChunkLocations, and the
 * Custodian reads it to detect under-replication. Liveness is derived from heartbeat staleness: a
 * row older than {@code missedThreshold × interval} (default 15 s) is treated as DOWN. Lives in the
 * placement module so the data plane (writer) and both control planes (readers) can share it.
 */
public final class DserverStatusTable {

    private static final String PREFIX = "dserver.status.";
    private static final ColumnKey HB = ColumnKey.of("hb", "ts");
    private static final ColumnKey DISK_FREE = ColumnKey.of("disk", "free");
    private static final ColumnKey EXTENTS = ColumnKey.of("extents", "count");

    public record DserverStatus(DserverId id, Instant lastHeartbeat, long diskFree, int numExtents) {}

    private final BigTableClient bt;
    private long ts = 0;

    public DserverStatusTable(BigTableClient bt) {
        this.bt = bt;
    }

    private RowKey rowKey(DserverId id) {
        return RowKey.of(PREFIX + id.hostPort());
    }

    public synchronized void writeStatus(DserverId id, Instant now, long diskFree, int numExtents) {
        RowKey row = rowKey(id);
        long t = ++ts;
        bt.put(row, HB, t, new WireWriter().i64(now.toEpochMilli()).toByteArray());
        bt.put(row, DISK_FREE, t, new WireWriter().i64(diskFree).toByteArray());
        bt.put(row, EXTENTS, t, new WireWriter().i32(numExtents).toByteArray());
    }

    public Optional<DserverStatus> get(DserverId id) {
        Optional<Row> rowOpt = bt.getRow(rowKey(id));
        if (rowOpt.isEmpty() || rowOpt.get().get(HB).isEmpty()) {
            return Optional.empty();
        }
        Row row = rowOpt.get();
        Instant hb = Instant.ofEpochMilli(new WireReader(row.get(HB).get()).i64());
        long disk = row.get(DISK_FREE).map(b -> new WireReader(b).i64()).orElse(0L);
        int extents = row.get(EXTENTS).map(b -> new WireReader(b).i32()).orElse(0);
        return Optional.of(new DserverStatus(id, hb, disk, extents));
    }

    public boolean isUp(DserverId id, Instant now, long staleThresholdSeconds) {
        return get(id).map(s -> !now.isAfter(s.lastHeartbeat().plusSeconds(staleThresholdSeconds)))
                .orElse(false);
    }

    /** All D-servers that have ever reported, partitioned by liveness at {@code now}. */
    public List<DserverId> liveDservers(Instant now, long staleThresholdSeconds) {
        List<DserverId> out = new ArrayList<>();
        for (var e : bt.scan(PREFIX).entrySet()) {
            DserverId id = new DserverId(e.getKey().asString().substring(PREFIX.length()));
            if (isUp(id, now, staleThresholdSeconds)) {
                out.add(id);
            }
        }
        return out;
    }

    public List<DserverId> downDservers(Instant now, long staleThresholdSeconds) {
        List<DserverId> out = new ArrayList<>();
        for (var e : bt.scan(PREFIX).entrySet()) {
            DserverId id = new DserverId(e.getKey().asString().substring(PREFIX.length()));
            if (!isUp(id, now, staleThresholdSeconds)) {
                out.add(id);
            }
        }
        return out;
    }
}
