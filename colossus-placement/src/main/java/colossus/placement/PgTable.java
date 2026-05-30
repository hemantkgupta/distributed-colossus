package colossus.placement;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Row;
import colossus.common.ColumnKey;
import colossus.common.DserverId;
import colossus.common.PgId;
import colossus.common.RowKey;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * The PG→D-server map, stored as a regular BigTable table ({@code placement.pg_table}) — Colossus
 * eats its own dogfood, so the placement table shards and splits exactly like the namespace. One
 * row per PG: {@code dservers:0..n} (replica set in primary order), {@code epoch:current} (bumped on
 * any replica-set change), {@code policy:rule}. The Curator reads this on GetChunkLocations; the
 * Custodian reads it to find under-replication and writes it back after a repair.
 */
public final class PgTable {

    private static final String DSERVERS = "dservers";
    private static final ColumnKey EPOCH = ColumnKey.of("epoch", "current");
    private static final ColumnKey POLICY = ColumnKey.of("policy", "rule");

    private final BigTableClient bt;
    private long ts = 0;

    public PgTable(BigTableClient bt) {
        this.bt = bt;
    }

    private RowKey key(PgId pg) {
        return RowKey.of(pg.rowKey());
    }

    /** Set (or replace) a PG's replica set, bumping its epoch. */
    public synchronized PgPlacement assign(PgId pg, List<DserverId> dservers, String policy) {
        int newEpoch = lookup(pg).map(p -> p.epoch() + 1).orElse(1);
        RowKey row = key(pg);
        long t = ++ts;
        // Clear stale replica slots beyond the new size, then write the new set.
        Optional<PgPlacement> prev = lookup(pg);
        int prevSize = prev.map(p -> p.dservers().size()).orElse(0);
        for (int i = 0; i < dservers.size(); i++) {
            bt.put(row, ColumnKey.of(DSERVERS, Integer.toString(i)), t,
                    dservers.get(i).hostPort().getBytes(StandardCharsets.UTF_8));
        }
        for (int i = dservers.size(); i < prevSize; i++) {
            bt.delete(row, ColumnKey.of(DSERVERS, Integer.toString(i)), t);
        }
        bt.put(row, EPOCH, t, intBytes(newEpoch));
        bt.put(row, POLICY, t, policy.getBytes(StandardCharsets.UTF_8));
        return new PgPlacement(pg, dservers, newEpoch, policy);
    }

    public Optional<PgPlacement> lookup(PgId pg) {
        Optional<Row> rowOpt = bt.getRow(key(pg));
        if (rowOpt.isEmpty()) {
            return Optional.empty();
        }
        Row row = rowOpt.get();
        NavigableMap<String, byte[]> ds = row.family(DSERVERS);
        if (ds.isEmpty()) {
            return Optional.empty();
        }
        List<DserverId> dservers = new ArrayList<>();
        for (var e : ds.entrySet()) {
            dservers.add(new DserverId(new String(e.getValue(), StandardCharsets.UTF_8)));
        }
        int epoch = row.get(EPOCH).map(PgTable::toInt).orElse(0);
        String policy = row.get(POLICY).map(b -> new String(b, StandardCharsets.UTF_8)).orElse("");
        return Optional.of(new PgPlacement(pg, dservers, epoch, policy));
    }

    private static byte[] intBytes(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private static int toInt(byte[] b) {
        return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
    }
}
