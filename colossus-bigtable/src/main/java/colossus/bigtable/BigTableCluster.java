package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-process BigTable cell: a {@link MasterTablet} routing table over a set of
 * {@link ReplicatedTablet} groups, hosted on a fixed pool of {@link TabletServer}s. Routing,
 * synchronous within-tablet replication, and median-key splitting are all modeled faithfully; the
 * production tablet scheduler and LSM compaction are not (ADR 0009). This is the substrate the
 * Curator, Custodian, and placement library all share.
 */
public final class BigTableCluster implements AutoCloseable {

    private final Path dataDir;
    private final List<TabletServer> servers;
    private final int rf;
    private final int rowSplitThreshold;
    private final long byteSplitThreshold;
    private final boolean fsync;

    private final MasterTablet master;
    private final ConcurrentHashMap<String, ReplicatedTablet> groups = new ConcurrentHashMap<>();
    private final AtomicInteger tabletSeq = new AtomicInteger(0);

    public BigTableCluster(Path dataDir, int rf, int rowSplitThreshold, long byteSplitThreshold, boolean fsync) {
        this.dataDir = dataDir;
        this.rf = rf;
        this.rowSplitThreshold = rowSplitThreshold;
        this.byteSplitThreshold = byteSplitThreshold;
        this.fsync = fsync;
        this.servers = new ArrayList<>();
        for (int i = 0; i < rf; i++) {
            servers.add(new TabletServer(TabletServerId.of("ts" + i)));
        }
        this.master = new MasterTablet(dataDir.resolve("master.log"), fsync);
        // Seed one tablet covering the whole keyspace.
        String first = newTabletId();
        groups.put(first, hostGroup(first, KeyRange.all()));
        master.assignRange(KeyRange.MIN, first);
    }

    private String newTabletId() {
        return String.format("t%04d", tabletSeq.getAndIncrement());
    }

    private ReplicatedTablet hostGroup(String tabletId, KeyRange range) {
        for (TabletServer s : servers) {
            s.host(tabletId, new Tablet(range, dataDir.resolve(tabletId).resolve(s.id().id() + ".log"), fsync));
        }
        return new ReplicatedTablet(tabletId, servers);
    }

    public Optional<String> route(RowKey key) {
        return master.route(key);
    }

    public List<TabletServer> servers() {
        return servers;
    }

    public int groupCount() {
        return groups.size();
    }

    private ReplicatedTablet groupOwning(String tabletId, RowKey key) {
        ReplicatedTablet rt = groups.get(tabletId);
        if (rt == null || !rt.range().contains(key)) {
            throw new NotOwnerException("tablet " + tabletId + " does not own " + key);
        }
        return rt;
    }

    public boolean checkAndMutate(String tabletId, RowKey key, List<Condition> conditions, List<Mutation> mutations) {
        ReplicatedTablet rt = groupOwning(tabletId, key);
        boolean res = rt.checkAndMutate(key, conditions, mutations);
        maybeSplit(tabletId);
        return res;
    }

    public Optional<byte[]> get(String tabletId, RowKey key, ColumnKey column) {
        return groupOwning(tabletId, key).get(key, column);
    }

    public Optional<Row> getRow(String tabletId, RowKey key) {
        return groupOwning(tabletId, key).getRow(key);
    }

    /** Scan all groups whose rows could carry the prefix, merged in key order. */
    public NavigableMap<RowKey, Row> scan(String prefix) {
        NavigableMap<RowKey, Row> out = new TreeMap<>();
        for (ReplicatedTablet rt : groups.values()) {
            out.putAll(rt.scanPrefix(prefix));
        }
        return out;
    }

    private synchronized void maybeSplit(String tabletId) {
        ReplicatedTablet rt = groups.get(tabletId);
        if (rt == null) {
            return;
        }
        Tablet probe = rt.members().get(0).tablet(tabletId);
        if (!TabletSplitter.shouldSplit(probe, rowSplitThreshold, byteSplitThreshold)) {
            return;
        }
        if (probe.rowCount() < 2) {
            return;
        }
        String lowId = newTabletId();
        String highId = newTabletId();
        RowKey oldStart = rt.range().start();
        ReplicatedTablet.SplitGroups sg = rt.split(dataDir, lowId, highId);
        groups.remove(tabletId);
        groups.put(lowId, sg.low());
        groups.put(highId, sg.high());
        master.assignRange(oldStart, lowId);     // low keeps the old range start
        master.assignRange(sg.splitKey(), highId);
    }

    @Override
    public void close() {
        master.close();
        for (TabletServer s : servers) {
            // Tablets are closed lazily; in this pedagogical build we leave file handles to GC.
        }
    }
}
