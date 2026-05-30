package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A tablet replicated synchronously across N tablet-servers. The first reachable member acts as
 * coordinator: it evaluates a CAS against its own copy (the single authority for the decision),
 * then replicates the resulting mutations to every other reachable member before acking. A write
 * is rejected up front if fewer than a quorum of members are reachable, so a committed write is
 * always on a majority and survives any minority failure. Reads are served from any reachable
 * member and are consistent because every reachable member applied the identical mutation list.
 */
public final class ReplicatedTablet {

    private final String tabletId;
    private final List<TabletServer> members;
    private final int quorum;

    public ReplicatedTablet(String tabletId, List<TabletServer> members) {
        if (members.isEmpty()) {
            throw new IllegalArgumentException("a replicated tablet needs at least one member");
        }
        this.tabletId = tabletId;
        this.members = List.copyOf(members);
        this.quorum = members.size() / 2 + 1;
    }

    public String tabletId() {
        return tabletId;
    }

    public KeyRange range() {
        // All replicas share a range; read it off the first member's tablet.
        return members.get(0).tablet(tabletId).range();
    }

    public int quorum() {
        return quorum;
    }

    private List<TabletServer> reachable() {
        List<TabletServer> up = new ArrayList<>();
        for (TabletServer m : members) {
            if (m.isUp()) {
                up.add(m);
            }
        }
        return up;
    }

    public synchronized boolean checkAndMutate(RowKey key, List<Condition> conditions, List<Mutation> mutations) {
        List<TabletServer> up = reachable();
        if (up.size() < quorum) {
            throw new Faults.QuorumLostException(tabletId, up.size(), quorum);
        }
        TabletServer coordinator = up.get(0);
        boolean decided = coordinator.apply(tabletId, key, conditions, mutations);
        if (!decided) {
            return false; // CAS preconditions failed — no replication
        }
        // Replicate the now-decided mutations unconditionally to the other reachable members.
        for (int i = 1; i < up.size(); i++) {
            up.get(i).apply(tabletId, key, List.of(), mutations);
        }
        return true;
    }

    public void put(RowKey key, ColumnKey column, long ts, byte[] value) {
        checkAndMutate(key, List.of(), List.of(Mutation.put(column, ts, value)));
    }

    public Optional<byte[]> get(RowKey key, ColumnKey column) {
        for (TabletServer m : members) {
            if (m.isUp()) {
                return m.read(tabletId, key, column);
            }
        }
        throw new Faults.QuorumLostException(tabletId, 0, quorum);
    }

    public Optional<Row> getRow(RowKey key) {
        for (TabletServer m : members) {
            if (m.isUp()) {
                return m.readRow(tabletId, key);
            }
        }
        throw new Faults.QuorumLostException(tabletId, 0, quorum);
    }

    /** Read the column from every reachable member — used to assert cross-replica consistency. */
    public boolean allReachableAgree(RowKey key, ColumnKey column) {
        Optional<byte[]> ref = null;
        for (TabletServer m : members) {
            if (!m.isUp()) {
                continue;
            }
            Optional<byte[]> v = m.read(tabletId, key, column);
            if (ref == null) {
                ref = v;
            } else if (!bytesEqual(ref, v)) {
                return false;
            }
        }
        return true;
    }

    private static boolean bytesEqual(Optional<byte[]> a, Optional<byte[]> b) {
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() == b.isEmpty();
        }
        return java.util.Arrays.equals(a.get(), b.get());
    }

    public List<TabletServer> members() {
        return members;
    }

    /** Scan rows with the given prefix from the first reachable member. */
    public java.util.NavigableMap<RowKey, Row> scanPrefix(String prefix) {
        for (TabletServer m : members) {
            if (m.isUp()) {
                return m.tablet(tabletId).scanPrefix(prefix);
            }
        }
        throw new Faults.QuorumLostException(tabletId, 0, quorum);
    }

    /** Split every member replica at the median; returns the two replacement groups. */
    public synchronized SplitGroups split(java.nio.file.Path dataDir, String lowId, String highId) {
        RowKey splitKey = null;
        for (TabletServer m : members) {
            Tablet src = m.tablet(tabletId);
            java.nio.file.Path lowWal = dataDir.resolve(lowId).resolve(m.id().id() + ".log");
            java.nio.file.Path highWal = dataDir.resolve(highId).resolve(m.id().id() + ".log");
            TabletSplitter.SplitResult r = TabletSplitter.split(src, lowWal, highWal);
            splitKey = r.splitKey();
            m.host(lowId, r.low());
            m.host(highId, r.high());
        }
        return new SplitGroups(splitKey,
                new ReplicatedTablet(lowId, members),
                new ReplicatedTablet(highId, members));
    }

    public record SplitGroups(RowKey splitKey, ReplicatedTablet low, ReplicatedTablet high) {}
}
