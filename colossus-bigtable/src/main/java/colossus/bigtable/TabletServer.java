package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A process that hosts one or more tablet replicas. In production a tablet-server is a Borg job;
 * here it is an in-JVM object with an {@code up} flag the {@code FaultInjector} can toggle to model
 * crashes and partitions. It applies mutations to the tablets it holds and serves reads.
 */
public final class TabletServer {

    private final TabletServerId id;
    private final ConcurrentHashMap<String, Tablet> tablets = new ConcurrentHashMap<>();
    private volatile boolean up = true;

    public TabletServer(TabletServerId id) {
        this.id = id;
    }

    public TabletServerId id() {
        return id;
    }

    public void host(String tabletId, Tablet tablet) {
        tablets.put(tabletId, tablet);
    }

    public Tablet tablet(String tabletId) {
        return tablets.get(tabletId);
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(boolean up) {
        this.up = up;
    }

    private void requireUp() {
        if (!up) {
            throw new Faults.UnreachableException(id);
        }
    }

    public boolean apply(String tabletId, RowKey key, List<Condition> conditions, List<Mutation> mutations) {
        requireUp();
        return tablets.get(tabletId).checkAndMutate(key, conditions, mutations);
    }

    public Optional<byte[]> read(String tabletId, RowKey key, ColumnKey column) {
        requireUp();
        return tablets.get(tabletId).get(key, column);
    }

    public Optional<Row> readRow(String tabletId, RowKey key) {
        requireUp();
        return tablets.get(tabletId).getRow(key);
    }
}
