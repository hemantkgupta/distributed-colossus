package colossus.simulator;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.client.ClusterView;
import colossus.client.ColossusClient;
import colossus.common.ChunkHandle;
import colossus.common.CuratorId;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.curator.Curator;
import colossus.curator.CuratorShard;
import colossus.curator.LeaseManager;
import colossus.curator.PlacementProvider;
import colossus.curator.ShardOwnershipRegistry;
import colossus.custodian.Custodian;
import colossus.custodian.WorkDispatcher;
import colossus.custodian.WorkItem;
import colossus.dmclock.DurabilityEscalator;
import colossus.dmclock.Lane;
import colossus.dmclock.LaneConfig;
import colossus.dserver.Dserver;
import colossus.dserver.ExtentStore;
import colossus.placement.DserverStatusTable;
import colossus.placement.HashRouter;
import colossus.placement.PgTable;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Boots a whole Colossus cell in one JVM: a BigTable cluster, a pool of D-servers (each with a
 * dmClock scheduler + durability escalator), Curators over a shared ownership registry, a Custodian,
 * and a client. It implements {@link ClusterView} (so the client can reach the live services) and
 * provides a {@link WorkDispatcher} that executes the Custodian's repair/scrub/rebalance work
 * against the real D-servers — escalating a target's REPAIR lane on a durability-floor breach.
 */
public final class Cluster implements ClusterView {

    private final Clock clock;
    private final int replicationFactor;
    private final int chunkSize;
    private final long staleThresholdSeconds;

    private final BigTableCluster btCluster;
    private final BigTableClient bt;
    private final HashRouter router;
    private final PgTable pgTable;
    private final PlacementProvider placement;
    private final DserverStatusTable status;
    private final ShardOwnershipRegistry registry;
    private final LeaseManager leases;

    private final Map<DserverId, Dserver> dservers = new LinkedHashMap<>();
    private final Map<DserverId, DurabilityEscalator> escalators = new LinkedHashMap<>();
    private final java.util.Set<DserverId> dead = new java.util.HashSet<>();
    private final List<Curator> curators = new ArrayList<>();

    public Cluster(Path dir, Clock clock, int numDservers, int replicationFactor, int chunkSize,
                   long staleThresholdSeconds) {
        this.clock = clock;
        this.replicationFactor = replicationFactor;
        this.chunkSize = chunkSize;
        this.staleThresholdSeconds = staleThresholdSeconds;

        this.btCluster = new BigTableCluster(dir.resolve("bt"), 3, 1_000_000, Long.MAX_VALUE, false);
        this.bt = new BigTableClient(btCluster);
        this.router = new HashRouter(4096);
        this.pgTable = new PgTable(bt);
        this.status = new DserverStatusTable(bt);
        this.registry = new ShardOwnershipRegistry(bt);
        this.leases = new LeaseManager(bt, 60);

        List<DserverId> ids = new ArrayList<>();
        for (int i = 0; i < numDservers; i++) {
            DserverId id = DserverId.of("d" + i, 7100);
            var scheduler = Dserver.defaultScheduler();
            Dserver d = new Dserver(id, new ExtentStore(dir.resolve("ext").resolve("d" + i), true),
                    scheduler, status, 1_000_000_000L);
            dservers.put(id, d);
            escalators.put(id, new DurabilityEscalator(scheduler,
                    new LaneConfig(200, 1, 1500), new LaneConfig(2000, 4, 5000)));
            ids.add(id);
        }
        this.placement = new PlacementProvider(pgTable, ids, replicationFactor, "rack-distinct-3x");
    }

    public Curator addCurator(String host) {
        Curator c = new Curator(CuratorId.of(host, 9000), bt, registry, placement, router, leases,
                clock, 60, staleThresholdSeconds);
        curators.add(c);
        return c;
    }

    public Custodian custodian(int rebalanceExtentThreshold) {
        return new Custodian(bt, pgTable, status, new ArrayList<>(dservers.keySet()), dispatcher(),
                replicationFactor, staleThresholdSeconds, 7 * 86400, rebalanceExtentThreshold);
    }

    public ColossusClient client() {
        return new ColossusClient(this, chunkSize);
    }

    // ---- lifecycle / fault helpers ----

    public void heartbeatDservers(java.time.Instant now) {
        for (var e : dservers.entrySet()) {
            if (!dead.contains(e.getKey())) {
                e.getValue().heartbeat(now);
            }
        }
    }

    public void killDserver(DserverId id) {
        dead.add(id); // stops heartbeating; its status row goes stale and reads avoid it
    }

    public void killCurator(Curator c) {
        curators.remove(c); // stops heartbeating; its shard ownership row goes stale
    }

    public BigTableCluster btCluster() {
        return btCluster;
    }

    public boolean isDead(DserverId id) {
        return dead.contains(id);
    }

    public BigTableClient bigtable() {
        return bt;
    }

    public PgTable pgTable() {
        return pgTable;
    }

    public HashRouter router() {
        return router;
    }

    public DserverStatusTable status() {
        return status;
    }

    public Map<DserverId, Dserver> dservers() {
        return dservers;
    }

    public List<Curator> curators() {
        return curators;
    }

    // ---- ClusterView ----

    @Override
    public Optional<CuratorShard> curatorFor(FilePath path) {
        for (Curator c : curators) {
            Optional<CuratorShard> s = c.shardFor(path);
            if (s.isPresent()) {
                return s;
            }
        }
        return Optional.empty();
    }

    @Override
    public Dserver dserver(DserverId id) {
        return dservers.get(id);
    }

    // ---- WorkDispatcher: executes Custodian work against real D-servers ----

    public WorkDispatcher dispatcher() {
        return item -> {
            switch (item.op()) {
                case REPAIR_COPY -> {
                    DserverId targetId = item.target().orElseThrow();
                    DserverId sourceId = item.source().orElseThrow();
                    Dserver target = dservers.get(targetId);
                    Dserver source = dservers.get(sourceId);
                    Lane lane = Lane.BACKGROUND;
                    if (item.priority() == WorkItem.Priority.REPAIR) {
                        escalators.get(targetId).onBreach(); // durability breach → escalate target
                        lane = Lane.REPAIR;
                    }
                    for (ChunkHandle h : source.hostedHandles()) {
                        if (router.placementGroupFor(h.hex()).equals(item.pg())) {
                            target.copyExtentFrom(h, source, lane);
                        }
                    }
                    return WorkDispatcher.WorkResult.success();
                }
                case SCRUB -> {
                    Dserver primary = dservers.get(item.source().orElseThrow());
                    boolean corrupt = false;
                    for (ChunkHandle h : primary.hostedHandles()) {
                        if (router.placementGroupFor(h.hex()).equals(item.pg())) {
                            long len = primary.store().length(h);
                            if (!primary.store().verify(h, 0, (int) len)) {
                                corrupt = true;
                            }
                        }
                    }
                    return WorkDispatcher.WorkResult.scrub(corrupt);
                }
                case REBALANCE_MOVE, TIER_DOWN -> {
                    return WorkDispatcher.WorkResult.success();
                }
            }
            return WorkDispatcher.WorkResult.success();
        };
    }

    public DurabilityEscalator escalator(DserverId id) {
        return escalators.get(id);
    }
}
