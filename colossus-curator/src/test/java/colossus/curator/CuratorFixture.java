package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.CuratorId;
import colossus.common.DserverId;
import colossus.placement.HashRouter;
import colossus.placement.PgTable;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/** Shared wiring for Curator tests: a BigTable cluster, placement over N D-servers, and a Curator. */
final class CuratorFixture {
    final BigTableClient bt;
    final ShardOwnershipRegistry registry;
    final HashRouter router;
    final PlacementProvider placement;
    final LeaseManager leases;

    CuratorFixture(Path dir) {
        this.bt = new BigTableClient(new BigTableCluster(dir, 3, 1_000_000, Long.MAX_VALUE, false));
        this.registry = new ShardOwnershipRegistry(bt);
        this.router = new HashRouter(4096);
        List<DserverId> dservers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            dservers.add(DserverId.of("d" + i, 7100));
        }
        this.placement = new PlacementProvider(new PgTable(bt), dservers, 3, "rack-distinct-3x");
        this.leases = new LeaseManager(bt, 60);
    }

    Curator curator(String host, Clock clock) {
        return new Curator(CuratorId.of(host, 9000), bt, registry, placement, router, leases,
                clock, 60, 15);
    }
}
