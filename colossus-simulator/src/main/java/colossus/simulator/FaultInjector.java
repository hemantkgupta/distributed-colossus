package colossus.simulator;

import colossus.bigtable.TabletServer;
import colossus.common.DserverId;
import colossus.curator.Curator;

import java.util.List;

/**
 * Injects the failures the chaos tests exercise: crash a D-server, crash a Curator, or partition a
 * majority of BigTable tablet-servers. Mirrors the failure-modes table in {@code docs/failure-modes}
 * and the implementation plan §9.
 */
public final class FaultInjector {

    private final Cluster cluster;

    public FaultInjector(Cluster cluster) {
        this.cluster = cluster;
    }

    public void killDserver(DserverId id) {
        cluster.killDserver(id);
    }

    public void killCurator(Curator c) {
        cluster.killCurator(c);
    }

    /** Take down {@code n} of the BigTable tablet-servers (n >= quorum loses metadata writes). */
    public void partitionTabletServers(int n) {
        List<TabletServer> servers = cluster.btCluster().servers();
        for (int i = 0; i < n && i < servers.size(); i++) {
            servers.get(i).setUp(false);
        }
    }

    public void healTabletServers() {
        for (TabletServer s : cluster.btCluster().servers()) {
            s.setUp(true);
        }
    }
}
