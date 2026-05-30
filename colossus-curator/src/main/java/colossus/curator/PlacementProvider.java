package colossus.curator;

import colossus.common.DserverId;
import colossus.common.PgId;
import colossus.placement.PgPlacement;
import colossus.placement.PgTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Resolves a PG to its replica set, assigning one on first use. In production the replica set is
 * chosen by a placement policy fed by hundreds of cluster signals; here a round-robin pick over the
 * known D-servers suffices to make AllocateChunk produce real locations without external bootstrap.
 * The assignment is persisted in the {@link PgTable} so it is stable across Curator restarts.
 */
public final class PlacementProvider {

    private final PgTable table;
    private final List<DserverId> dservers;
    private final int replicationFactor;
    private final String policy;

    public PlacementProvider(PgTable table, List<DserverId> dservers, int replicationFactor, String policy) {
        if (dservers.size() < replicationFactor) {
            throw new IllegalArgumentException("need at least " + replicationFactor + " D-servers");
        }
        this.table = table;
        this.dservers = List.copyOf(dservers);
        this.replicationFactor = replicationFactor;
        this.policy = policy;
    }

    public Optional<PgPlacement> lookup(PgId pg) {
        return table.lookup(pg);
    }

    public synchronized PgPlacement ensure(PgId pg) {
        Optional<PgPlacement> existing = table.lookup(pg);
        if (existing.isPresent()) {
            return existing.get();
        }
        List<DserverId> picked = new ArrayList<>(replicationFactor);
        int n = dservers.size();
        for (int i = 0; i < replicationFactor; i++) {
            picked.add(dservers.get(Math.floorMod(pg.id() + i, n)));
        }
        return table.assign(pg, picked, policy);
    }
}
