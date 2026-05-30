package colossus.placement;

import colossus.common.DserverId;
import colossus.common.PgId;

import java.util.List;

/**
 * The current placement of one PG: its replica set, its monotonic epoch (bumped on any change to
 * the replica set), and the named policy that produced it. Carried (epoch only) on client D-server
 * RPCs so a stale client is rejected at the front door with STALE_PLACEMENT_REPLY.
 */
public record PgPlacement(PgId pgId, List<DserverId> dservers, int epoch, String policy) {
    public PgPlacement {
        dservers = List.copyOf(dservers);
    }

    public DserverId primary() {
        return dservers.get(0);
    }
}
