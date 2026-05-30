package colossus.client;

import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.curator.CuratorShard;
import colossus.dserver.Dserver;

import java.util.Optional;

/**
 * The client's view of the live cluster: which Curator shard owns a path, and how to reach a
 * D-server by id. In a real deployment these are resolved by RPC + cached routing; the simulator
 * implements this interface over in-JVM Curators and D-servers. Resolving the owning shard can
 * return empty if the path's Curator is mid-failover — the client retries, mirroring the
 * WRONG_SHARD_REDIRECT / connection-refused path.
 */
public interface ClusterView {

    /** The Curator shard currently owning {@code path}, or empty if none is reachable right now. */
    Optional<CuratorShard> curatorFor(FilePath path);

    /** The live D-server with this id. */
    Dserver dserver(DserverId id);
}
