package colossus.common;

import java.util.Objects;

/** Identity of a stateless namespace-shard owner, encoded as {@code host:port}. */
public record CuratorId(String hostPort) {
    public CuratorId {
        Objects.requireNonNull(hostPort, "hostPort");
    }

    public static CuratorId of(String host, int port) {
        return new CuratorId(host + ":" + port);
    }
}
