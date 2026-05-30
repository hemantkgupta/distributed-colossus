package colossus.common;

import java.util.Objects;

/** Identity of a background reconciler process, encoded as {@code host:port}. */
public record CustodianId(String hostPort) {
    public CustodianId {
        Objects.requireNonNull(hostPort, "hostPort");
    }

    public static CustodianId of(String host, int port) {
        return new CustodianId(host + ":" + port);
    }
}
