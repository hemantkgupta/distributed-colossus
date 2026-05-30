package colossus.common;

import java.util.Objects;

/** Identity of a data-plane daemon, encoded as {@code host:port}. */
public record DserverId(String hostPort) implements Comparable<DserverId> {
    public DserverId {
        Objects.requireNonNull(hostPort, "hostPort");
    }

    public static DserverId of(String host, int port) {
        return new DserverId(host + ":" + port);
    }

    public String host() {
        return hostPort.substring(0, hostPort.lastIndexOf(':'));
    }

    public int port() {
        return Integer.parseInt(hostPort.substring(hostPort.lastIndexOf(':') + 1));
    }

    @Override
    public int compareTo(DserverId o) {
        return hostPort.compareTo(o.hostPort);
    }
}
