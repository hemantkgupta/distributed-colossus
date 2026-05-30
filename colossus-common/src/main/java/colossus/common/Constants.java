package colossus.common;

/** Cluster-wide constants. Defaults mirror {@code colossus.properties} in the impl plan §12. */
public final class Constants {
    private Constants() {}

    public static final int CHUNK_SIZE_BYTES = 64 * 1024 * 1024;   // 64 MB
    public static final int BLOCK_SIZE_BYTES = 64 * 1024;          // 64 KB CRC granularity
    public static final int LEASE_DURATION_SECONDS = 60;
    public static final int HEARTBEAT_INTERVAL_SECONDS = 5;
    public static final int HEARTBEAT_MISSED_THRESHOLD = 3;        // 15 s = dead
    public static final int REPLICATION_FACTOR = 3;
    public static final int PG_COUNT_PER_POOL = 4096;
    public static final int DMCLOCK_TICK_MICROS = 1000;
}
