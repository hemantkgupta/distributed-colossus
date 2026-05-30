package colossus.common;

/**
 * A Placement Group id — the indirection layer between chunks and D-servers.
 * A chunk hashes to a PG; a PG maps (via a KV lookup) to a small replica set.
 */
public record PgId(int id) implements Comparable<PgId> {
    public static PgId of(int id) {
        return new PgId(id);
    }

    /** Row-key rendering in the placement table: {@code pg/00001234}. */
    public String rowKey() {
        return String.format("pg/%08x", id);
    }

    @Override
    public int compareTo(PgId o) {
        return Integer.compare(id, o.id);
    }
}
