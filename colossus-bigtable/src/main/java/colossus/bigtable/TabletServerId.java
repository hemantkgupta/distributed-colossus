package colossus.bigtable;

/** Identity of a process that hosts tablet replicas. */
public record TabletServerId(String id) {
    public static TabletServerId of(String id) {
        return new TabletServerId(id);
    }
}
