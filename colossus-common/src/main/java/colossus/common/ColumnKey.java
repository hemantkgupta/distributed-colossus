package colossus.common;

import java.util.Objects;

/**
 * A BigTable column, addressed as {@code family:qualifier}. Families in the namespace
 * schema: meta, chunks, locations, lease, scrub. In the placement schema: dservers,
 * epoch, policy.
 */
public record ColumnKey(String family, String qualifier) implements Comparable<ColumnKey> {
    public ColumnKey {
        Objects.requireNonNull(family, "family");
        Objects.requireNonNull(qualifier, "qualifier");
    }

    public static ColumnKey of(String family, String qualifier) {
        return new ColumnKey(family, qualifier);
    }

    public String full() {
        return family + ":" + qualifier;
    }

    @Override
    public int compareTo(ColumnKey o) {
        int c = family.compareTo(o.family);
        return c != 0 ? c : qualifier.compareTo(o.qualifier);
    }
}
