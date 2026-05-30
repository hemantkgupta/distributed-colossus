package colossus.bigtable;

import colossus.common.ColumnKey;

/**
 * A single-column write within a row. {@code delete=true} writes a tombstone at {@code timestamp};
 * otherwise it writes {@code value}. A list of mutations applied via
 * {@link Row#checkAndMutate} is atomic with respect to the row lock.
 */
public record Mutation(ColumnKey column, long timestamp, byte[] value, boolean delete) {

    public Mutation {
        value = value == null ? null : value.clone();
    }

    public static Mutation put(ColumnKey column, long timestamp, byte[] value) {
        return new Mutation(column, timestamp, value, false);
    }

    public static Mutation delete(ColumnKey column, long timestamp) {
        return new Mutation(column, timestamp, null, true);
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
