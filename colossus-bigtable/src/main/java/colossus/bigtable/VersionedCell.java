package colossus.bigtable;

/**
 * One timestamped version of a cell. A {@code null} value is a tombstone (the column was
 * deleted at this timestamp). Byte arrays are defensively copied on the way in and out.
 */
public record VersionedCell(long timestamp, byte[] value) {

    public VersionedCell {
        value = value == null ? null : value.clone();
    }

    public boolean isTombstone() {
        return value == null;
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
