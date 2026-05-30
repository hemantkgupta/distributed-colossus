package colossus.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A binary-safe BigTable row key. Ordering is unsigned-lexicographic, which is what
 * BigTable scans and tablet ranges rely on.
 */
public record RowKey(byte[] bytes) implements Comparable<RowKey> {
    public RowKey {
        bytes = bytes.clone(); // defensive copy — records expose the array
    }

    public static RowKey of(String s) {
        return new RowKey(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    public String asString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(RowKey o) {
        return Arrays.compareUnsigned(bytes, o.bytes);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RowKey r && Arrays.equals(bytes, r.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "RowKey(" + asString() + ")";
    }
}
