package colossus.bigtable;

import colossus.common.ColumnKey;

import java.util.Arrays;

/**
 * A precondition tested under the row lock during {@link Row#checkAndMutate}. An
 * {@code expectedValue} of {@code null} means "expect the column to be absent" (no live value).
 */
public record Condition(ColumnKey column, byte[] expectedValue) {

    public Condition {
        expectedValue = expectedValue == null ? null : expectedValue.clone();
    }

    public static Condition expectAbsent(ColumnKey column) {
        return new Condition(column, null);
    }

    public static Condition expectValue(ColumnKey column, byte[] expectedValue) {
        if (expectedValue == null) {
            throw new IllegalArgumentException("use expectAbsent for a null expectation");
        }
        return new Condition(column, expectedValue);
    }

    public boolean expectsAbsent() {
        return expectedValue == null;
    }

    boolean matches(byte[] actual) {
        if (expectedValue == null) {
            return actual == null;
        }
        return actual != null && Arrays.equals(expectedValue, actual);
    }

    @Override
    public byte[] expectedValue() {
        return expectedValue == null ? null : expectedValue.clone();
    }
}
