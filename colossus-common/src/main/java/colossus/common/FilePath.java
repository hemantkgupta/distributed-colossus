package colossus.common;

import java.util.Objects;

/**
 * A file path. Doubles as the BigTable row key for the namespace table
 * (one file = one row, row key = path).
 */
public record FilePath(String path) {
    public FilePath {
        Objects.requireNonNull(path, "path");
    }

    public static FilePath of(String path) {
        return new FilePath(path);
    }

    public boolean isAbsolute() {
        return path.startsWith("/");
    }

    /** The first {@code chars} characters of the path — used for shard-prefix routing. */
    public String prefix(int chars) {
        return path.substring(0, Math.min(chars, path.length()));
    }

    public RowKey toRowKey() {
        return RowKey.of(path);
    }
}
