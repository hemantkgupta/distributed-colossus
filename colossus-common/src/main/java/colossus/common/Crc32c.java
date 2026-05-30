package colossus.common;

import java.util.zip.CRC32C;

/**
 * CRC32C helper used for per-block extent integrity (the {@code .crc} sidecar holds one
 * 4-byte CRC per {@link Constants#BLOCK_SIZE_BYTES} block). Pure JDK — no native ISA-L.
 */
public final class Crc32c {
    private Crc32c() {}

    public static int of(byte[] data) {
        return of(data, 0, data.length);
    }

    public static int of(byte[] data, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(data, off, len);
        return (int) c.getValue();
    }
}
