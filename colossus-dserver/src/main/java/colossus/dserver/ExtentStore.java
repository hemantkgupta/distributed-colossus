package colossus.dserver;

import colossus.common.ChunkHandle;
import colossus.common.Constants;
import colossus.common.Crc32c;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The on-disk extent layout (§7.2): one file per extent ({@code <handle>.ext}) plus a CRC32C
 * sidecar ({@code <handle>.crc}, one 4-byte CRC per 64 KB block). Writes fsync both files before
 * returning, modeling the durability boundary (O_DIRECT is modeled, not enforced — ADR 0010).
 * Reads verify the CRC of every block they touch, so silent corruption is caught at read time and
 * by the Custodian's scrub.
 */
public final class ExtentStore {

    private static final int BLOCK = Constants.BLOCK_SIZE_BYTES;

    private final Path dataDir;
    private final boolean fsync;

    public ExtentStore(Path dataDir, boolean fsync) {
        this.dataDir = dataDir;
        this.fsync = fsync;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("mkdir " + dataDir, e);
        }
    }

    private Path ext(ChunkHandle h) {
        return dataDir.resolve(h.hex() + ".ext");
    }

    private Path crc(ChunkHandle h) {
        return dataDir.resolve(h.hex() + ".crc");
    }

    public boolean exists(ChunkHandle h) {
        return Files.exists(ext(h));
    }

    public long length(ChunkHandle h) {
        try {
            return Files.exists(ext(h)) ? Files.size(ext(h)) : 0;
        } catch (IOException e) {
            throw new UncheckedIOException("size " + h.hex(), e);
        }
    }

    /** Write bytes at an offset and refresh the CRC of every block the write touched. */
    public void writeAt(ChunkHandle h, long offset, byte[] bytes) {
        try (RandomAccessFile data = new RandomAccessFile(ext(h).toFile(), "rw");
             RandomAccessFile crc = new RandomAccessFile(crc(h).toFile(), "rw")) {
            data.seek(offset);
            data.write(bytes);
            long firstBlock = offset / BLOCK;
            long lastBlock = (offset + bytes.length - 1) / BLOCK;
            for (long b = firstBlock; b <= lastBlock; b++) {
                refreshBlockCrc(data, crc, b);
            }
            if (fsync) {
                data.getFD().sync();
                crc.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("write " + h.hex(), e);
        }
    }

    private void refreshBlockCrc(RandomAccessFile data, RandomAccessFile crc, long blockIndex) throws IOException {
        long start = blockIndex * BLOCK;
        long fileLen = data.length();
        if (start >= fileLen) {
            return;
        }
        int len = (int) Math.min(BLOCK, fileLen - start);
        byte[] block = new byte[len];
        data.seek(start);
        data.readFully(block);
        int c = Crc32c.of(block);
        crc.seek(blockIndex * 4L);
        crc.writeInt(c);
    }

    /** Read {@code size} bytes at {@code offset}, verifying the CRC of every block touched. */
    public byte[] read(ChunkHandle h, long offset, int size) {
        try (RandomAccessFile data = new RandomAccessFile(ext(h).toFile(), "r")) {
            long fileLen = data.length();
            int toRead = (int) Math.min(size, Math.max(0, fileLen - offset));
            byte[] out = new byte[toRead];
            data.seek(offset);
            data.readFully(out);
            if (!verify(h, offset, toRead)) {
                throw new CorruptionException(h, offset);
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("read " + h.hex(), e);
        }
    }

    /** Recompute and compare the stored CRC for every block overlapping [offset, offset+size). */
    public boolean verify(ChunkHandle h, long offset, int size) {
        if (size == 0) {
            return true;
        }
        try (RandomAccessFile data = new RandomAccessFile(ext(h).toFile(), "r");
             RandomAccessFile crc = new RandomAccessFile(crc(h).toFile(), "r")) {
            long firstBlock = offset / BLOCK;
            long lastBlock = (offset + size - 1) / BLOCK;
            long fileLen = data.length();
            for (long b = firstBlock; b <= lastBlock; b++) {
                long start = b * BLOCK;
                if (start >= fileLen) {
                    break;
                }
                int len = (int) Math.min(BLOCK, fileLen - start);
                byte[] block = new byte[len];
                data.seek(start);
                data.readFully(block);
                crc.seek(b * 4L);
                int stored = crc.readInt();
                if (stored != Crc32c.of(block)) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("verify " + h.hex(), e);
        }
    }

    public void delete(ChunkHandle h) {
        try {
            Files.deleteIfExists(ext(h));
            Files.deleteIfExists(crc(h));
        } catch (IOException e) {
            throw new UncheckedIOException("delete " + h.hex(), e);
        }
    }

    /** Thrown when a read's CRC check fails — the on-disk bytes do not match their stored CRC. */
    public static final class CorruptionException extends RuntimeException {
        public CorruptionException(ChunkHandle h, long offset) {
            super("CRC mismatch on extent " + h.hex() + " at offset " + offset);
        }
    }
}
