package colossus.dserver;

import colossus.common.ChunkHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtentStoreTest {

    private static final ChunkHandle H = ChunkHandle.of(0x4242);

    @Test
    void writeReadRoundtrip(@TempDir Path dir) {
        ExtentStore store = new ExtentStore(dir, true);
        byte[] data = "hello colossus extent".getBytes();
        store.writeAt(H, 0, data);
        assertThat(store.read(H, 0, data.length)).isEqualTo(data);
        assertThat(store.length(H)).isEqualTo(data.length);
        assertThat(store.exists(H)).isTrue();
    }

    @Test
    void writeAtOffsetAndPartialRead(@TempDir Path dir) {
        ExtentStore store = new ExtentStore(dir, true);
        store.writeAt(H, 0, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertThat(store.read(H, 2, 3)).containsExactly(3, 4, 5);
    }

    @Test
    void durableAcrossFreshStoreHandle(@TempDir Path dir) {
        new ExtentStore(dir, true).writeAt(H, 0, new byte[]{9, 8, 7});
        // fsync-before-return means a fresh store over the same dir reads the bytes back.
        assertThat(new ExtentStore(dir, true).read(H, 0, 3)).containsExactly(9, 8, 7);
    }

    @Test
    void crcCatchesSingleByteCorruption(@TempDir Path dir) throws IOException {
        ExtentStore store = new ExtentStore(dir, true);
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) data[i] = (byte) i;
        store.writeAt(H, 0, data);
        assertThat(store.verify(H, 0, data.length)).isTrue();

        // Flip one byte directly in the .ext file, bypassing the CRC update.
        try (RandomAccessFile raf = new RandomAccessFile(dir.resolve(H.hex() + ".ext").toFile(), "rw")) {
            raf.seek(500);
            int b = raf.read();
            raf.seek(500);
            raf.write(b ^ 0x01);
        }
        assertThat(store.verify(H, 0, data.length)).isFalse();
        assertThatThrownBy(() -> store.read(H, 0, data.length))
                .isInstanceOf(ExtentStore.CorruptionException.class);
    }

    @Test
    void deleteRemovesExtent(@TempDir Path dir) {
        ExtentStore store = new ExtentStore(dir, true);
        store.writeAt(H, 0, new byte[]{1});
        store.delete(H);
        assertThat(store.exists(H)).isFalse();
        assertThat(store.length(H)).isZero();
    }
}
