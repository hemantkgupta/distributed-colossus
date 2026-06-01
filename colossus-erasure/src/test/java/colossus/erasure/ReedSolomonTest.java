package colossus.erasure;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReedSolomonTest {

    private static final int K = 6;
    private static final int M = 3;
    private static final int N = K + M;

    /** Build the full n-shard stripe (k data + m parity) for the given data. */
    private static byte[][] stripe(ReedSolomon rs, byte[][] data) {
        byte[][] parity = rs.encode(data);
        byte[][] all = new byte[N][];
        System.arraycopy(data, 0, all, 0, K);
        System.arraycopy(parity, 0, all, K, M);
        return all;
    }

    private static byte[][] randomData(Random rng, int len) {
        byte[][] data = new byte[K][len];
        for (int i = 0; i < K; i++) {
            rng.nextBytes(data[i]);
        }
        return data;
    }

    @Test
    void encodeProducesMParityShardsOfTheRightLength() {
        ReedSolomon rs = ReedSolomon.rs63();
        assertThat(rs.dataShards()).isEqualTo(6);
        assertThat(rs.parityShards()).isEqualTo(3);
        assertThat(rs.totalShards()).isEqualTo(9);

        byte[][] data = randomData(new Random(1), 1024);
        byte[][] parity = rs.encode(data);
        assertThat(parity.length).isEqualTo(3);
        for (byte[] p : parity) {
            assertThat(p).hasSize(1024);
        }
    }

    @Test
    void roundTripWithNoLossReturnsOriginalData() {
        ReedSolomon rs = ReedSolomon.rs63();
        byte[][] data = randomData(new Random(2), 256);
        byte[][] all = stripe(rs, data);
        boolean[] present = new boolean[N];
        java.util.Arrays.fill(present, true);

        byte[][] recovered = rs.reconstruct(all, present);
        for (int i = 0; i < K; i++) {
            assertThat(recovered[i]).containsExactly(data[i]);
        }
    }

    @Test
    void survivesAnyThreeLossesForEveryTriple() {
        ReedSolomon rs = ReedSolomon.rs63();
        Random rng = new Random(12345);
        byte[][] data = randomData(rng, 1024);
        byte[][] all = stripe(rs, data);

        // Exhaustively try EVERY way to lose 3 of the 9 shards (C(9,3) = 84 triples),
        // covering all-data, all-parity, and mixed losses.
        List<int[]> triples = combinations(N, 3);
        assertThat(triples).hasSize(84);

        for (int[] lost : triples) {
            byte[][] shards = all.clone();
            boolean[] present = new boolean[N];
            java.util.Arrays.fill(present, true);
            for (int idx : lost) {
                present[idx] = false;
                shards[idx] = null; // erased: gone entirely
            }

            byte[][] recovered = rs.reconstruct(shards, present);
            for (int i = 0; i < K; i++) {
                assertThat(recovered[i])
                        .as("data shard %d after losing %s", i, java.util.Arrays.toString(lost))
                        .containsExactly(data[i]);
            }
        }
    }

    @Test
    void reconstructAllRecoversDataAndParity() {
        ReedSolomon rs = ReedSolomon.rs63();
        byte[][] data = randomData(new Random(77), 512);
        byte[][] all = stripe(rs, data);

        // Lose 2 data + 1 parity.
        byte[][] shards = all.clone();
        boolean[] present = new boolean[N];
        java.util.Arrays.fill(present, true);
        int[] lost = {0, 3, 7};
        for (int idx : lost) {
            present[idx] = false;
            shards[idx] = null;
        }

        byte[][] full = rs.reconstructAll(shards, present);
        assertThat(full.length).isEqualTo(N);
        for (int i = 0; i < K; i++) {
            assertThat(full[i]).containsExactly(data[i]);
        }
        // The recovered parity must match a fresh encode of the recovered data.
        byte[][] parity = rs.encode(data);
        for (int i = 0; i < M; i++) {
            assertThat(full[K + i]).containsExactly(parity[i]);
        }
    }

    @Test
    void garbageInErasedSlotsIsIgnored() {
        // Reconstruct must not read !present slots: fill them with garbage and still succeed.
        ReedSolomon rs = ReedSolomon.rs63();
        byte[][] data = randomData(new Random(555), 128);
        byte[][] all = stripe(rs, data);
        byte[][] shards = all.clone();
        boolean[] present = new boolean[N];
        java.util.Arrays.fill(present, true);
        int[] lost = {1, 4, 8};
        Random rng = new Random(999);
        for (int idx : lost) {
            present[idx] = false;
            byte[] junk = new byte[128];
            rng.nextBytes(junk);
            shards[idx] = junk; // garbage, not null
        }

        byte[][] recovered = rs.reconstruct(shards, present);
        for (int i = 0; i < K; i++) {
            assertThat(recovered[i]).containsExactly(data[i]);
        }
    }

    @Test
    void fewerThanKPresentCannotReconstruct() {
        ReedSolomon rs = ReedSolomon.rs63();
        byte[][] data = randomData(new Random(3), 64);
        byte[][] all = stripe(rs, data);
        boolean[] present = new boolean[N];
        java.util.Arrays.fill(present, true);
        // Only 5 present (< k=6): losing any 4 leaves too few survivors.
        present[0] = false;
        present[1] = false;
        present[2] = false;
        present[3] = false;

        assertThatThrownBy(() -> rs.reconstruct(all, present))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 6");
    }

    @Test
    void workedCheckOnTinyData() {
        // Length-1 shards: each shard is a single byte. Verify encode is the GF dot product of the
        // Cauchy rows with the data column, and that reconstruct from a mixed survivor set inverts it.
        ReedSolomon rs = ReedSolomon.rs63();
        byte[][] data = {
                {(byte) 0x01}, {(byte) 0x02}, {(byte) 0x03},
                {(byte) 0x04}, {(byte) 0x05}, {(byte) 0x06}
        };
        byte[][] parity = rs.encode(data);
        assertThat(parity.length).isEqualTo(3);

        // Recompute parity[0] by hand using the Cauchy row for parity index 0:
        //   x_0 = 0, y_j = m + j = 3 + j ; entry = inv(0 ^ (3+j)) = inv(3+j)
        int expectedP0 = 0;
        for (int j = 0; j < K; j++) {
            int coeff = GaloisField.inv(GaloisField.add(0, M + j)); // inv(3+j)
            expectedP0 = GaloisField.add(expectedP0, GaloisField.mul(coeff, data[j][0] & 0xFF));
        }
        assertThat(parity[0][0] & 0xFF).isEqualTo(expectedP0);

        // Now lose data shards 0 and 5 plus parity shard 1, and reconstruct.
        byte[][] all = new byte[N][];
        System.arraycopy(data, 0, all, 0, K);
        System.arraycopy(parity, 0, all, K, M);
        boolean[] present = new boolean[N];
        java.util.Arrays.fill(present, true);
        present[0] = false;
        present[5] = false;
        present[K + 1] = false; // parity shard index 1
        all[0] = null;
        all[5] = null;
        all[K + 1] = null;

        byte[][] recovered = rs.reconstruct(all, present);
        assertThat(recovered[0][0] & 0xFF).isEqualTo(0x01);
        assertThat(recovered[5][0] & 0xFF).isEqualTo(0x06);
        for (int i = 0; i < K; i++) {
            assertThat(recovered[i][0] & 0xFF).isEqualTo(i + 1);
        }
    }

    @Test
    void matrixInvertRoundTripsToIdentity() {
        // Sanity-check the GF Gauss–Jordan inverter on a small Cauchy-derived matrix.
        ReedSolomon rs = ReedSolomon.rs63();
        // Use three parity rows + identity columns indirectly by inverting a 3x3 Cauchy block.
        int[][] cauchy = new int[3][3];
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                cauchy[i][j] = GaloisField.inv(GaloisField.add(i, 3 + j));
            }
        }
        int[][] inv = ReedSolomon.invert(cauchy);
        // cauchy * inv == I over GF(2^8)
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                int acc = 0;
                for (int t = 0; t < 3; t++) {
                    acc = GaloisField.add(acc, GaloisField.mul(cauchy[i][t], inv[t][j]));
                }
                assertThat(acc).as("(%d,%d) of product", i, j).isEqualTo(i == j ? 1 : 0);
            }
        }
        assertThat(rs.toString()).contains("k=6, m=3");
    }

    // ---- helpers ----

    /** All r-element index subsets of {0..n-1}, returned as sorted int[] of size r. */
    private static List<int[]> combinations(int n, int r) {
        List<int[]> out = new ArrayList<>();
        int[] idx = new int[r];
        for (int i = 0; i < r; i++) {
            idx[i] = i;
        }
        while (true) {
            out.add(idx.clone());
            int i = r - 1;
            while (i >= 0 && idx[i] == n - r + i) {
                i--;
            }
            if (i < 0) {
                break;
            }
            idx[i]++;
            for (int j = i + 1; j < r; j++) {
                idx[j] = idx[j - 1] + 1;
            }
        }
        return out;
    }
}
