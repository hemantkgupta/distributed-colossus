package colossus.erasure;

import java.util.Arrays;

/**
 * A <em>systematic</em> Reed–Solomon erasure coder over {@link GaloisField GF(2^8)} — the same
 * scheme Colossus, HDFS-EC, Ceph, and Backblaze use to trade a small parity overhead for the
 * ability to survive disk and machine loss without 3× full replication.
 *
 * <p><b>RS(k, m).</b> The data is split into {@code k} equal-length shards. The coder produces
 * {@code m} parity shards of the same length. The code is <em>maximum-distance-separable (MDS)</em>:
 * it tolerates the loss of <em>any</em> {@code m} of the {@code n = k + m} shards and reconstructs
 * the originals from <em>any</em> {@code k} survivors. The default {@link #rs63()} is
 * RS(6, 3) — 6 data + 3 parity — which is Colossus's headline configuration: it survives any 3
 * simultaneous losses at 1.5× storage overhead instead of the 3× of triple replication.
 *
 * <p><b>Systematic.</b> The first {@code k} of the {@code n} encoded shards <em>are</em> the
 * original data shards, byte-for-byte. So a normal read touches only data shards — no decode math
 * on the happy path. Decode (the {@code k}-of-{@code n} reconstruct) runs only after a loss.
 *
 * <p><b>The generator matrix.</b> Encoding is a matrix–vector product over GF(2^8). We build an
 * {@code n × k} generator matrix {@code G}:
 * <pre>
 *   G = [ I_k ]   top k rows: the k×k identity  → systematic (data passes through unchanged)
 *       [ C   ]   bottom m rows: an m×k Cauchy matrix → the parity rows
 * </pre>
 * Row {@code r} of {@code G} dotted with the data column gives encoded shard {@code r}. For each of
 * the {@code L} byte positions we treat the {@code k} data bytes as a column vector and multiply.
 *
 * <p><b>Why Cauchy (not Vandermonde).</b> Reconstruction needs the {@code k × k} submatrix formed
 * by <em>any</em> {@code k} surviving rows of {@code G} to be invertible. A Cauchy matrix
 * {@code C[i][j] = 1 / (x_i ⊕ y_j)} (with all {@code x_i}, {@code y_j} distinct and the two sets
 * disjoint) has the strong property that <em>every</em> square submatrix is invertible. Combined
 * with the identity block this makes {@code G} super-regular: every {@code k}-subset of its
 * {@code n} rows is invertible, which is exactly the MDS / any-{@code k}-reconstruct guarantee.
 * Vandermonde matrices need extra care (and can fail for some k/m) to keep this property; Cauchy is
 * the simpler, always-correct choice. We pick {@code x_i = i} for parity row {@code i} and
 * {@code y_j = m + j} for data column {@code j}, so the x-set {@code {0..m-1}} and y-set
 * {@code {m..m+k-1}} are disjoint and within the 256 field elements as long as {@code k + m ≤ 256}.
 *
 * <p><b>Repair reads {@code k}.</b> Reconstructing one lost shard still requires reading {@code k}
 * survivors and solving the {@code k × k} system — the well-known "repair cost" of plain RS that
 * motivates locally-repairable codes (LRC). This class implements classic RS; the read amplification
 * on repair is intentional and documented, not a bug.
 *
 * <p>Instances are immutable and thread-safe. All {@code byte[]} inputs are defensively copied;
 * outputs are freshly allocated.
 */
public final class ReedSolomon {

    private final int k;
    private final int m;
    private final int n;

    /** The n×k generator matrix: rows 0..k-1 are the identity, rows k..n-1 are the Cauchy parity. */
    private final int[][] generator;

    /**
     * Build a systematic RS(k, m) coder.
     *
     * @param k number of data shards, &gt;= 1
     * @param m number of parity shards, &gt;= 1; {@code k + m} must be &lt;= 256
     */
    public ReedSolomon(int k, int m) {
        if (k < 1 || m < 1) {
            throw new IllegalArgumentException("k and m must be >= 1 (got k=" + k + ", m=" + m + ")");
        }
        if (k + m > GaloisField.FIELD_SIZE) {
            throw new IllegalArgumentException(
                    "k + m must be <= " + GaloisField.FIELD_SIZE + " (got " + (k + m) + ")");
        }
        this.k = k;
        this.m = m;
        this.n = k + m;
        this.generator = buildGenerator(k, m);
    }

    /** The canonical Colossus configuration: 6 data + 3 parity, surviving any 3 losses at 1.5× cost. */
    public static ReedSolomon rs63() {
        return new ReedSolomon(6, 3);
    }

    public int dataShards() {
        return k;
    }

    public int parityShards() {
        return m;
    }

    public int totalShards() {
        return n;
    }

    /**
     * Build the n×k systematic generator matrix: identity on top, Cauchy parity on the bottom.
     *
     * <p>Cauchy entry for parity row {@code i} (0-based among the m parity rows) and data column
     * {@code j}: {@code 1 / (x_i ⊕ y_j)} with {@code x_i = i} and {@code y_j = m + j}. The xs and ys
     * are disjoint, so {@code x_i ⊕ y_j} is never 0 and the inverse always exists.
     */
    private static int[][] buildGenerator(int k, int m) {
        int n = k + m;
        int[][] g = new int[n][k];
        for (int i = 0; i < k; i++) {
            g[i][i] = 1; // identity block
        }
        for (int i = 0; i < m; i++) {
            int x = i;          // x-set: {0 .. m-1}
            for (int j = 0; j < k; j++) {
                int y = m + j;  // y-set: {m .. m+k-1}, disjoint from x-set
                g[k + i][j] = GaloisField.inv(GaloisField.add(x, y));
            }
        }
        return g;
    }

    /**
     * Encode {@code k} data shards into {@code m} parity shards.
     *
     * @param dataShards exactly {@code k} non-null shards, all the same length {@code L}
     * @return {@code m} freshly-allocated parity shards, each of length {@code L}
     */
    public byte[][] encode(byte[][] dataShards) {
        if (dataShards == null || dataShards.length != k) {
            throw new IllegalArgumentException("expected " + k + " data shards");
        }
        int len = shardLength(dataShards);
        // Defensive copy of inputs so concurrent mutation by the caller can't corrupt the encode.
        byte[][] data = new byte[k][];
        for (int i = 0; i < k; i++) {
            data[i] = dataShards[i].clone();
        }

        byte[][] parity = new byte[m][len];
        // parity[i][b] = sum_j  C[i][j] * data[j][b]   over GF(2^8), for each byte position b.
        for (int i = 0; i < m; i++) {
            int[] row = generator[k + i]; // the i-th parity (Cauchy) row
            byte[] out = parity[i];
            for (int b = 0; b < len; b++) {
                int acc = 0;
                for (int j = 0; j < k; j++) {
                    acc = GaloisField.add(acc, GaloisField.mul(row[j], data[j][b] & 0xFF));
                }
                out[b] = (byte) acc;
            }
        }
        return parity;
    }

    /**
     * Reconstruct the {@code k} original data shards from any {@code k} (or more) survivors.
     *
     * @param shards  array of length {@code n} (indices 0..k-1 are data shards, k..n-1 are parity);
     *                entries where {@code !present[i]} may be {@code null} or garbage and are ignored
     * @param present length-{@code n} mask; at least {@code k} entries must be {@code true}
     * @return the {@code k} reconstructed data shards (freshly allocated, defensively independent)
     */
    public byte[][] reconstruct(byte[][] shards, boolean[] present) {
        if (shards == null || shards.length != n) {
            throw new IllegalArgumentException("expected " + n + " shard slots");
        }
        if (present == null || present.length != n) {
            throw new IllegalArgumentException("expected present mask of length " + n);
        }
        int available = 0;
        for (boolean p : present) {
            if (p) {
                available++;
            }
        }
        if (available < k) {
            throw new IllegalArgumentException(
                    "need at least " + k + " present shards to reconstruct, have " + available);
        }

        int len = shardLengthOfPresent(shards, present);

        // Choose the first k present shards. Their generator rows form a k×k matrix A; the encoded
        // bytes they hold form the right-hand side. Solving A · data = rhs recovers the originals.
        int[] rows = new int[k];
        int picked = 0;
        for (int i = 0; i < n && picked < k; i++) {
            if (present[i]) {
                rows[picked++] = i;
            }
        }

        int[][] a = new int[k][k];
        for (int r = 0; r < k; r++) {
            a[r] = generator[rows[r]].clone();
        }
        int[][] aInv = invert(a);

        // Gather the surviving bytes for the chosen rows into a defensively-copied buffer.
        byte[][] sub = new byte[k][];
        for (int r = 0; r < k; r++) {
            sub[r] = shards[rows[r]].clone();
        }

        // data[j][b] = sum_r  aInv[j][r] * sub[r][b]   over GF(2^8).
        byte[][] data = new byte[k][len];
        for (int j = 0; j < k; j++) {
            int[] invRow = aInv[j];
            byte[] out = data[j];
            for (int b = 0; b < len; b++) {
                int acc = 0;
                for (int r = 0; r < k; r++) {
                    acc = GaloisField.add(acc, GaloisField.mul(invRow[r], sub[r][b] & 0xFF));
                }
                out[b] = (byte) acc;
            }
        }
        return data;
    }

    /**
     * Reconstruct everything: the {@code k} data shards <em>and</em> the {@code m} parity shards,
     * re-encoding any parity that was lost. Convenience for a full-stripe repair.
     *
     * @return all {@code n} shards (data then parity), freshly allocated
     */
    public byte[][] reconstructAll(byte[][] shards, boolean[] present) {
        byte[][] data = reconstruct(shards, present);
        byte[][] parity = encode(data);
        byte[][] all = new byte[n][];
        for (int i = 0; i < k; i++) {
            all[i] = data[i];
        }
        for (int i = 0; i < m; i++) {
            all[k + i] = parity[i];
        }
        return all;
    }

    // ---- matrix helpers over GF(2^8) ----

    /**
     * Invert a {@code k×k} matrix over GF(2^8) by Gauss–Jordan elimination. Cauchy super-regularity
     * guarantees the generator-derived matrices passed here are non-singular, but we still check the
     * pivot and fail loudly if a zero column ever appears (defends against programming error).
     */
    static int[][] invert(int[][] matrix) {
        int size = matrix.length;
        // Augmented [A | I]; we work on a copy so the caller's matrix is untouched.
        int[][] a = new int[size][2 * size];
        for (int i = 0; i < size; i++) {
            if (matrix[i].length != size) {
                throw new IllegalArgumentException("matrix must be square");
            }
            System.arraycopy(matrix[i], 0, a[i], 0, size);
            a[i][size + i] = 1;
        }

        for (int col = 0; col < size; col++) {
            // Find a pivot row at or below `col` with a non-zero entry in this column.
            int pivot = -1;
            for (int r = col; r < size; r++) {
                if (a[r][col] != 0) {
                    pivot = r;
                    break;
                }
            }
            if (pivot < 0) {
                throw new ArithmeticException("matrix is singular over GF(2^8); cannot invert");
            }
            if (pivot != col) {
                int[] tmp = a[pivot];
                a[pivot] = a[col];
                a[col] = tmp;
            }

            // Scale the pivot row so the pivot becomes 1.
            int pivVal = a[col][col];
            int pivInv = GaloisField.inv(pivVal);
            for (int c = 0; c < 2 * size; c++) {
                a[col][c] = GaloisField.mul(a[col][c], pivInv);
            }

            // Eliminate this column from every other row.
            for (int r = 0; r < size; r++) {
                if (r == col) {
                    continue;
                }
                int factor = a[r][col];
                if (factor == 0) {
                    continue;
                }
                for (int c = 0; c < 2 * size; c++) {
                    a[r][c] = GaloisField.add(a[r][c], GaloisField.mul(factor, a[col][c]));
                }
            }
        }

        int[][] inv = new int[size][size];
        for (int i = 0; i < size; i++) {
            System.arraycopy(a[i], size, inv[i], 0, size);
        }
        return inv;
    }

    private int shardLength(byte[][] dataShards) {
        int len = -1;
        for (int i = 0; i < k; i++) {
            if (dataShards[i] == null) {
                throw new IllegalArgumentException("data shard " + i + " is null");
            }
            if (len < 0) {
                len = dataShards[i].length;
            } else if (dataShards[i].length != len) {
                throw new IllegalArgumentException("all data shards must have equal length");
            }
        }
        if (len < 0) {
            throw new IllegalArgumentException("no data shards");
        }
        return len;
    }

    private int shardLengthOfPresent(byte[][] shards, boolean[] present) {
        int len = -1;
        for (int i = 0; i < n; i++) {
            if (!present[i]) {
                continue;
            }
            if (shards[i] == null) {
                throw new IllegalArgumentException("shard " + i + " marked present but is null");
            }
            if (len < 0) {
                len = shards[i].length;
            } else if (shards[i].length != len) {
                throw new IllegalArgumentException("all present shards must have equal length");
            }
        }
        if (len < 0) {
            throw new IllegalArgumentException("no present shards");
        }
        return len;
    }

    @Override
    public String toString() {
        return "ReedSolomon(k=" + k + ", m=" + m + ")[" + Arrays.deepToString(generator) + "]";
    }
}
