package colossus.erasure;

/**
 * Arithmetic over the finite field GF(2^8) — the field Colossus (like every production erasure
 * coder: Jerasure, Intel ISA-L, Backblaze's Java RS) does Reed–Solomon math in.
 *
 * <p>Why a field and not ordinary integer arithmetic? Reed–Solomon needs the four operations
 * (+, −, ×, ÷) to be exact and closed over a fixed set of symbols. Bytes are the natural symbol,
 * so we need a field with exactly 256 elements. GF(2^8) is that field: its elements are the 256
 * bytes {@code 0..255}, addition is XOR (so it never carries and never overflows a byte), and
 * multiplication is polynomial multiplication of two bytes (treated as degree-7 polynomials over
 * GF(2)) reduced modulo a fixed irreducible "primitive" polynomial.
 *
 * <p>This implementation uses the universally-standard primitive polynomial
 * {@code 0x11D = x^8 + x^4 + x^3 + x^2 + 1} and generator {@code g = 0x02}. Because {@code g} is a
 * generator, the powers {@code g^0, g^1, … , g^254} enumerate all 255 non-zero elements before
 * cycling (the multiplicative group has order 255). That lets us turn the expensive bit-by-bit
 * multiply into two table lookups and an add, via discrete logarithms:
 * <pre>
 *   a · b = g^(log_g a + log_g b)
 * </pre>
 *
 * <p>Two tables, both built once in a static initializer:
 * <ul>
 *   <li>{@code EXP[i] = g^i}. Sized 512 and "doubled" (EXP[i] = EXP[i-255] for i in 255..511) so
 *       that {@code log a + log b} (which is at most 254+254 = 508) can index it without a modulo.</li>
 *   <li>{@code LOG[x] = i} such that {@code g^i = x}, for x in 1..255. {@code LOG[0]} is undefined
 *       (0 has no discrete log) and left at 0.</li>
 * </ul>
 *
 * <p>All methods take and return {@code int}s holding a byte value in {@code [0,255]}. The class is
 * stateless and thread-safe; everything is {@code static}.
 */
public final class GaloisField {

    /** Field size: GF(2^8) has 256 elements. */
    public static final int FIELD_SIZE = 256;

    /** Order of the multiplicative group (non-zero elements): 255. */
    public static final int ORDER = FIELD_SIZE - 1;

    /** Primitive (irreducible) polynomial x^8 + x^4 + x^3 + x^2 + 1. */
    public static final int PRIMITIVE_POLYNOMIAL = 0x11D;

    /** Multiplicative generator of GF(2^8) under {@link #PRIMITIVE_POLYNOMIAL}. */
    public static final int GENERATOR = 0x02;

    // EXP is doubled (length 512) so log-sums up to 508 index it directly without a modulo.
    private static final int[] EXP = new int[FIELD_SIZE * 2];
    private static final int[] LOG = new int[FIELD_SIZE];

    static {
        int x = 1;
        for (int i = 0; i < ORDER; i++) {
            EXP[i] = x;
            LOG[x] = i;
            // x *= GENERATOR  in GF(2^8): shift left by 1, then reduce mod the primitive poly.
            x <<= 1;
            if ((x & 0x100) != 0) {
                x ^= PRIMITIVE_POLYNOMIAL;
            }
        }
        // After ORDER steps x must return to 1 (g has order 255). Mirror the table for index >= 255.
        for (int i = ORDER; i < EXP.length; i++) {
            EXP[i] = EXP[i - ORDER];
        }
        // LOG[0] is undefined; LOG[1] == 0 falls out of the loop above.
    }

    private GaloisField() {
    }

    /** Field addition. In GF(2^8) addition (and subtraction) is bitwise XOR. */
    public static int add(int a, int b) {
        return (a ^ b) & 0xFF;
    }

    /** Field subtraction — identical to {@link #add} in characteristic 2. */
    public static int sub(int a, int b) {
        return (a ^ b) & 0xFF;
    }

    /** Field multiplication: 0 if either operand is 0, else {@code g^(log a + log b)}. */
    public static int mul(int a, int b) {
        a &= 0xFF;
        b &= 0xFF;
        if (a == 0 || b == 0) {
            return 0;
        }
        return EXP[LOG[a] + LOG[b]];
    }

    /** Field division {@code a / b}. {@code b} must be non-zero. */
    public static int div(int a, int b) {
        a &= 0xFF;
        b &= 0xFF;
        if (b == 0) {
            throw new ArithmeticException("division by zero in GF(2^8)");
        }
        if (a == 0) {
            return 0;
        }
        // log a - log b may be negative; + ORDER keeps the index in [0, 2*ORDER).
        return EXP[LOG[a] - LOG[b] + ORDER];
    }

    /** Multiplicative inverse {@code a^-1}. {@code a} must be non-zero. */
    public static int inv(int a) {
        a &= 0xFF;
        if (a == 0) {
            throw new ArithmeticException("0 has no multiplicative inverse in GF(2^8)");
        }
        // a^-1 = g^(255 - log a). When log a == 0 (a == 1) this is g^255 == g^0 == 1 via the
        // doubled table, which is correct.
        return EXP[ORDER - LOG[a]];
    }

    /** Exponentiation {@code a^n} for n >= 0. By convention {@code a^0 == 1} for all a (incl. 0). */
    public static int pow(int a, int n) {
        a &= 0xFF;
        if (n < 0) {
            throw new IllegalArgumentException("negative exponent: " + n);
        }
        if (n == 0) {
            return 1;
        }
        if (a == 0) {
            return 0;
        }
        // a^n = g^(n * log a mod 255).
        int e = (LOG[a] * (n % ORDER)) % ORDER;
        return EXP[e];
    }
}
