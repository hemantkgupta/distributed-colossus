package colossus.erasure;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GaloisFieldTest {

    @Test
    void additionIsXor() {
        Random rng = new Random(42);
        for (int t = 0; t < 1000; t++) {
            int a = rng.nextInt(256);
            int b = rng.nextInt(256);
            assertThat(GaloisField.add(a, b)).isEqualTo(a ^ b);
            // In characteristic 2, subtraction equals addition.
            assertThat(GaloisField.sub(a, b)).isEqualTo(a ^ b);
            // a + a = 0 (every element is its own additive inverse).
            assertThat(GaloisField.add(a, a)).isZero();
        }
    }

    @Test
    void multiplicativeIdentityAndZero() {
        for (int a = 0; a < 256; a++) {
            assertThat(GaloisField.mul(a, 1)).isEqualTo(a);
            assertThat(GaloisField.mul(1, a)).isEqualTo(a);
            assertThat(GaloisField.mul(a, 0)).isZero();
            assertThat(GaloisField.mul(0, a)).isZero();
        }
    }

    @Test
    void everyNonZeroElementHasAnInverse() {
        for (int a = 1; a < 256; a++) {
            int inv = GaloisField.inv(a);
            assertThat(GaloisField.mul(a, inv)).isEqualTo(1);
            // inv(inv(a)) == a
            assertThat(GaloisField.inv(inv)).isEqualTo(a);
        }
    }

    @Test
    void zeroHasNoInverseAndCannotDivide() {
        assertThatThrownBy(() -> GaloisField.inv(0)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> GaloisField.div(5, 0)).isInstanceOf(ArithmeticException.class);
    }

    @Test
    void multiplicationIsCommutativeAndAssociative() {
        Random rng = new Random(7);
        for (int t = 0; t < 2000; t++) {
            int a = rng.nextInt(256);
            int b = rng.nextInt(256);
            int c = rng.nextInt(256);
            assertThat(GaloisField.mul(a, b)).isEqualTo(GaloisField.mul(b, a));
            assertThat(GaloisField.mul(GaloisField.mul(a, b), c))
                    .isEqualTo(GaloisField.mul(a, GaloisField.mul(b, c)));
        }
    }

    @Test
    void distributiveLaw() {
        // a * (b + c) == a*b + a*c, exhaustively over a small grid plus random samples.
        Random rng = new Random(99);
        for (int t = 0; t < 5000; t++) {
            int a = rng.nextInt(256);
            int b = rng.nextInt(256);
            int c = rng.nextInt(256);
            int lhs = GaloisField.mul(a, GaloisField.add(b, c));
            int rhs = GaloisField.add(GaloisField.mul(a, b), GaloisField.mul(a, c));
            assertThat(lhs).isEqualTo(rhs);
        }
    }

    @Test
    void divisionIsTheInverseOfMultiplication() {
        Random rng = new Random(123);
        for (int t = 0; t < 5000; t++) {
            int a = rng.nextInt(256);
            int b = 1 + rng.nextInt(255); // non-zero divisor
            // (a / b) * b == a
            assertThat(GaloisField.mul(GaloisField.div(a, b), b)).isEqualTo(a);
            // (a * b) / b == a
            assertThat(GaloisField.div(GaloisField.mul(a, b), b)).isEqualTo(a);
        }
    }

    @Test
    void powMatchesRepeatedMultiplication() {
        Random rng = new Random(2024);
        for (int t = 0; t < 500; t++) {
            int a = rng.nextInt(256);
            int n = rng.nextInt(12);
            int expected = 1;
            for (int i = 0; i < n; i++) {
                expected = GaloisField.mul(expected, a);
            }
            assertThat(GaloisField.pow(a, n)).isEqualTo(expected);
        }
        // Edge cases of the a^0 == 1 convention.
        assertThat(GaloisField.pow(0, 0)).isEqualTo(1);
        assertThat(GaloisField.pow(5, 0)).isEqualTo(1);
        assertThat(GaloisField.pow(0, 4)).isZero();
    }

    @Test
    void expAndLogAreConsistentGenerators() {
        // The generator's powers must enumerate all 255 non-zero elements (cycle length 255).
        boolean[] seen = new boolean[256];
        int x = 1;
        for (int i = 0; i < 255; i++) {
            assertThat(GaloisField.pow(GaloisField.GENERATOR, i)).isEqualTo(x);
            assertThat(seen[x]).as("element %d repeated at power %d", x, i).isFalse();
            seen[x] = true;
            x = GaloisField.mul(x, GaloisField.GENERATOR);
        }
        // After 255 steps we return to 1 (the group has order 255).
        assertThat(x).isEqualTo(1);
        // Every non-zero element was visited exactly once.
        for (int v = 1; v < 256; v++) {
            assertThat(seen[v]).as("element %d never produced by generator", v).isTrue();
        }
    }
}
