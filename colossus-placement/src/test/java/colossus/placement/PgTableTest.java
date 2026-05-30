package colossus.placement;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.DserverId;
import colossus.common.PgId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PgTableTest {

    private static final DserverId A = DserverId.of("h", 1);
    private static final DserverId B = DserverId.of("h", 2);
    private static final DserverId C = DserverId.of("h", 3);
    private static final DserverId D = DserverId.of("h", 4);

    private BigTableClient client(Path dir) {
        return new BigTableClient(new BigTableCluster(dir, 1, 1_000_000, Long.MAX_VALUE, false));
    }

    @Test
    void assignAndLookupRoundtrip(@TempDir Path dir) {
        PgTable table = new PgTable(client(dir));
        PgPlacement p = table.assign(PgId.of(137), List.of(A, B, C), "rack-distinct-3x");
        assertThat(p.epoch()).isEqualTo(1);
        assertThat(table.lookup(PgId.of(137))).hasValueSatisfying(got -> {
            assertThat(got.dservers()).containsExactly(A, B, C);
            assertThat(got.primary()).isEqualTo(A);
            assertThat(got.policy()).isEqualTo("rack-distinct-3x");
            assertThat(got.epoch()).isEqualTo(1);
        });
    }

    @Test
    void epochBumpsOnReplicaSetChange(@TempDir Path dir) {
        PgTable table = new PgTable(client(dir));
        table.assign(PgId.of(1), List.of(A, B, C), "p");
        table.assign(PgId.of(1), List.of(A, B, D), "p"); // C replaced by D after a repair
        PgPlacement got = table.lookup(PgId.of(1)).orElseThrow();
        assertThat(got.epoch()).isEqualTo(2);
        assertThat(got.dservers()).containsExactly(A, B, D);
    }

    @Test
    void shrinkingReplicaSetClearsStaleSlots(@TempDir Path dir) {
        PgTable table = new PgTable(client(dir));
        table.assign(PgId.of(2), List.of(A, B, C), "p");
        table.assign(PgId.of(2), List.of(A, B), "p"); // down to 2 replicas
        assertThat(table.lookup(PgId.of(2)).orElseThrow().dservers()).containsExactly(A, B);
    }

    @Test
    void missingPgReturnsEmpty(@TempDir Path dir) {
        assertThat(new PgTable(client(dir)).lookup(PgId.of(999))).isEmpty();
    }
}
