package colossus.bigtable;

import colossus.common.RowKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MasterTabletTest {

    @Test
    void routesToCorrectTabletByFloorLookup(@TempDir Path dir) {
        try (MasterTablet mt = new MasterTablet(dir.resolve("master.log"), true)) {
            mt.assignRange(RowKey.of(""), "tabletA");        // [-inf, /m)
            mt.assignRange(RowKey.of("/m"), "tabletB");      // [/m, +inf)
            assertThat(mt.route(RowKey.of("/a"))).contains("tabletA");
            assertThat(mt.route(RowKey.of("/lll"))).contains("tabletA");
            assertThat(mt.route(RowKey.of("/m"))).contains("tabletB");   // boundary is inclusive start
            assertThat(mt.route(RowKey.of("/zzz"))).contains("tabletB");
        }
    }

    @Test
    void routingSurvivesRestart(@TempDir Path dir) {
        Path wal = dir.resolve("master.log");
        try (MasterTablet mt = new MasterTablet(wal, true)) {
            mt.assignRange(RowKey.of(""), "tabletA");
            mt.assignRange(RowKey.of("/photos"), "tabletB");
        }
        try (MasterTablet reopened = new MasterTablet(wal, true)) {
            assertThat(reopened.rangeCount()).isEqualTo(2);
            assertThat(reopened.route(RowKey.of("/photos/x"))).contains("tabletB");
            assertThat(reopened.route(RowKey.of("/docs"))).contains("tabletA");
        }
    }

    @Test
    void reassigningRangeUpdatesOwner(@TempDir Path dir) {
        try (MasterTablet mt = new MasterTablet(dir.resolve("master.log"), true)) {
            mt.assignRange(RowKey.of(""), "tabletA");
            mt.assignRange(RowKey.of("/m"), "tabletB"); // split point introduced
            assertThat(mt.route(RowKey.of("/x"))).contains("tabletB");
            assertThat(mt.rangeCount()).isEqualTo(2);
        }
    }
}
