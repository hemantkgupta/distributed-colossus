package colossus.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ValueTypesTest {

    @Test
    void chunkHandleEqualityAndHex() {
        assertThat(ChunkHandle.of(4242)).isEqualTo(new ChunkHandle(4242));
        assertThat(ChunkHandle.of(4242).hashCode()).isEqualTo(new ChunkHandle(4242).hashCode());
        assertThat(ChunkHandle.of(0x4242).hex()).isEqualTo("0000000000004242");
    }

    @Test
    void chunkHandleOrdering() {
        assertThat(ChunkHandle.of(1)).isLessThan(ChunkHandle.of(2));
    }

    @Test
    void filePathPrefixMath() {
        FilePath p = FilePath.of("/photos/2024/img.jpg");
        assertThat(p.prefix(8)).isEqualTo("/photos/");
        assertThat(p.prefix(1000)).isEqualTo("/photos/2024/img.jpg"); // clamps, no exception
        assertThat(p.isAbsolute()).isTrue();
        assertThat(p.toRowKey()).isEqualTo(RowKey.of("/photos/2024/img.jpg"));
    }

    @Test
    void rowKeyUnsignedOrdering() {
        // 0x80 must sort ABOVE 0x7f under unsigned comparison (signed would invert).
        RowKey low = new RowKey(new byte[]{0x7f});
        RowKey high = new RowKey(new byte[]{(byte) 0x80});
        assertThat(low).isLessThan(high);
        assertThat(RowKey.of("a")).isLessThan(RowKey.of("b"));
        assertThat(RowKey.of("a")).isLessThan(RowKey.of("aa"));
    }

    @Test
    void rowKeyDefensiveCopyAndEquality() {
        byte[] raw = {1, 2, 3};
        RowKey k = new RowKey(raw);
        raw[0] = 99;                       // mutate caller's array
        assertThat(k.bytes()[0]).isEqualTo((byte) 1); // unchanged
        assertThat(k).isNotEqualTo(RowKey.of("")); // distinct content
        assertThat(new RowKey(new byte[]{1, 2, 3})).isEqualTo(new RowKey(new byte[]{1, 2, 3}));
        assertThat(new RowKey(new byte[]{1, 2, 3}).hashCode())
                .isEqualTo(new RowKey(new byte[]{1, 2, 3}).hashCode());
    }

    @Test
    void columnKeyFullForm() {
        assertThat(ColumnKey.of("locations", "0000000000004242").full())
                .isEqualTo("locations:0000000000004242");
        assertThat(ColumnKey.of("a", "z")).isLessThan(ColumnKey.of("b", "a"));
    }

    @Test
    void pgIdRowKeyRendering() {
        assertThat(PgId.of(0x1234).rowKey()).isEqualTo("pg/00001234");
        assertThat(PgId.of(1)).isLessThan(PgId.of(2));
    }

    @Test
    void dserverIdHostPortSplit() {
        DserverId d = DserverId.of("10.0.0.5", 7100);
        assertThat(d.hostPort()).isEqualTo("10.0.0.5:7100");
        assertThat(d.host()).isEqualTo("10.0.0.5");
        assertThat(d.port()).isEqualTo(7100);
    }

    @Test
    void leaseValidityWindow() {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        LeaseToken lease = new LeaseToken(ChunkHandle.of(1), DserverId.of("h", 1), t0, t0.plusSeconds(60));
        assertThat(lease.isValid(t0.plusSeconds(59))).isTrue();
        assertThat(lease.isValid(t0.plusSeconds(61))).isFalse();
        assertThat(lease.renew(t0.plusSeconds(30), 60).expiresAt())
                .isEqualTo(t0.plusSeconds(90));
    }

    @Test
    void crc32cDetectsCorruption() {
        byte[] data = "hello colossus".getBytes();
        int good = Crc32c.of(data);
        data[0] ^= 0x01;
        assertThat(Crc32c.of(data)).isNotEqualTo(good);
    }

    @Test
    void constantsMatchPlan() {
        assertThat(Constants.CHUNK_SIZE_BYTES).isEqualTo(67108864);
        assertThat(Constants.PG_COUNT_PER_POOL).isEqualTo(4096);
        assertThat(List.of(Constants.HEARTBEAT_INTERVAL_SECONDS, Constants.HEARTBEAT_MISSED_THRESHOLD))
                .containsExactly(5, 3);
    }
}
