package colossus.client;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.BigTableCluster;
import colossus.common.CuratorId;
import colossus.common.DserverId;
import colossus.common.FilePath;
import colossus.curator.Curator;
import colossus.curator.CuratorShard;
import colossus.curator.LeaseManager;
import colossus.curator.PlacementProvider;
import colossus.curator.ShardOwnershipRegistry;
import colossus.dserver.Dserver;
import colossus.dserver.ExtentStore;
import colossus.placement.HashRouter;
import colossus.placement.PgTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColossusClientTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void writeSplitsPayloadAcrossChunksAndReadsBoundaries(@TempDir Path dir) {
        ClientFixture f = new ClientFixture(dir, 16);
        FilePath path = FilePath.of("/data/payload.bin");
        byte[] payload = new byte[40];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 13 + 5);
        }

        assertThat(f.client.create(path, "alice")).isTrue();
        f.client.write(path, payload);

        assertThat(f.client.stat(path).chunkCount()).isEqualTo(3);
        assertThat(f.client.read(path, 0, payload.length)).isEqualTo(payload);
        assertThat(f.client.read(path, 14, 8)).isEqualTo(Arrays.copyOfRange(payload, 14, 22));
    }

    @Test
    void appendRejectsPayloadLargerThanOneChunkBeforeAllocating(@TempDir Path dir) {
        ClientFixture f = new ClientFixture(dir, 8);
        FilePath path = FilePath.of("/data/log");
        assertThat(f.client.create(path, "alice")).isTrue();

        assertThatThrownBy(() -> f.client.append(path, new byte[9]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds chunkSize");
        assertThat(f.client.stat(path).chunkCount()).isZero();
    }

    @Test
    void constructorRejectsNonPositiveChunkSize() {
        assertThatThrownBy(() -> new ColossusClient(emptyView(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize");
    }

    @Test
    void readRejectsNegativeRanges() {
        ColossusClient client = new ColossusClient(emptyView(), 16);

        assertThatThrownBy(() -> client.read(FilePath.of("/data/f"), -1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
        assertThatThrownBy(() -> client.read(FilePath.of("/data/f"), 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }

    @Test
    void curatorResolutionRetriesTransientWrongShard(@TempDir Path dir) {
        ClientFixture f = new ClientFixture(dir, 16);
        AtomicInteger attempts = new AtomicInteger();
        ClusterView flaky = new ClusterView() {
            @Override
            public Optional<CuratorShard> curatorFor(FilePath path) {
                return attempts.incrementAndGet() < 3 ? Optional.empty() : f.curatorFor(path);
            }

            @Override
            public Dserver dserver(DserverId id) {
                return f.dserver(id);
            }
        };
        ColossusClient client = new ColossusClient(flaky, 16);

        assertThat(client.create(FilePath.of("/data/f"), "alice")).isTrue();
        assertThat(attempts.get()).isEqualTo(3);
    }

    private static ClusterView emptyView() {
        return new ClusterView() {
            @Override
            public Optional<CuratorShard> curatorFor(FilePath path) {
                return Optional.empty();
            }

            @Override
            public Dserver dserver(DserverId id) {
                throw new AssertionError("no D-server expected");
            }
        };
    }

    private static final class ClientFixture implements ClusterView {
        private final CuratorShard shard;
        private final Map<DserverId, Dserver> dservers = new LinkedHashMap<>();
        private final ColossusClient client;

        private ClientFixture(Path dir, int chunkSize) {
            BigTableClient bt = new BigTableClient(
                    new BigTableCluster(dir.resolve("bt"), 3, 1_000_000, Long.MAX_VALUE, false));
            HashRouter router = new HashRouter(4096);
            List<DserverId> ids = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                DserverId id = DserverId.of("d" + i, 7100);
                ids.add(id);
                dservers.put(id, new Dserver(id, new ExtentStore(dir.resolve("ext").resolve("d" + i), false),
                        Dserver.defaultScheduler(), null, 1_000_000_000L));
            }
            PlacementProvider placement = new PlacementProvider(new PgTable(bt), ids, 3, "rack-distinct-3x");
            Curator curator = new Curator(CuratorId.of("c1", 9000), bt, new ShardOwnershipRegistry(bt),
                    placement, router, new LeaseManager(bt, 60),
                    Clock.fixed(T0, ZoneOffset.UTC), 60, 15);
            this.shard = curator.assumeShard("/");
            this.client = new ColossusClient(this, chunkSize);
        }

        @Override
        public Optional<CuratorShard> curatorFor(FilePath path) {
            return shard.owns(path) ? Optional.of(shard) : Optional.empty();
        }

        @Override
        public Dserver dserver(DserverId id) {
            return dservers.get(id);
        }
    }
}
