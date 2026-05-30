package colossus.bigtable;

import colossus.common.ColumnKey;
import colossus.common.RowKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicatedTabletTest {

    private static final ColumnKey SIZE = ColumnKey.of("meta", "size");

    private static ReplicatedTablet threeWay(Path dir) {
        List<TabletServer> servers = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TabletServer s = new TabletServer(TabletServerId.of("ts" + i));
            s.host("t1", new Tablet(KeyRange.all(), dir.resolve("ts" + i + ".log"), true));
            servers.add(s);
        }
        return new ReplicatedTablet("t1", servers);
    }

    @Test
    void writeReplicatesToAllMembers(@TempDir Path dir) {
        ReplicatedTablet rt = threeWay(dir);
        rt.put(RowKey.of("/a"), SIZE, 1, new byte[]{42});
        for (TabletServer m : rt.members()) {
            assertThat(m.read("t1", RowKey.of("/a"), SIZE))
                    .hasValueSatisfying(v -> assertThat(v).containsExactly(42));
        }
        assertThat(rt.allReachableAgree(RowKey.of("/a"), SIZE)).isTrue();
    }

    @Test
    void readsConsistentAfterOneReplicaFails(@TempDir Path dir) {
        ReplicatedTablet rt = threeWay(dir);
        rt.put(RowKey.of("/a"), SIZE, 1, new byte[]{7});
        rt.members().get(0).setUp(false); // lose one of three — quorum (2) still met
        rt.put(RowKey.of("/a"), SIZE, 2, new byte[]{8});
        assertThat(rt.get(RowKey.of("/a"), SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(8));
        assertThat(rt.allReachableAgree(RowKey.of("/a"), SIZE)).isTrue();
    }

    @Test
    void writeFailsWhenQuorumLost(@TempDir Path dir) {
        ReplicatedTablet rt = threeWay(dir);
        rt.members().get(0).setUp(false);
        rt.members().get(1).setUp(false); // 2 of 3 down → quorum (2) not reachable
        assertThatThrownBy(() -> rt.put(RowKey.of("/a"), SIZE, 1, new byte[]{1}))
                .isInstanceOf(Faults.QuorumLostException.class);
    }

    @Test
    void readsContinueWhenQuorumLost(@TempDir Path dir) {
        ReplicatedTablet rt = threeWay(dir);
        rt.put(RowKey.of("/a"), SIZE, 1, new byte[]{5});
        rt.members().get(0).setUp(false);
        rt.members().get(1).setUp(false);
        // Failure-mode table: writes fail, but a surviving replica still serves (possibly stale) reads.
        assertThat(rt.get(RowKey.of("/a"), SIZE)).hasValueSatisfying(v -> assertThat(v).containsExactly(5));
    }

    @Test
    void casPreconditionStillEnforcedAcrossReplicas(@TempDir Path dir) {
        ReplicatedTablet rt = threeWay(dir);
        boolean first = rt.checkAndMutate(RowKey.of("/a"),
                List.of(Condition.expectAbsent(SIZE)), List.of(Mutation.put(SIZE, 1, new byte[]{1})));
        boolean second = rt.checkAndMutate(RowKey.of("/a"),
                List.of(Condition.expectAbsent(SIZE)), List.of(Mutation.put(SIZE, 2, new byte[]{2})));
        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(rt.allReachableAgree(RowKey.of("/a"), SIZE)).isTrue();
    }
}
