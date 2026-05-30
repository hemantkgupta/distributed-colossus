package colossus.curator;

import colossus.common.CuratorId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CuratorFailoverTest {

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void heartbeatUpdatesOwnershipRow(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        Curator c1 = f.curator("c1", clock);
        c1.assumeShard("/photos/");

        Instant firstHb = f.registry.lastHeartbeat("/photos/").orElseThrow();
        clock.advanceSeconds(5);
        c1.heartbeatTick();

        assertThat(f.registry.ownerOf("/photos/")).contains(CuratorId.of("c1", 9000));
        assertThat(f.registry.lastHeartbeat("/photos/").orElseThrow()).isAfter(firstHb);
    }

    @Test
    void idleCuratorClaimsStaleShardWithin15s(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        Curator c1 = f.curator("c1", clock);
        Curator c2 = f.curator("c2", clock);

        c1.assumeShard("/photos/");
        assertThat(c1.owns("/photos/")).isTrue();

        // c1 dies — stops heartbeating. Before the 15 s threshold, c2 must NOT steal the shard.
        clock.advanceSeconds(10);
        assertThat(c2.claimStaleTick()).isEmpty();
        assertThat(f.registry.ownerOf("/photos/")).contains(CuratorId.of("c1", 9000));

        // Past the threshold, c2 sees the stale heartbeat and claims via CAS.
        clock.advanceSeconds(8); // total 18 s since last heartbeat
        List<String> claimed = c2.claimStaleTick();
        assertThat(claimed).containsExactly("/photos/");
        assertThat(f.registry.ownerOf("/photos/")).contains(CuratorId.of("c2", 9000));
        assertThat(c2.owns("/photos/")).isTrue();
    }

    @Test
    void deposedCuratorDropsShardOnNextHeartbeat(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        Curator c1 = f.curator("c1", clock);
        Curator c2 = f.curator("c2", clock);

        c1.assumeShard("/photos/");
        clock.advanceSeconds(18);
        c2.claimStaleTick(); // c2 now owns the shard

        // c1's heartbeat CAS misses (it is no longer owner) → it must release the shard from memory.
        c1.heartbeatTick();
        assertThat(c1.owns("/photos/")).isFalse();
        assertThat(c2.owns("/photos/")).isTrue();
    }

    @Test
    void onlyOneIdleCuratorWinsConcurrentClaim(@TempDir Path dir) {
        CuratorFixture f = new CuratorFixture(dir);
        MutableClock clock = new MutableClock(t0);
        Curator c1 = f.curator("c1", clock);
        Curator c2 = f.curator("c2", clock);
        Curator c3 = f.curator("c3", clock);

        c1.assumeShard("/photos/");
        clock.advanceSeconds(20); // c1 stale

        int winners = 0;
        winners += c2.claimStaleTick().size();
        winners += c3.claimStaleTick().size();
        // The second claimant sees a fresh heartbeat (set by the winner) and backs off.
        assertThat(winners).isEqualTo(1);
    }
}
