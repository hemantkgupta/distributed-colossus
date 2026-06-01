package colossus.simulator;

import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.dserver.Dserver;
import colossus.erasure.ReedSolomon;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * The simulator-level integration of the durability lifecycle's re-encode step (CP33). It plays the
 * role of the Custodian's tier loop dispatching erasure-coding work to D-servers: read a sealed,
 * replicated extent, RS(6,3)-encode it into 9 fragments spread across distinct D-servers, then drop
 * the replicas. It also performs degraded reads — gathering any k surviving fragments and
 * reconstructing. (Lives in the simulator because it spans the control plane's decision and the data
 * plane's bytes, which the Custodian module deliberately cannot.)
 */
public final class ErasureCoordinator {

    /** The result of erasure-coding a chunk: where its 9 fragments live + the geometry to read them back. */
    public record EcChunk(ChunkHandle handle, List<DserverId> fragmentHosts, int originalLen, int shardLen) {
        public EcChunk {
            fragmentHosts = List.copyOf(fragmentHosts);
        }
    }

    private final Function<DserverId, Dserver> lookup;
    private final ReedSolomon rs = ReedSolomon.rs63(); // k=6, m=3

    public ErasureCoordinator(Function<DserverId, Dserver> lookup) {
        this.lookup = lookup;
    }

    /**
     * Re-encode a replicated extent to RS(6,3): read it from a surviving replica, split into 6 data
     * shards (zero-padded), compute 3 parity shards, write all 9 fragments across {@code fragmentHosts},
     * then reclaim the replicas. Returns the placement needed to read it back.
     */
    public EcChunk reEncode(ChunkHandle handle, List<DserverId> replicas, List<DserverId> fragmentHosts) {
        int k = rs.dataShards();
        int n = rs.totalShards();
        if (fragmentHosts.size() < n) {
            throw new IllegalArgumentException("need " + n + " fragment hosts, got " + fragmentHosts.size());
        }
        Dserver src = lookup.apply(replicas.get(0));
        int len = (int) src.store().length(handle);
        byte[] data = src.store().read(handle, 0, len);

        int shardLen = Math.max(1, (int) Math.ceil(len / (double) k));
        byte[][] dataShards = new byte[k][shardLen];
        for (int i = 0; i < k; i++) {
            int off = i * shardLen;
            int copy = Math.min(shardLen, Math.max(0, len - off));
            if (copy > 0) {
                System.arraycopy(data, off, dataShards[i], 0, copy);
            }
        }
        byte[][] parity = rs.encode(dataShards);

        for (int i = 0; i < k; i++) {
            lookup.apply(fragmentHosts.get(i)).store().writeFragment(handle, i, dataShards[i]);
        }
        for (int j = 0; j < rs.parityShards(); j++) {
            lookup.apply(fragmentHosts.get(k + j)).store().writeFragment(handle, k + j, parity[j]);
        }
        // Reclaim the replicas only after every fragment is durable (crash-safe ordering).
        for (DserverId r : replicas) {
            lookup.apply(r).dropExtent(handle);
        }
        return new EcChunk(handle, fragmentHosts.subList(0, n), len, shardLen);
    }

    /**
     * Read an erasure-coded chunk, tolerating up to m down hosts. Present fragments are read directly
     * (systematic code → data fragments are plaintext); if a data fragment is missing it is
     * reconstructed from any k survivors.
     */
    public byte[] read(EcChunk ec, Set<DserverId> down) {
        int n = ec.fragmentHosts().size();
        byte[][] shards = new byte[n][];
        boolean[] present = new boolean[n];
        for (int i = 0; i < n; i++) {
            DserverId host = ec.fragmentHosts().get(i);
            Dserver d = lookup.apply(host);
            if (!down.contains(host) && d != null && d.store().hasFragment(ec.handle(), i)) {
                shards[i] = d.store().readFragment(ec.handle(), i);
                present[i] = true;
            }
        }
        byte[][] dataShards = rs.reconstruct(shards, present); // k recovered data shards
        int k = rs.dataShards();
        byte[] all = new byte[k * ec.shardLen()];
        for (int i = 0; i < k; i++) {
            System.arraycopy(dataShards[i], 0, all, i * ec.shardLen(), ec.shardLen());
        }
        return Arrays.copyOf(all, ec.originalLen());
    }
}
