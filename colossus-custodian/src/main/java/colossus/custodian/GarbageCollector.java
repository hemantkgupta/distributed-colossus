package colossus.custodian;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Row;
import colossus.common.ChunkHandle;
import colossus.common.DserverId;
import colossus.common.PgId;
import colossus.common.WireReader;
import colossus.placement.HashRouter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reclaims <em>orphaned extents</em> — extents physically held by a D-server that no live file row in
 * the namespace references any more. This is the GFS-reaper / deletion-reclamation mechanism: deletion
 * in Colossus is lazy. {@code CuratorShard.delete} tombstones every cell of a file row (including its
 * whole {@code chunks:} family), but the extents stay resident on the D-servers — there is no
 * synchronous "go delete these bytes" RPC. The Garbage Collector closes that gap out-of-band: it takes
 * the authoritative set of <em>live</em> chunk handles (every handle still referenced by a non-deleted
 * file row) and subtracts it from a D-server's hosted set; whatever remains is garbage and gets a
 * {@link WorkItem.Op#DELETE_EXTENT} work item dispatched against it.
 *
 * <p>Operating on observed state — the namespace rows read straight from BigTable and the D-server's
 * own hosted-handle set — rather than on Curator directives is the same design stance the
 * {@link Custodian} takes (ADR 0004): the control plane never trusts a delete notification it might
 * have missed during a partition; it reconciles against ground truth. A handle is only ever treated as
 * an orphan when it is provably absent from every live file row under the scanned prefix, so a
 * straggling replica from a partition is collected only once the namespace agrees it is unreferenced.
 *
 * <p>It is stateless and bound to one {@code target} D-server (the one whose hosted set is being
 * reclaimed). The Custodian, in its GC tick, constructs one of these per D-server it sweeps.
 */
public final class GarbageCollector {

    private final BigTableClient bt;
    private final DserverId target;
    private final HashRouter router;

    /**
     * @param bt     reads live namespace file rows
     * @param target the D-server whose hosted extents are being reclaimed — the target of every
     *               {@link WorkItem.Op#DELETE_EXTENT} this collector emits
     * @param router maps an orphan handle to its owning PG (deterministic, ADR 0005), so the emitted
     *               work item carries the PG that owns the extent being reclaimed
     */
    public GarbageCollector(BigTableClient bt, DserverId target, HashRouter router) {
        this.bt = bt;
        this.target = target;
        this.router = router;
    }

    /**
     * Scan the namespace rows under {@code namespacePrefix} (e.g. {@code "/"}) and collect every chunk
     * handle referenced by a live file row's {@code chunks:} family. A deleted file's row still appears
     * in the scan, but its columns are tombstones, so {@link Row#family(String)} returns no live values
     * for it — deleted files contribute nothing, which is exactly the orphan signal we want.
     *
     * <p>Handles are decoded inline from the 8-byte big-endian wire format the namespace schema uses
     * (the same {@code i64(handle.id())} layout {@code NamespaceCodec.handleBytes} writes). The
     * Custodian must not depend on the Curator module, so the decode is done here with
     * {@link WireReader} rather than via {@code NamespaceCodec}.
     */
    public Set<ChunkHandle> liveHandles(String namespacePrefix) {
        Set<ChunkHandle> live = new HashSet<>();
        for (Row row : bt.scan(namespacePrefix).values()) {
            // The "chunks" family maps qualifier (chunk index) -> 8-byte handle value, latest live only.
            for (byte[] handleBytes : row.family("chunks").values()) {
                live.add(ChunkHandle.of(new WireReader(handleBytes).i64()));
            }
        }
        return live;
    }

    /**
     * Return the hosted handles that no live file row references — the orphans. Order follows the
     * iteration order of {@code hostedHandles}; duplicates in the input are de-duplicated.
     */
    public List<ChunkHandle> findOrphans(Collection<ChunkHandle> hostedHandles, String namespacePrefix) {
        Set<ChunkHandle> live = liveHandles(namespacePrefix);
        List<ChunkHandle> orphans = new ArrayList<>();
        Set<ChunkHandle> seen = new HashSet<>();
        for (ChunkHandle h : hostedHandles) {
            if (!live.contains(h) && seen.add(h)) {
                orphans.add(h);
            }
        }
        return orphans;
    }

    /**
     * For each orphan among {@code hostedHandles}, dispatch a {@link WorkItem.Op#DELETE_EXTENT} work
     * item against this collector's target D-server and return the dispatched items. The owning PG is
     * derived deterministically from the handle so the work item names the PG that owns the reclaimed
     * extent. Reclamation runs on the BACKGROUND lane (it is never urgent — the bytes are already dead).
     */
    public List<WorkItem> reclaim(Collection<ChunkHandle> hostedHandles, String namespacePrefix,
                                  WorkDispatcher dispatcher) {
        List<WorkItem> dispatched = new ArrayList<>();
        for (ChunkHandle orphan : findOrphans(hostedHandles, namespacePrefix)) {
            PgId pg = router.placementGroupFor(orphan.hex());
            WorkItem item = WorkItem.deleteExtent(pg, target);
            WorkDispatcher.WorkResult r = dispatcher.dispatch(item);
            if (r.ok()) {
                dispatched.add(item);
            }
        }
        return dispatched;
    }
}
