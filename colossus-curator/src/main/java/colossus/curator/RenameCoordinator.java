package colossus.curator;

import colossus.bigtable.BigTableClient;
import colossus.bigtable.Condition;
import colossus.bigtable.Mutation;
import colossus.bigtable.Row;
import colossus.common.ColumnKey;
import colossus.common.FilePath;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Cross-shard rename (§6.10). A rename whose source and destination fall in different Curator shards
 * spans two BigTable rows, possibly in different tablets — which BigTable cannot mutate in one
 * transaction. Real Colossus reaches for Spanner here; this build ships a client-driven two-phase
 * protocol instead, and is explicit that it is <b>not strictly serializable</b> under concurrent
 * renames of overlapping paths (ADR 0013).
 *
 * <pre>
 *   PREPARE   on source: CAS a RENAME_TARGET marker onto the old row (state = RENAMING)
 *   COMMIT    on dest:   CAS the old row's snapshot into the new row (create-if-absent)
 *   FINALIZE  on source: tombstone the old row
 * </pre>
 *
 * A crash between PREPARE and FINALIZE leaves a dangling RENAMING row; {@link #recover} resolves it —
 * finalize if the destination already exists, otherwise revert the source to ACTIVE. The same protocol
 * is used for same-shard renames too (this build's BigTable has no cross-row atomicity even within a
 * shard); real Colossus does the same-shard case in a single CAS-pair.
 */
public final class RenameCoordinator {

    private final BigTableClient bt;
    private final Clock clock;
    private long lastTs = 0;

    public RenameCoordinator(BigTableClient bt, Clock clock) {
        this.bt = bt;
        this.clock = clock;
    }

    private synchronized long nextTs() {
        lastTs = Math.max(clock.instant().toEpochMilli(), lastTs + 1);
        return lastTs;
    }

    private boolean exists(FilePath p) {
        return bt.getRow(p.toRowKey()).map(r -> r.get(NamespaceCodec.META_SIZE).isPresent()).orElse(false);
    }

    /** Snapshot of a row's live columns, excluding the rename marker. */
    private Map<ColumnKey, byte[]> snapshot(FilePath p) {
        Map<ColumnKey, byte[]> out = new LinkedHashMap<>();
        Row row = bt.getRow(p.toRowKey()).orElseThrow();
        for (ColumnKey c : row.liveColumns()) {
            if (c.equals(NamespaceCodec.RENAME_TARGET)) {
                continue;
            }
            row.get(c).ifPresent(v -> out.put(c, v));
        }
        return out;
    }

    private void finalizeDelete(FilePath p) {
        Row row = bt.getRow(p.toRowKey()).orElse(null);
        if (row == null) {
            return;
        }
        long t = nextTs();
        List<Mutation> deletes = new ArrayList<>();
        for (ColumnKey c : row.liveColumns()) {
            deletes.add(Mutation.delete(c, t));
        }
        bt.checkAndMutate(p.toRowKey(), List.of(), deletes);
    }

    /**
     * Rename {@code oldPath} → {@code newPath}. Returns true on success; false if the destination
     * already exists or the source is already mid-rename. Throws if the source does not exist.
     */
    public boolean rename(FilePath oldPath, FilePath newPath) {
        if (!exists(oldPath)) {
            throw new CuratorShard.FileNotFoundException(oldPath);
        }
        if (exists(newPath)) {
            return false; // destination occupied
        }
        // PREPARE — mark the source RENAMING (fails if another rename already claimed it).
        boolean prepared = bt.checkAndMutate(oldPath.toRowKey(),
                List.of(Condition.expectAbsent(NamespaceCodec.RENAME_TARGET)),
                List.of(Mutation.put(NamespaceCodec.RENAME_TARGET, nextTs(),
                        newPath.path().getBytes(StandardCharsets.UTF_8))));
        if (!prepared) {
            return false;
        }
        // COMMIT — create the destination row from the source snapshot.
        Map<ColumnKey, byte[]> snap = snapshot(oldPath);
        long t = nextTs();
        List<Mutation> muts = new ArrayList<>();
        for (var e : snap.entrySet()) {
            muts.add(Mutation.put(e.getKey(), t, e.getValue()));
        }
        boolean committed = bt.checkAndMutate(newPath.toRowKey(),
                List.of(Condition.expectAbsent(NamespaceCodec.META_SIZE)), muts);
        if (!committed) {
            // Destination appeared concurrently — abort: drop the marker, leave the source intact.
            bt.checkAndMutate(oldPath.toRowKey(), List.of(),
                    List.of(Mutation.delete(NamespaceCodec.RENAME_TARGET, nextTs())));
            return false;
        }
        // FINALIZE — delete the source.
        finalizeDelete(oldPath);
        return true;
    }

    /**
     * Resolve dangling renames under a namespace prefix (a client crashed between PREPARE and
     * FINALIZE). For each row still carrying a RENAME_TARGET: if the destination row exists the commit
     * succeeded, so finalize (delete the source); otherwise revert the source to ACTIVE.
     *
     * @return the source paths it resolved
     */
    public List<FilePath> recover(String namespacePrefix) {
        List<FilePath> resolved = new ArrayList<>();
        for (var e : bt.scan(namespacePrefix).entrySet()) {
            Row row = e.getValue();
            Optional<byte[]> target = row.get(NamespaceCodec.RENAME_TARGET);
            if (target.isEmpty()) {
                continue;
            }
            FilePath src = FilePath.of(e.getKey().asString());
            FilePath dst = FilePath.of(new String(target.get(), StandardCharsets.UTF_8));
            if (exists(dst)) {
                finalizeDelete(src);            // commit had completed — finish it
            } else {
                bt.checkAndMutate(src.toRowKey(), List.of(),
                        List.of(Mutation.delete(NamespaceCodec.RENAME_TARGET, nextTs()))); // revert
            }
            resolved.add(src);
        }
        return resolved;
    }
}
