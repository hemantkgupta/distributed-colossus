package colossus.bigtable;

import colossus.common.RowKey;

/**
 * Thrown when a request reaches a tablet (or tablet-server) that no longer owns the row's range —
 * typically after a split or ownership change. The client's recovery is to refetch the routing
 * table from the MasterTablet and retry, mirroring BigTable's NOT_OWNER behavior.
 */
public class NotOwnerException extends RuntimeException {
    public NotOwnerException(RowKey key, KeyRange range) {
        super("row " + key + " not in tablet range " + range);
    }

    public NotOwnerException(String message) {
        super(message);
    }
}
