# Wire format

Hand-rolled, length-prefixed binary framing (no protobuf/JSON/Java-serialization — ADR 0012).
Implemented in `colossus-common`: `WireWriter`, `WireReader`, `WireFrame`, `MessageType`.

## Frame

```
+--------------+--------+----------+----------------+----------+
| length (4B)  | type   | reqId    | shard / epoch  | payload  |
| big-endian   | (1 B)  | (4 B)    | (4 B)          | (rest)   |
+--------------+--------+----------+----------------+----------+
```

- `length` includes itself.
- `type` is a `MessageType` code (see the enum; 43 codes across the plane boundaries).
- `shard / epoch`: the target shard id for Curator RPCs; the client's cached placement epoch for
  D-server RPCs (so a stale client is rejected with `STALE_PLACEMENT_REPLY`); 0 otherwise.

## Encoding primitives

- ints big-endian; `i64` for handles and timestamps (millis).
- strings: 2-byte length-prefixed UTF-8 (binary-safe — not modified UTF-8).
- byte arrays: 4-byte length-prefixed; `shortBytes` (RowKey/ColumnKey): 2-byte length-prefixed.

The in-process build uses these codecs for the BigTable WAL records, the namespace/placement cell
values, and the status/ownership rows. A networked build would frame the same bytes over TCP.
