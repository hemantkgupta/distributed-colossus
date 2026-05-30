# BigTable row schemas

One file = one row; row atomicity = file atomicity (ADR 0002). All values are encoded with the
`colossus-common` wire codecs.

## Namespace table (row key = file path)

| Family      | Qualifier          | Value |
|-------------|--------------------|-------|
| `meta`      | `size`             | 8B long |
| `meta`      | `ctime` / `mtime`  | 8B millis |
| `meta`      | `owner`            | UTF-8 |
| `chunks`    | `<index>`          | ChunkHandle (8B) |
| `locations` | `<handle-hex>`     | count + list of `host:port` |
| `lease`     | `<handle-hex>`     | handle + primary + grantedAt + expiresAt |
| `scrub`     | `last_completed`   | 8B millis |

A `GetChunkLocations` is a single row read; `AllocateChunk` is a single CAS recording the chunk +
locations + lease together.

## Placement table `placement.pg_table` (row key = `pg/<8 hex>`)

| Family     | Qualifier   | Value |
|------------|-------------|-------|
| `dservers` | `0..n`      | replica set in primary order |
| `epoch`    | `current`   | 4B int, bumps on any replica-set change |
| `policy`   | `rule`      | UTF-8 policy name |

## Membership / ownership rows

- `dserver.status.<host:port>` — `hb:ts`, `disk:free`, `extents:count`. Liveness = heartbeat freshness.
- `curator.shards.<prefix>` — `owner:id`, `hb:ts`. Ownership transfer is a CAS on this row.
