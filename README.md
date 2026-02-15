# zola_db

An embedded Rust library for time-series asof joins. Inspired by kdb+, it stores by-symbol tick data as mmap'd column files partitioned by date and answers "what was the last (or next) value for this symbol at this time?" queries.

## Design goals

- **Simplicity** — small API surface (`Db::open`, `write`, `asof`), flat file layout, no query language.
- **Zero-copy reads** — partitions are mmap'd at open; column access goes straight from page cache to typed slices via `zerocopy`.
- **Predictable performance** — binary search within sorted partitions, O(1) cross-partition lookback via per-partition sidecars.

## On-disk layout

```
<root>/<table>/.schema              # "name:type" per value column
<root>/<table>/<YYYY.MM.DD>/
    timestamp.col                   # i64 microseconds since epoch
    symbol.col                      # i64 symbol ids
    <value>.col                     # i64 or f64 per schema
    .parted                         # sorted (symbol_id, start, end) index
    .last_values                    # sidecar: last value per symbol
    .first_values                   # sidecar: first value per symbol
```

Column files have a 24-byte header (magic `ZOLA`, version, column type, row count) followed by native-endian 8-byte values. Partitions are written atomically via a temp-dir rename pattern.

## API

| Method | Purpose |
|---|---|
| `Db::open(root)` | Open or create a database, mmap all existing partitions |
| `db.write(table, schema, timestamps, symbols, columns)` | Append rows, auto-partitioning by date |
| `db.asof(table, probes, direction)` | Asof join: find nearest row per (symbol, timestamp) probe |

`Direction::Backward` finds the most recent row at or before the probe time. `Direction::Forward` finds the earliest row at or after.

## Tradeoffs

- **Single-hop lookback** — cross-partition asof checks only the immediate neighbor's sidecar. If a symbol is absent from that neighbor, the result is NULL rather than searching further back. This keeps query time bounded.
- **No concurrency** — single-writer, single-reader via `&mut self` / `&self`. No locks, no WAL.
- **No symbol intern table** — symbols are bare `i64` ids. The caller owns the mapping from strings to ids.
- **mmap-dependent** — read performance relies on the OS page cache. No custom buffer pool.
- **Host-endian** — column files use native byte order. Database files are not portable across architectures.
- **Microsecond timestamps** — fixed resolution; no configurable precision.
