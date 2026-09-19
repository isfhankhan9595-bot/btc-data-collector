# Storage namespaces

How Binance, Bybit and OKX write raw data into one `data/` directory without
sharing sequence numbers, temporary files, or rows.

## The problem this solves

`ParquetWriter` scopes three things to a **stream directory**
(`data/raw/<stream>/`): segment sequence numbers, `.tmp` files, and orphan
recovery. Sequence allocation is scan-then-create: read the directory, choose
`max + 1`, and only later open `<segment>.seg.tmp`. Nothing reserves the number
in between.

So two live writers on one stream directory both choose the same sequence and
open the **same `.tmp` path**. Worse, the second writer's orphan recovery
deletes every `*.seg.tmp` it finds, which is the first writer's live segment,
and reports it as a `DATA_DROP`. The publish-time `FileExistsError` guard does
not help; it fires only after the damage. (`test_hazard_two_unlocked_writers_…`
pins this behaviour so the lock below cannot be dismissed as unnecessary.)

PR #13's OKX capture wrote to `raw_wire` and `quality_events`, the same
directories the Binance collector uses.

## The invariant

> One venue's stream = one stream directory = at most one live writer.

Three layers enforce it. Each catches a different failure.

| Layer | Where | Catches |
|---|---|---|
| Namespace registry | `storage_layout.venue_stream` | Two venues choosing one directory. Runners take their names from it. |
| Namespace guard | `ParquetWriter.__init__` → `check_stream_namespace` | A writer declared as one venue on another venue's stream (e.g. `exchange="OKX"` on `raw_wire`, or a forgotten `exchange=` on `okx_raw_wire`). |
| Single-writer lock | `ParquetWriter` (`<stream_dir>/.writer.lock`, `flock`) | Any two live writers on one directory: duplicate service start, a mis-named stream, a future venue that skips the registry. |

## Namespace map

| Venue | `raw_wire` | `quality_events` | Other streams |
|---|---|---|---|
| Binance | `raw_wire` | `quality_events` | `raw_rest`, `orderbook`, `trades`, `markprice`, `openinterest`, `liquidation`, `binance_*_raw` |
| Bybit | `bybit_raw_wire` | `bybit_quality_events` | `bybit_orderbook`, `bybit_trades`, `bybit_markprice`, `bybit_openinterest`, `bybit_liquidation` |
| OKX | `okx_raw_wire` | `okx_quality_events` | none yet |

Binance keeps the unprefixed names because its recorded history lives there and
renaming would strand it. Any new venue must be registered in
`VENUE_STREAM_PREFIX`; `venue_stream()` raises for an unregistered venue rather
than defaulting to a shared name.

## The writer lock

- Taken in `ParquetWriter.__init__` before sequence allocation or orphan
  recovery; held for the writer's lifetime, **including across hour rollover**.
  Released by the public `close()`.
- A second writer fails at construction with `StorageWriterLockedError`, naming
  the holder's pid. It has not touched the holder's sequence, `.tmp`, or
  counter files.
- `flock` is released by the kernel when the holder dies, so a SIGKILLed
  collector never locks out its successor. The successor then recovers the
  orphaned `.tmp` as before (`DATA_DROP`, attributed to the writer's own
  `exchange`).
- `.writer.lock` is not a segment name; `parse_segment_name`, `iter_segments`,
  compaction and sequence allocation all ignore it. It is never deleted
  (unlinking a lock file re-opens the race it prevents).
- Consequence for operators: starting a second copy of a collector against the
  same `data/` now fails immediately instead of degrading silently.
- After `close()` a writer no longer owns its stream: a later `write()` that
  would roll over into a new hour raises instead of opening a segment without
  the lock.
- The lock is only as strong as the lock file's inode. Deleting
  `.writer.lock` out from under a running collector (e.g. an over-eager
  `find data -name '.*' -delete`) lets a second writer take a fresh lock on a
  new inode. Nothing in this repository deletes it.

## Legacy data and compatibility

Nothing is moved or rewritten.

- **Binance** history in `raw_wire` / `quality_events` is unchanged and still
  read from there.
- **OKX captured before this change** lives in the unprefixed `raw_wire` and
  `quality_events`, mixed with Binance's rows. New OKX data goes to
  `okx_raw_wire` / `okx_quality_events` only.
- Readers resolve the directories through `storage_layout.read_streams`.
  `read_streams("OKX", "raw_wire")` is `("okx_raw_wire", "raw_wire")`: the
  venue's own directory first, then the legacy one. **Every row is kept only if
  its own `venue` column matches the requested venue.** Rows naming another
  venue, or none, are excluded and reported in `ReplaySource.skipped_rows` (and
  logged). They are never replayed through the wrong adapter and never
  silently discarded.
- This also fixes a latent defect on the Binance side: Binance replay of a
  directory containing legacy OKX frames in `raw_wire` would previously have
  fed them to the Binance adapter.
- `replay_directory` / `scripts/replay.py --venue` now select the reader **and**
  the adapter by venue. `ReplayEngine` still supports only Binance and Bybit;
  OKX replay fails with an explicit error.

## Known limits (not closed here)

- **Compaction.** `scripts/compact_daily.py` compacts only the streams in its
  `STREAM_SCHEMAS` (the five Binance canonical streams). Venue-prefixed streams,
  and `raw_wire` for any venue, are not compacted. Compaction works per stream
  directory, so it cannot mix venues, but it does not cover Bybit or OKX yet.
- **Single host, local filesystem.** `flock` provides no cross-host exclusion
  and is unreliable on some network filesystems. The collectors are single-host
  systemd services.
- **Non-POSIX.** Without `fcntl` the lock is skipped with a logged warning;
  single-writer is then unenforced.
- **Legacy `quality_events` readers.** No reader in this repository consumes
  `quality_events` by venue today. Anything that does must filter on the
  `exchange` column when reading the unprefixed directory, which may contain
  OKX rows from before this change.
- **Sequence numbers are per stream directory**, not global. They are not
  comparable across venues and must not be used to order data between them.
