# Deterministic replay

Implemented in `collector/collector/replay.py`; CLI at `collector/scripts/replay.py`.

## What was here before

`scripts/replay_test.py` loaded a derived Parquet file and asserted bounds on
its columns — no NaNs, OBI within [-1, 1], positive spread. That is a dataset
sanity check. It never replayed anything: no recorded clock, no raw event
source, no reconstruction, no determinism check. It could not have detected a
reconstruction bug, because it never ran the reconstruction.

It has been deleted.

## What replay means here

Replay drives the **same objects** the live collector drives:

```
recorded frame -> BinanceAdapter.normalize() -> LocalBook.apply() -> quality state
```

Only the clock and the source differ. There is deliberately no replay-only
reconstruction path: a second implementation would be free to diverge from
production and would mask exactly the bugs replay exists to catch.

## Causality

Recorded websocket frames and recorded REST snapshot responses merge into
**one** time-ordered stream:

- a websocket frame becomes available at its `local_receive_ts`
- a REST snapshot becomes available at its `response_receive_ts`

This mirrors live, where a snapshot requested during a gap arrives
asynchronously and can only bridge once it has landed. A snapshot is never
visible before the moment it arrived in the recorded run, so replay cannot
repair a gap with information from the future. A snapshot request with no
response is excluded entirely — it bridged nothing live, so it bridges
nothing here.

## Determinism

Frames carry a total order key `(timestamp, kind_rank, source_index)`, so
ties never depend on filesystem iteration, dict ordering or sort stability.
Within one millisecond a wire frame sorts before a snapshot, matching live,
where the diff was already in the socket buffer when the HTTP response
completed.

`ReplayResult.digest` hashes book states **and** quality transitions.
A replay that reached the same prices by a different quality path does not
compare equal.

```bash
python -m collector.scripts.replay 2026-06-03 --data-dir data --verify-determinism
```

## No network

The module imports no HTTP client and performs no I/O beyond reading recorded
segments. Enforced by an AST-based test over the module's own imports, not a
substring scan.

## What replay refuses to do

| Situation | Behaviour |
|---|---|
| frame was undecodable when recorded | stays undecodable; counted, never re-parsed |
| snapshot request failed | recorded as an attempt; bridges nothing |
| snapshot never returned | excluded from the stream entirely |
| snapshot malformed or empty | rejected, counted |
| `depth10` partial | refused as non-authoritative, as live does |
| broken chain, later tidy increments | stays non-VALID; increments cannot repair it |
| no recorded frames at all | CLI exits 1 — an empty replay is not a clean replay |

## Known limitations

- **Binance only.** Bybit and OKX have no live client feeding raw capture,
  so there is nothing recorded to replay.
- Replay reconstructs the **order book**. Trades, mark price, OI and
  liquidations are captured in `raw_wire` but are not yet driven through
  their feature paths during replay.
- Parity is demonstrated against a hand-driven `LocalBook` over the same
  frames. A full live-vs-replay parity harness over a captured production
  session does not exist yet.
- Binance duplicate-diff semantics remain unverified against current official
  documentation (D14). `BinanceSequenceComparator` has no duplicate branch, so
  a resent diff trips the `pu` rule and is treated as a gap. That is the
  conservative outcome — it cannot produce a falsely VALID book — but it is an
  assumption, not a documented rule, and is asserted as such in tests.
