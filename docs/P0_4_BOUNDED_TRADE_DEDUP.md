# P0-4: Bounded trade-ID deduplication without false data

## 1. P0-4 definition

Reconstructed from the task's own restatement (no separate P0-4 ticket
text exists in the repository) and cross-checked against
`docs/TRADE_DEDUPLICATION.md` (PR #49/#50): `ExchangeAdapter._seen_trade_ids`
(`adapters/base.py`) is an unbounded Python `set` that grows for the
adapter instance's entire process lifetime. Unsafe for a 24/7 collector on
a small EC2 instance. The fix must not weaken exact duplicate-detection
correctness to achieve boundedness.

## 2. Current architecture (verified against source, not assumed)

- `_seen_trade_ids: set[tuple]` is a plain per-instance attribute,
  initialized empty in `ExchangeAdapter.__init__`.
- Every concrete adapter's `normalize()` is wrapped by
  `ExchangeAdapter.__init_subclass__` to call `self._dedupe_trades(...)`
  after `self._stamp_instrument(...)`, unconditionally (adapters/base.py).
- Identity key: `(exchange, market_type, instrument.key or "<unidentified>",
  stream, trade_id)`. A `trade_id is None` event is never checked against
  or added to the set (kept unconditionally).
- **Lifecycle**: every live runner (`run_collector.py`,
  `run_bybit_collector.py`, `run_binance_spot_collector.py`,
  `run_okx_collector.py`) constructs its adapter exactly once, in
  `__init__`, and never reassigns it (confirmed by grep: one occurrence of
  `= BinanceAdapter()` / `= BybitAdapter()` / etc. per file, each inside
  `__init__`). Reconnect logic lives entirely inside `WebSocketClient` and
  never touches the runner's adapter attribute -- so `_seen_trade_ids`
  survives reconnects within one process run, is lost entirely on process
  restart, and is never shared across processes.
- `ReplayEngine.__init__` constructs one adapter instance per `ReplayEngine`
  object; `ReplayEngine.run()` processes an entire `ReplaySource` (which
  can span a whole day's segments) through that one instance -- so replay's
  dedup state spans the full replay run, same lifetime shape as live.
- Concurrency: single-threaded/single-task per adapter instance. Each
  runner's websocket message handling is `async def` but processes one
  message at a time on one event loop; nothing calls `normalize()`
  concurrently for the same adapter instance. Confirmed by reading each
  runner's `handle_message`/equivalent -- no `asyncio.gather`/task-spawning
  around adapter calls.
- Quality events: a duplicate trade produces an informational
  `QualityEventType.DUPLICATE` event (`rows_lost=0`), wired through
  `UnhandledReason.DUPLICATE_TRADE` -- unaffected by this task, unchanged.
- Order of operations: `_stamp_instrument` before `_dedupe_trades`, so the
  dedup key can use the already-resolved instrument. Dedup happens before
  the event ever reaches a Parquet writer (raw capture, which happens at a
  separate call site prior to `normalize()`, is unaffected either way).

## 3. Evidence review (PR #49/#50/#51, re-verified, not re-litigated)

PR #50's Outcome B conclusion is re-confirmed, not re-derived from scratch:
no supported venue's official documentation states a maximum
duplicate-redelivery delay; Bybit's trade ID (`publicTrade`'s `i` field) is
a UUID string, ruling out any ordering-based high-water-mark design
universally. Nothing in this session's investigation contradicts that.
What this session adds is a different question PR #50 didn't need to
answer: **can RAM be bounded without evicting anything, ever** -- i.e. by
moving the exact, permanent record off the Python heap. If nothing is ever
evicted, the "we can't prove a safe expiry horizon" problem does not need
solving, because there is no expiry.

## 4. Candidate designs evaluated

| | RAM | Disk | Exactness | Latency (measured/estimated) | Crash behavior | Concurrency |
|---|---|---|---|---|---|---|
| **A. Unchanged (status quo)** | Unbounded | none | Exact | ~zero (Python set) | Everything lost on restart (accepted, documented limitation) | Single-threaded, safe |
| **B. TTL/LRU/high-water** | Bounded | none | **Not exact** -- can silently accept a real duplicate as new | fast | N/A | N/A | Rejected outright: PR #50 already proved no venue supports a safe horizon; Bybit's UUID IDs rule out high-water universally |
| **C. Bloom/probabilistic filter** | Bounded | maybe | **Not exact** -- false positives can suppress a legitimate trade | fast | N/A | N/A | Rejected: task explicitly forbids probabilistic structures as the sole authority |
| **D. SQLite exact index (this PR)** | Bounded (RAM holds only the live connection object, not the keys) | Grows, but off-heap | Exact, non-evicting | **Measured**, this sandbox: ~46,000 inserts/s sustained (21.5 μs/insert avg, per-trade autocommit, WAL + synchronous=NORMAL), ~180,000/s on the duplicate path, 200k unique 60-char keys -> 35.8 MiB on disk | New failure mode: a narrow crash window between durable insert and canonical emission (see §11) | Safe as implemented (one connection, one adapter, no concurrent access) -- WAL supports concurrent readers if ever needed later |

Options A-C from the task's own list (persistent index, partitioned
persistent state, sequence-aware dedup) collapse to variations of D once
"exact, non-evicting" is fixed as a hard requirement; D (SQLite) was
chosen over a hand-rolled append-only file + index because SQLite already
provides atomic insert-if-new semantics (`INSERT OR IGNORE` +
`cursor.rowcount`) without this project needing to implement its own
crash-safe index format, and is stdlib (no new dependency).

## 5. Decision

**Outcome A for the component; Outcome B for production wiring.**

`PersistentTradeIdIndex` (`collector/collector/persistent_trade_id_index.py`)
is a real, tested, exact, off-heap implementation -- not a document-only
conclusion. It is proven correct by randomized differential testing against
`ReferenceExactSet` (the same logic `_dedupe_trades` already uses, extracted
so it can serve as the correctness oracle) across duplicates, reconnect-style
overlaps, multiple venues/instruments/streams, and partial reordering.

**It is deliberately NOT wired into `ExchangeAdapter._dedupe_trades` as the
production default in this change.** Two reasons, stated precisely rather
than as vague caution:

1. **Deployment validation gap.** The ~46,000 inserts/s figure is this
   sandbox's disk, not the target EC2 instance's, and does not include
   contention with the Parquet writers already doing disk I/O on the same
   instance during live operation. The margin over any plausible real
   trade rate is large (multiple orders of magnitude), which is why this
   is flagged as a validation gap rather than a likely blocker -- but it
   has not been closed, and this task cannot close it from this sandbox.
2. **A genuinely new failure mode, not present today.** The in-memory set
   is wholly lost on any process restart -- today, a restart already means
   zero dedup protection for anything redelivered afterward (an accepted,
   documented limitation). A durable index changes this: if the durable
   insert succeeds but the process crashes before the corresponding
   canonical trade event is actually emitted and persisted downstream,
   that trade is durably marked "seen" without ever having been recorded
   -- a narrow-window "swallowed trade," a failure mode the current design
   does not have (because today everything about a crash is forgotten
   together). Resolving this fully would require two-phase-commit-style
   coordination between the dedup index and the canonical writers -- a
   substantially larger architectural change than "bound memory,"
   explicitly out of this task's scope per its own file-scope-control
   section.

This is not "no implementation" (Outcome B in the pure sense) -- a real,
tested, ready-to-adopt component exists. It is a deliberate choice not to
flip a live 24/7 system's hot path onto unvalidated new behavior on the
strength of a sandbox benchmark and an unresolved crash-ordering question,
when the task's own final principle is explicit: *"exactness > convenience"*
and *"do not force a result."*

## 6. Implementation

Files:
- `collector/collector/persistent_trade_id_index.py` (new): `PersistentTradeIdIndex`
  (the SQLite-backed exact index), `ReferenceExactSet` (the extracted
  reference oracle), `identity_key()` (the tested five-component join
  format), `differential_check()` (the shared driver for randomized
  differential tests), `PersistentIndexError` (fail-loud on any SQLite
  error -- open, insert, or lookup; never silently treated as "unseen" or
  "everything is a duplicate").
- `collector/tests/test_persistent_trade_id_index.py` (new): 21 tests.
- This document (new).

**`adapters/base.py` is untouched.** `_seen_trade_ids`, `_dedupe_trades`,
and the production identity contract are exactly as before this change.

Semantics preserved by the new component (proven, not asserted): identity
scoping (exchange/market_type/instrument/stream all independently tested),
reconnect-overlap suppression, atomicity (one `INSERT OR IGNORE`
transaction, no SELECT-then-INSERT race window -- enforced by a structural
test, not just a differential one).

New behavior this component *would* introduce if adopted: restart survival
(membership persists across a simulated process restart via the same file
-- proven directly), and the crash-window failure mode described in §5.

## 7. Tests

`pytest -q tests/test_persistent_trade_id_index.py` → **21 passed**.
Categories: unseen-accepted, duplicate-suppressed, exchange/market_type/
instrument/stream isolation, reconnect-overlap, restart-survival,
atomicity (structural), 3 failure-mode tests (missing directory, corrupted
file, closed connection), 5 randomized differential tests (different
seeds), missing-ID-style sentinel behavior, a throughput sanity floor, and
a row-count-accuracy check.

Full repository suite: `pytest -q tests` → **1262 passed** (1241 branch
baseline + 21 new). `compileall` → clean. `git diff --check` → clean.
`git status --short` shows exactly the two new files -- no production
source touched.

## 8. Mutation results (real source mutated, run, restored)

| Mutation | Result |
|---|---|
| Replace `INSERT OR IGNORE` + rowcount check with `INSERT OR REPLACE` always returning `True` (defeats duplicate detection) | 10 failures |
| Weaken atomic insert-if-new to a separate `SELECT` then conditional `INSERT` (reintroduces the TOCTOU race the design explicitly avoids) | 1 failure (the dedicated structural guard) |

Both applied to the real file, targeted suite run, failures recorded, file
restored, `diff -q` confirmed byte-identical, full suite re-run green.

## 9. Performance/memory evidence

**Measured** (this sandbox, Python 3.12, CPython sqlite3, WAL +
`synchronous=NORMAL`): 200,000 unique 60-character keys, per-trade
autocommit (no batching -- the worst case for durability-vs-speed):
~46,000 inserts/s (21.5 μs/insert average), ~180,000/s on the
already-seen (duplicate) path, 35.8 MiB on disk.

**Estimated, not measured**: real EC2 instance throughput and latency
under concurrent I/O with the Parquet writers. Not attempted -- would
require a real deployment, out of reach from this sandbox.

## 10. Regression audit

Unrelated systems explicitly confirmed unchanged: `adapters/base.py`
(byte-for-byte, per `git status --short` showing it absent from the
change), replay engine, CVD (cumulative and windowed), order-book
reconstruction, all four live runners, PR #54's forensic tooling
(inspected as supporting evidence only, not modified), AWS/deployment
configuration (untouched, no live exchange connection made).

## 11. Remaining limitations

- **Proven**: exactness (differential testing), atomicity (structural
  test + mutation), identity scoping, reconnect-overlap suppression,
  restart survival, fail-loud failure semantics, sandbox throughput.
- **Observed but not a guarantee**: the ~46,000 inserts/s figure is one
  sandbox's disk on one run; not a promise about the target instance.
- **Assumed, stated explicitly as assumed**: that per-trade autocommit
  durability (rather than batching several trades per transaction) is the
  right tradeoff if this is ever adopted -- batching would increase
  throughput further at the cost of a larger crash-window (more trades
  per un-flushed transaction), a tradeoff this document does not resolve
  because it depends on the real answer to the crash-ordering question in
  §5, which remains open.
- **Requires future evidence before production adoption**: (a) a real
  deployment benchmark on the target EC2 instance under concurrent load;
  (b) an explicit decision on the crash-ordering question (accept the
  narrow swallowed-trade window as-is, given it strictly improves on
  today's "everything lost on any restart," or invest in two-phase-commit
  coordination with the canonical writers); (c) if adopted, a migration
  path for existing long-running adapter instances (this change does not
  touch `ExchangeAdapter.__init__`, so there is nothing to migrate yet).

## 12. Git state

- Branch: `p0-4-bounded-trade-dedup`
- Base: `main` @ `790df62` (verified via `git rev-parse origin/main`)
- Final HEAD: see the commit this file ships in
- Changed files: `collector/collector/persistent_trade_id_index.py` (new),
  `collector/tests/test_persistent_trade_id_index.py` (new), this document
  (new). `adapters/base.py` and all other production files: unchanged.
- Working tree: clean after commit.
- PR: to be opened as **draft**, not merged, per this task's explicit
  instruction.
