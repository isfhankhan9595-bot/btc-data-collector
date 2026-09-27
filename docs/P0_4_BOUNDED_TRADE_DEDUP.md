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

## 11. Crash-consistency analysis (this session's core finding)

### The storage durability boundary, read directly from source

`ParquetWriter.write()` only appends to an in-memory Python list
(`self.buffer`). `flush()` moves buffered rows into the still-open,
uncommitted `.tmp` Parquet writer and atomically persists a row-count
sidecar (`fsync` + `os.replace`) — but **does not `fsync` the `.tmp`
Parquet file itself**. The `.tmp` file only becomes crash-durable inside
`_close_segment()`, which explicitly `fsync`s it before `os.replace`ing it
to its final published name. A segment closes only every
`segment_seconds` (default 30s) or `segment_rows` (default 5,000) — not
per trade.

`_recover_orphans()`, run on every `ParquetWriter` construction (i.e. on
every restart), deletes any `*.seg.tmp` left over from a crash and emits
an honest `DATA_DROP` quality event for however many rows it counted via
the sidecar. **Verified empirically in this session, not just read from
source**: a `ParquetWriter` was flushed (rows moved into the open
writer) but never closed, then a fresh `ParquetWriter` was constructed
for the same stream directory (simulating a restart) — the orphaned
`.tmp` file was deleted and a `DATA_DROP` (`rows_lost=1`) was emitted, as
predicted.

`RawCapture` (`raw_capture.py`) is built on the exact same `ParquetWriter`
abstraction, so raw wire evidence has the identical ~30s/5,000-row
durability lag and the identical crash-discard behavior. **This means
Invariant E ("raw wire evidence must remain intact even when canonical
dedup suppresses an event") is only true up to this same boundary** — a
crash inside the buffering window loses both raw and canonical evidence
for that window symmetrically, which is the *existing*, already-accepted
contract of this storage layer, not something P0-4 introduces or can fix.

### Why this changes the crash-ordering analysis

The task's own framing (§6, options A and B) implicitly assumes "canonical
trade write" is a single, synchronously-observable durable event that a
dedup insert can be ordered before or after. **It is not.** A trade's
canonical row becomes durable only when its *containing segment* closes,
up to 30 seconds or 5,000 rows later — an emergent property of a batched
lifecycle, not a per-trade event. This reframes both orderings:

**Option A (dedup-first: insert into SQLite, then hand the row to
`ParquetWriter.write()`).** If the process crashes before that trade's
segment closes, `_recover_orphans` already discards the row and reports
`DATA_DROP` — **today, with no dedup at all**. Wiring in dedup-first
makes this *worse*, not merely equally risky: the SQLite index would
already durably remember the ID as seen, so a venue redelivery of the
same trade (which would otherwise let the collector recover the lost
row) is now **permanently and silently suppressed** — the reported,
bounded `DATA_DROP` becomes an unreported, permanent gap. Verified
directly from the interaction between `_dedupe_trades`'s synchronous
insert-then-emit contract and `_recover_orphans`'s unconditional discard.

**Option B (canonical-first: hand the row to `ParquetWriter.write()`,
then insert into SQLite).** Two sub-cases, not one:

- *Crash before the segment closes*: the row is discarded by
  `_recover_orphans` exactly as above, **and** the dedup insert never
  ran — so a later redelivery is correctly treated as new and produces
  exactly one canonical row. This sub-case is safe, and is safe for a
  specific reason: the same crash discards both consistently.
- *Crash after the segment closes (row durably published) but before the
  dedup insert commits*: the canonical row **is** durably on disk, but
  the index does not know it. A redelivery is (correctly, by the index's
  own logic) treated as new, and a genuine **duplicate canonical row is
  written** — the exact failure mode the task's §6.B names. This window
  recurs roughly every 30 seconds (or 5,000 rows) per stream — it is not
  a rare edge case, it is a *routine* segment boundary.

**Neither simple ordering is safe across the whole lifecycle** with the
current buffered-segment architecture, for a structural reason (the
granularity mismatch), not a missing feature of `PersistentTradeIdIndex`
itself.

### The smallest correct fix identified, not implemented

Move the dedup **commit** to segment-close granularity instead of
per-trade: batch every trade ID a segment durably contains and insert
them into the persistent index in the same step that publishes the
segment (immediately after the `fsync`+`os.replace` that makes the
segment durable, ideally using the segment's own publication as the
recovery anchor — e.g. a startup reconciliation pass that can detect a
published segment whose IDs are not yet reflected in the index and
finish that batch insert before accepting new traffic). This aligns
dedup durability with canonical durability at the same instant instead
of two independent, differently-timed commits: a crash before segment
close loses both consistently (today's already-accepted contract,
unchanged); a crash after segment close but before the batched index
update leaves a single, much narrower window per segment (not per
trade), closable further with a startup reconciliation step.

This is a real architectural change — it moves duplicate suppression
from "immediate, per-message" to "deferred until the containing segment
is durable," which means a duplicate arriving within one still-open
segment's window would not yet be reflected in the index and could be
double-counted *within that segment* unless the in-memory
`_seen_trade_ids` (today's exact, unbounded set) is *also* kept as the
authoritative same-segment check, with the persistent index only
handling cross-segment/cross-restart durability. That combination —
today's exact in-memory set for the live segment, batched persistence at
segment-close time for restart survival — is the design this session
recommends as the next concrete step. **Not implemented in this session**:
it touches `ParquetWriter`'s segment-close path and the runner
constructors in a way that goes beyond "wire in a set replacement," and
per this task's own instruction ("if a larger change is truly required
for correctness, explain exactly why before doing it" / "do not perform a
broad architectural rewrite"), implementing it here would be exactly that
broader rewrite.

### Conclusion: PR #57 remains NOT READY

The existing `PersistentTradeIdIndex` component (exact, atomic,
differentially tested against the reference set) is unchanged in its own
correctness. What this session's audit adds is proof that **wiring it
into the current per-trade `_dedupe_trades` call site is unsafe**,
independent of the deployment-benchmark and throughput questions raised
earlier — a structural granularity mismatch with `ParquetWriter`'s own
durability boundary, not a benchmark or configuration problem. Production
wiring remains deferred; the identified next step (segment-granularity
dedup commit, in-memory set as the same-segment authority) is the
smallest correct design found, not yet built.

## 12. Remaining limitations


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

## 13. Git state

- Branch: `p0-4-bounded-trade-dedup`
- Base: `main` @ `790df62` (verified via `git rev-parse origin/main`)
- Final HEAD: see the commit this file ships in
- Changed files: `collector/collector/persistent_trade_id_index.py` (new),
  `collector/tests/test_persistent_trade_id_index.py` (new), this document
  (new). `adapters/base.py` and all other production files: unchanged.
- Working tree: clean after commit.
- PR: to be opened as **draft**, not merged, per this task's explicit
  instruction.
