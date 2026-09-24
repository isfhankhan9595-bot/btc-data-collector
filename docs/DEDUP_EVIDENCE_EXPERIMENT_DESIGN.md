# Trade Deduplication: Observational Evidence Experiment Design

Designed before any code was written, per this phase's own instruction.
**No live capture has occurred.** This session built offline analysis
infrastructure and verified it against synthetic, explicitly-labeled data
only — AWS/live exchange access was not authorized and was not attempted.
This document, and the tool it describes, exist so that *when* real
capture data becomes available (live-authorized or otherwise), there is
already a tested, correct way to analyze it — rather than writing
ad hoc analysis code under time pressure once evidence exists.

## A. What exactly is being observed?

Whether the collector's live/replay path ever produces two
`CanonicalTradeEvent`s that share a dedup identity — and, when it does,
how far apart they arrived and whether a reconnect sits between them. The
unit of observation is the same tuple `_dedupe_trades` already uses:
`(exchange, market_type, instrument_key_or_"", stream, trade_id)`.

## B. What timestamps are recorded?

Every `DedupEvidenceRecord` below carries both, never conflated:

- `exchange_event_ts`: the venue's own timestamp for the trade, descriptive
  only, never used for eligibility or ordering in the analysis's causal
  sense (consistent with every other module in this repository).
- `local_receive_ts`: when the collector's own transport layer received
  the message. This is the field all delay/ordering statistics below are
  computed from.

`local_processing_ts` (when application code got around to handling the
message) and `connection_id`/`reconnect_marker` are optional fields for
distinguishing network delivery timing from application processing delay
(Section 17 of the task) and for reconnect association (Section 15) —
present in the record shape now so a real capture experiment does not need
a schema change later, even though this session has no data to populate
them with.

`local_receive_ts` validity is explicit and evidence-preserving: `None`,
bool, non-int, malformed, and negative values are classified as invalid
timestamp evidence (reported with provenance), not clamped or silently
coerced into delay math.

## C. What constitutes an exact duplicate?

Two records with an identical
`(exchange, market_type, instrument_key, stream, trade_id)` tuple, where
`trade_id` is not `None`. A `trade_id is None` record is **never**
compared against anything, including another `None` record — mirroring
`_dedupe_trades`'s own missing-ID exemption exactly, so this tool's notion
of "duplicate" cannot silently diverge from the production dedup
contract's notion of it.

Current venue semantics are preserved: `trade_id=""` is still an identity
value; only `trade_id is None` is treated as missing-ID.

## D. How is duplicate delay measured?

`duplicate.local_receive_ts - first.local_receive_ts`, where "first" is
the earliest record (by `local_receive_ts`) sharing that identity tuple in
the analyzed set. Every statistic derived from this (min/median/p95/p99/
p99.9/max) is reported labeled **"observed in this sample"** — never
"maximum possible" or "guaranteed" (Section 19 of the task; see also
`ObservedStatistic`'s own docstring below).

When two records share the same `local_receive_ts`, the analyzer does not
invent a meaningful arrival order: duplicate reports carry an explicit
`arrival_order_ambiguous=True` marker.

## E. How is reconnect association measured?

If a `reconnect_marker` is present on the duplicate record and absent (or
different) on the first record, the duplicate is classified
`reconnect_associated=True`. This is **correlation, not proven causality**
— the task explicitly warns against inferring causality merely because
two events occurred near a reconnect, and this tool does not attempt to.

Where available, a transition in `connection_generation` is also treated as
temporal reconnect association (still correlation-only, never causal proof).

## F. How is ID ordering measured?

Separately, per venue/stream, using **local receive order** (never
exchange-event order, and never "ID sorted", since either would beg the
question the analysis exists to answer): for each pair of consecutive
records with numeric-shaped IDs (Binance, OKX), report whether the ID
increased, stayed equal (a duplicate, handled above), or decreased. For
Bybit's UUID-shaped `i`, this analysis is explicitly not attempted —
`OrderingReport.applicable` is `False` for that venue's stream, not a
silently-empty or fabricated "always increasing" result.

## G. How are stream boundaries preserved?

The identity tuple's `stream` component: OKX's `trades` and `trades-all`
are never compared against each other, matching `docs/TRADE_DEDUPLICATION.md`'s
own decision not to merge them without proof.

## H. How is venue identity preserved?

The identity tuple's `exchange`/`market_type`/`instrument_key` components:
Binance Spot and Binance USD-M, sharing a native symbol, are never
compared against each other — same reasoning `instrument.py`'s own
collision-resistance design already establishes elsewhere in this
repository.

## I. How are results stored?

Nothing is persisted by this tool itself. It is a pure function of
whatever `DedupEvidenceRecord` sequence it is given —
`collector/collector/dedup_evidence_analysis.py:analyze_dedup_evidence(records)`
→ `DedupEvidenceReport`. Callers choose their own persistence; this
session did not build a capture pipeline (no live authorization), only
the analysis a future capture pipeline's output could be fed into.

Raw-wire conversion helpers are now included:

- `convert_raw_wire_to_dedup_evidence(raw_rows)` parses raw payloads via
  existing adapter `normalize()` semantics (without calling production trade
  dedup), emits forensic trade records, and classifies malformed evidence.
- `analyze_dedup_evidence_from_raw_wire(raw_rows)` runs conversion and
  duplicate analysis in one step.

Malformed JSON, missing payload, empty payload, truncated payload, and
unsupported/unroutable/non-trade frames remain explicit invalid evidence
records rather than disappearing.

## J. How are observations distinguished from guarantees?

Every numeric result in `DedupEvidenceReport` is wrapped in
`ObservedStatistic`, whose `__repr__`/`describe()` always renders as
"observed in this sample of N records", never as an unqualified number —
making it structurally awkward to accidentally quote a bare "17 seconds"
without the qualifier attached, rather than relying on every future
caller to remember to add the caveat in prose.

Duplicate classes:

- `EXACT_DUPLICATE`: identity match + same compared payload semantics.
- `IDENTITY_PAYLOAD_CONFLICT`: identity match but at least one of
  `canonical_price`, `canonical_quantity`, `canonical_side`, or
  `exchange_event_ts` differs.

For each duplicate, `different_fields` and provenance/hash locators are
reported without embedding full payload bodies in every duplicate object.

Raw payload hash semantics are strict: SHA-256 is computed over the exact
captured payload-string bytes (`utf-8`). Hash equality means byte equality
only; it is not itself semantic equality, and hash inequality alone does
not prove an economic conflict.

## K. Explicit non-claims

This evidence path does **not** claim:

- any protocol guarantee,
- any maximum duplicate-redelivery horizon,
- universal ordering across all delivery paths,
- capture completeness,
- reconnect causality.

This repository-only change does **not** perform live exchange capture and
therefore does not create empirical exchange evidence by itself.

## Status of this session's work

- No live exchange connection was made. AWS untouched.
- The repository contains no real/replayable production trade capture
  data (independently re-confirmed by PR #51's own search, and not
  re-searched again in this session since nothing has changed).
- All tests for the analysis tool below use synthetic, explicitly-labeled
  fixture data (`tests/test_dedup_evidence_analysis.py`) to verify the
  *tool's arithmetic* is correct — this proves the analyzer works, not
  that any exchange behaves a particular way. No test in that file is
  presented as, or should be read as, evidence about real exchange
  behavior.
- The two central open questions (maximum duplicate-redelivery horizon;
  sufficient ordering/uniqueness guarantee for a bounded strategy) remain
  exactly as unresolved as PR #50/#51 left them. This session did not
  attempt to resolve them without evidence, and does not claim to.

## Hostile-audit fixes to this PR's own converter (this session)

The prior commit's `convert_raw_wire_to_dedup_evidence` called
`adapter.normalize()` directly -- the production-wrapped method, which
always applies `_dedupe_trades`. This defeated the converter's stated
purpose: a single raw frame carrying the same trade identity twice (a
real, producible case -- confirmed for OKX's `trades` channel, which
returns one event per array element) would have its second occurrence
silently suppressed before this module ever saw it, exactly the evidence
this tool exists to preserve. Verified empirically (not merely reasoned
from `functools.wraps`'s documentation): `adapter.normalize()` on such a
frame returns 1 event; the fix (`_dedup_bypassed_normalize`, using
`adapter.normalize.__wrapped__` + a manual `_stamp_instrument` call)
returns 2. Fails loudly (`DedupBypassUnavailableError`) rather than
silently falling back to the dedup-active path if a future adapter's
`normalize` is not wrapped the expected way.

Also fixed: `analyze_dedup_evidence` previously skipped any record with
an invalid `local_receive_ts` before checking `trade_id`, via an early
`continue` -- meaning such a record was invisible to both identity-
collision detection AND `missing_id_count` accounting when both problems
applied to the same record. `invalid_timestamp_identity_collisions` (and
its `_examples` counterpart) is now a real, computed, tested field:
an identity that occurs more than once, at least one occurrence with an
invalid timestamp, counted per identity, kept explicitly separate from
`duplicate_count` (which only covers pairs whose timing could actually
be classified with two valid timestamps).

**New finding, documented rather than silently worked around:** this
module reads `row.get("reconnect_marker")` throughout, but
`RawWireRecord`/`RAW_WIRE_SCHEMA` (`collector/collector/raw_capture.py`)
has no `reconnect_marker` field at all -- only `connection_generation`.
On real captured production data, `reconnect_marker` will always be
`None`, so `reconnect_associated` can only ever be established via a
`connection_generation` transition today, never via a marker transition.
This is not a bug in this module (the field is correctly optional and
"not enough evidence" is the correct behavior for a missing marker), but
it does mean the marker-based code path is currently untestable against
real data and only exercised by synthetic fixtures. If reconnect-marker
correlation is wanted from real captures, `RawWireRecord` would need a
new field -- a production schema change, explicitly out of this PR's
forensic-only scope, not made here.

Fabricated-timestamp isolation (`local_receive_ts=0` passed to the
parser when the raw value is invalid) was audited and confirmed safe by
reading every adapter directly: no trade parser reads back
`event.local_receive_ts`, and `event.local_process_ts` is never set by
any trade parser (always `None` regardless of what was passed) -- so the
placeholder cannot leak into any evidence field. The `DedupEvidenceRecord`
itself always uses the row's ORIGINAL value. Proven with a dedicated test
(`test_fabricated_receive_ts_placeholder_never_leaks_into_evidence`)
rather than left as an audit claim only.

10 new tests added (`test_dedup_evidence_analysis.py`, 35 -> 45): same-
frame duplicate preservation (identical payload, conflicting payload,
and distinct-ID cases), a direct production-vs-bypass event-count diff,
the fail-loud-without-`__wrapped__` guard, the fabricated-timestamp
isolation proof, the four invalid-timestamp/identity-collision
combinations Critical Issue #4 called out, and `keep_raw_payload=False`
coverage. Full suite: 1264 passed (1254 baseline + 10 new). `compileall`
clean, `git diff --check` clean. Scope unchanged: exactly
`dedup_evidence_analysis.py`, its test file, and this doc -- no
production dedup, replay, CVD, or orderbook code touched.
