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
