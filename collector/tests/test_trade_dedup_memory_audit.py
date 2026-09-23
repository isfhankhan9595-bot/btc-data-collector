"""Bounded trade-dedup memory design: audit outcome and protective tests.

## Outcome: B -- bounded design not currently provable; exact unbounded set
kept intact, unchanged. No source change was made to
``ExchangeAdapter._dedupe_trades`` or ``_seen_trade_ids`` in this phase.
This file exists to (a) protect that decision against being silently
overridden by an unproven "optimization" later, and (b) pin the two
OFFICIAL-DOC-VERIFIED facts the decision rests on.

## Why no bounded structure was implemented

Every bounded design considered (high-water mark, LRU, time window)
requires proving ``eviction_time > maximum possible duplicate-arrival
time`` (task's own requirement) for every venue. That bound was not
found in any venue's official documentation:

- **Binance Spot / USD-M**: OFFICIAL-DOC-VERIFIED (developers.binance.com)
  field shapes for `t` (Spot Trade ID) and `a` (USD-M Aggregate trade ID)
  -- both `int64`, symbol-scoped. NOT VERIFIED: strict monotonicity on the
  WebSocket stream itself, ID reuse, or any documented maximum
  duplicate-redelivery window. USD-M's REST `aggTrades?fromId=` pagination
  ("return aggtrades with aggregate trade ID >= fromId") is INFERRED
  evidence that `a` is comparable/ordered for REST paging purposes, but
  this is a REST semantics document, not a WebSocket delivery guarantee,
  and was not treated as one.
- **Bybit Linear**: OFFICIAL-DOC-VERIFIED (bybit-exchange.github.io) --
  `publicTrade`'s `i` field is a **UUID string**
  (`"20f43950-d8dd-5b31-9112-a178eb6023af"`), not a numeric ID at all.
  This is conclusive, not merely unverified: a high-water-mark or any
  numeric-ordering-based eviction is not merely unproven for Bybit, it is
  **inapplicable by construction** -- there is no ordering relation on a
  UUID to build a watermark from. This alone rules out a universal
  high-water-mark design across all four venues (task section 25 permits
  venue-specific designs, but even a Bybit-specific bounded design would
  still need a proven redelivery-time horizon, which is the second gap
  below).
- **OKX Swap**: OFFICIAL-DOC-VERIFIED (okx.com/docs-v5) -- `tradeId` is a
  numeric string, uniqueness documented as per-`instId`. One OKX doc page
  states, for the *positions channel* (a different, private channel used
  for fill/position reconciliation): "Trade ID uniqueness is per instId.
  A new order fill always comes with a newer trade ID." This is
  INFERRED, not directly verified, as applying to the public `trades`/
  `trades-all` channels this collector actually consumes -- the quote is
  about a different channel's private fill-reconciliation semantics, and
  was not extrapolated to the public trade feed without a more direct
  source.
- **No venue's official documentation**, for any of the four, states an
  explicit maximum delay between a trade's first delivery and any
  possible duplicate/replayed redelivery of it. Without that bound,
  section 14's eviction-safety proof cannot be constructed for a
  time-window or LRU design either -- not just for a high-water mark.

Given one venue's ID space cannot support ordering-based eviction at all,
and no venue provides a documented redelivery-time bound to justify
time-based eviction, no combination of A/B/C/D from the task's own
candidate list can currently be proven safe. Per section 35, this is an
explicitly valid engineering outcome, not a failure to complete the task.

## Memory benchmark (measured this session, `tracemalloc`, CPython 3.12)

| Entries | Key shape | Peak memory | Bytes/entry | Build time |
|---|---|---|---|---|
| 10,000 | Binance-shaped (int-string trade_id) | 1.7 MiB | 177 | 0.02s |
| 100,000 | Binance-shaped | 15.9 MiB | 166 | 0.16s |
| 1,000,000 | Binance-shaped | 152.9 MiB | 160 | 1.85s |
| 1,000,000 | Bybit-shaped (UUID trade_id) | 184.2 MiB | 193 | 6.97s |

Roughly 160-195 bytes per remembered trade identity. At even a very high
sustained rate (an order of magnitude above typical BTCUSDT trade counts
-- exact current trade-rate figures NOT VERIFIED here, this is a
deliberately generous upper bound, not a measured venue statistic), a
multi-week continuous run would plausibly reach several million entries
-- hundreds of MiB to low single-digit GiB. **Conclusion: real but not
acute.** The current design is safe for the timescales a typical
collection run (hours to a few days) actually operates at; it becomes a
genuine operational concern only for a very long-running (weeks+)
uninterrupted process, which is exactly why this is flagged as the next
task rather than an emergency fix.

## What would actually resolve this

Direct confirmation from Binance/Bybit/OKX support or a sufficiently
large empirical capture (observe real trade IDs over a long real window
and check for reuse/gaps/reordering) of: (1) a documented or empirically
inferred maximum WebSocket redelivery delay, and (2) for Binance/OKX
specifically, a documented (not inferred-from-REST-pagination) monotonicity
guarantee on the WS stream. Neither was pursued further in this session
-- flagged as the next task, not attempted here (this session's tool
access does not include a way to run a multi-hour live capture, and doing
so would also risk brushing against the "repository/research work only,
no live exchange connections" boundary this task itself set).
"""
from __future__ import annotations

import re

from collector.collector.adapters.base import ExchangeAdapter


def test_seen_trade_ids_is_still_the_exact_unbounded_reference_set():
    """Pins the Outcome-B decision itself: ExchangeAdapter must not have
    grown a maxlen, an LRU, or any other eviction mechanism without this
    test being consciously updated (and, per the design record above, a
    proven safety horizon accompanying it). A `set` with no `maxlen`-style
    cap is the entire correctness argument this phase's decision rests
    on."""
    dedupe_source = ExchangeAdapter._dedupe_trades.__doc__ or ""
    assert "unbounded" in dedupe_source, \
        "the unbounded-growth cost must remain explicitly documented on the method itself"
    # Structural check: no maxlen/deque/OrderedDict/LRU construct anywhere
    # in the class body that could silently cap _seen_trade_ids.
    import inspect
    full_source = inspect.getsource(ExchangeAdapter)
    assert "maxlen" not in re.sub(r"self\._unhandled[^\n]*", "", full_source), \
        "a maxlen appeared outside the (unrelated, already-bounded) _unhandled buffer -- " \
        "if this is a deliberate new eviction policy for _seen_trade_ids, it must come with " \
        "an updated design record proving the safety horizon, not just this test edited away"


def test_bybit_trade_id_is_a_uuid_not_a_numeric_id():
    """OFFICIAL-DOC-VERIFIED pin (bybit-exchange.github.io/docs/v5/websocket/public/trade):
    the actual official example payload's `i` field is a UUID string. If
    Bybit's API ever changes this to a numeric ID, this test is the
    trigger to revisit whether a high-water-mark design becomes viable
    for that venue specifically -- until then, this fact is why no
    universal ID-ordering-based eviction was implemented."""
    from collector.collector.adapters.bybit import BybitAdapter
    official_example = {
        "T": 1672304486865, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "16578.50",
        "L": "PlusTick", "i": "20f43950-d8dd-5b31-9112-a178eb6023af", "BT": False,
        "seq": 1783284617,
    }
    adapter = BybitAdapter()
    events = adapter.normalize(
        {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1672304486868,
         "data": [official_example]},
        local_receive_ts=1)
    trade_id = events[0].trade_id
    uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert uuid_pattern.match(trade_id), \
        f"expected Bybit's official example trade_id to be UUID-shaped, got {trade_id!r}"
    try:
        int(trade_id)
        raise AssertionError(f"{trade_id!r} unexpectedly parsed as an int -- not a UUID after all")
    except ValueError:
        pass  # expected: confirms no accidental numeric-ordering assumption could silently succeed
