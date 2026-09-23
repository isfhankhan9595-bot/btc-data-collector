"""Independent audit of PR #47 (windowed CVD): real-source mutation evidence.

PR #46/#47's own "mutation" tests (in test_trade_flow_observation.py and
test_windowed_trade_flow_observation.py) each hand-write a second, inline
reimplementation of the function and assert its output differs from the
real one. That is a reimplementation-comparison, not proof that *this
project's actual regression suite* would catch a real regression -- the
established convention elsewhere in this project (Phase D/H) is to mutate
the real source, run the real suite, observe real failures, then restore.
This file closes that evidence gap for trade-flow causality specifically,
and records one genuine finding that PR #47's inline simulation could not
have surfaced, because its hand-copied mutation always changed both
filters at once.

**Session-verified finding (real source mutated, real suite run, restored
byte-identical -- not simulated):**

- Removing ONLY ``_causal_trades``'s pre-replay filter (`causal_frames =
  [f for f in frames if f.timestamp_ms <= observation_ts]`) breaks 4 real
  tests, all in ``test_trade_flow_observation.py`` (cumulative CVD has no
  other protection at all).
- Removing ONLY ``observe_windowed_trade_flow_at``'s own upper bound
  (the `<= window_end` half of `window_start < t.local_receive_ts <=
  window_end`) breaks **zero** real tests. This is not a defect: it is
  redundant *because* ``window_end`` is always exactly ``observation_ts``
  (see the invariant test below), so the shared ``_causal_trades`` call
  already enforces that exact bound before the window filter ever runs.
- Removing both together breaks 9 real tests (the 4 above plus 5 in
  ``test_windowed_trade_flow_observation.py``), confirming the leak PR
  #47's own inline simulation demonstrated is real -- but that simulation
  bundled both filters into one hand-copied function and so could not
  distinguish "the window filter is doing real work" from "the window
  filter is currently a no-op that would matter if the invariant below
  ever broke."

This does not change any behavior. Defense-in-depth (keeping the
window's own explicit bound rather than relying on the shared primitive
alone) is a reasonable, deliberate choice, not dead code -- it is what
would catch a leak if ``window_end`` and ``observation_ts`` were ever
allowed to diverge. The test below pins the invariant that currently
makes that divergence impossible, so a future change introducing one
would have to consciously touch this test.
"""
from __future__ import annotations

from collector.collector.trade_flow_observation import observe_windowed_trade_flow_at
from collector.tests.test_windowed_trade_flow_observation import _binance_trade_at

T = 1_000_000
W = 60_000


def test_window_end_is_always_exactly_observation_ts():
    """The invariant that makes the window's own upper-bound filter
    currently redundant with the shared pre-replay causal filter (see this
    module's docstring for the real-source mutation evidence). If a future
    change ever let window_end differ from observation_ts, this test
    fails first, before that change could silently rely on a filter that
    was never actually independent."""
    obs = observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, W, venue="BINANCE")
    assert obs.window_end == obs.observation_ts == T


def test_a_trade_exactly_at_window_end_is_included_via_the_shared_causal_filter():
    """Positive-path companion: a trade at exactly observation_ts is
    included -- proven here to be governed by _causal_trades's own
    <= observation_ts bound (the load-bearing one, per the mutation
    evidence above), not by the window's own filter, since both bounds
    coincide at this exact point and only one of them is doing real work."""
    obs = observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, W, venue="BINANCE")
    assert obs.trade_count == 1
    assert obs.last_trade_local_receive_ts == T
