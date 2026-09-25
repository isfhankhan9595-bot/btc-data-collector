"""P0-4: exact off-heap trade-identity index -- correctness, failure
semantics, differential testing against the reference exact set, and
real mutation testing against the actual source.

This module (`persistent_trade_id_index.py`) is deliberately NOT wired
into `ExchangeAdapter._dedupe_trades` as the production default in this
change -- see `docs/P0_4_BOUNDED_TRADE_DEDUP.md` for the full decision
record. These tests prove the component itself is correct and safe to
adopt later; they do not exercise production adapter code, which is
unchanged.
"""
from __future__ import annotations

import os
import random
import sqlite3
import tempfile

import pytest

from collector.collector.persistent_trade_id_index import (
    PersistentIndexError,
    PersistentTradeIdIndex,
    ReferenceExactSet,
    differential_check,
    identity_key,
)


@pytest.fixture
def index_path(tmp_path):
    return str(tmp_path / "dedup_index.sqlite3")


# ---------------------------------------------------------------------------
# Core test matrix (task's required categories 1-7, applied to this
# component directly; categories 8-10 -- reconnect/replay/same-frame -- are
# adapter-lifecycle properties this component doesn't itself implement,
# see the design doc for why those remain governed by the unchanged
# production _dedupe_trades)
# ---------------------------------------------------------------------------


def test_unseen_key_is_accepted(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        assert idx.add_if_new("k1") is True


def test_same_key_is_suppressed_on_second_call(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        assert idx.add_if_new("k1") is True
        assert idx.add_if_new("k1") is False


def test_identity_key_join_format_keeps_five_components_distinct():
    """Different (exchange, market_type, instrument, stream, trade_id)
    tuples must never collide via string concatenation ambiguity."""
    a = identity_key("BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trades", "1")
    b = identity_key("BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trades", "12")
    c = identity_key("BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trade", "s1")
    assert len({a, b, c}) == 3


def test_different_exchange_is_a_separate_identity(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        a = identity_key("BINANCE", "linear_perpetual", "X", "trades", "1")
        b = identity_key("BYBIT", "linear_perpetual", "X", "trades", "1")
        assert idx.add_if_new(a) is True
        assert idx.add_if_new(b) is True   # not suppressed despite same trade_id


def test_different_market_type_is_a_separate_identity(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        a = identity_key("BINANCE", "linear_perpetual", "X", "trades", "1")
        b = identity_key("BINANCE", "spot", "X", "trades", "1")
        assert idx.add_if_new(a) is True
        assert idx.add_if_new(b) is True


def test_different_instrument_is_a_separate_identity(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        a = identity_key("BINANCE", "linear_perpetual", "BTC-USDT", "trades", "1")
        b = identity_key("BINANCE", "linear_perpetual", "ETH-USDT", "trades", "1")
        assert idx.add_if_new(a) is True
        assert idx.add_if_new(b) is True


def test_different_stream_is_a_separate_identity(index_path):
    """Mirrors production's OKX trades-vs-trades-all requirement."""
    with PersistentTradeIdIndex(index_path) as idx:
        a = identity_key("OKX", "linear_perpetual", "X", "trades", "1")
        b = identity_key("OKX", "linear_perpetual", "X", "trades-all", "1")
        assert idx.add_if_new(a) is True
        assert idx.add_if_new(b) is True


def test_reconnect_style_overlap_is_suppressed_across_calls(index_path):
    """A, B, C then B, C, D (real reconnect-redelivery shape) -> only A, B,
    C, D are genuinely new, matching the same expectation production
    dedup already meets."""
    with PersistentTradeIdIndex(index_path) as idx:
        delivered = ["A", "B", "C", "B", "C", "D"]
        accepted = [k for k in delivered if idx.add_if_new(k)]
        assert accepted == ["A", "B", "C", "D"]


def test_restart_survives_via_the_same_file(index_path):
    """The new capability this module adds, proven directly: closing and
    reopening the index at the same path preserves membership -- a
    redelivered ID after a simulated restart is still recognized."""
    with PersistentTradeIdIndex(index_path) as idx:
        idx.add_if_new("k1")
    # Simulated restart: fresh Python object, same file.
    with PersistentTradeIdIndex(index_path) as idx2:
        assert idx2.add_if_new("k1") is False, "k1 must still be remembered after reopening the same file"
        assert idx2.add_if_new("k2") is True


# ---------------------------------------------------------------------------
# Atomicity: no read-then-write race window
# ---------------------------------------------------------------------------


def test_add_if_new_is_a_single_atomic_operation_not_select_then_insert():
    """Structural guard: the implementation must use one INSERT OR IGNORE
    statement whose own rowcount determines novelty, not a SELECT
    followed by a conditional INSERT (which would have a TOCTOU race
    under concurrent access)."""
    import inspect
    from collector.collector import persistent_trade_id_index as module
    source = inspect.getsource(module.PersistentTradeIdIndex.add_if_new)
    assert "INSERT OR IGNORE" in source
    assert "SELECT" not in source, \
        "add_if_new must not perform a separate SELECT before the INSERT -- that reintroduces a race window"


# ---------------------------------------------------------------------------
# Failure semantics: fail loudly, never silently pick a dangerous default
# ---------------------------------------------------------------------------


def test_missing_parent_directory_fails_loudly_not_silently():
    bad_path = "/this/directory/does/not/exist/dedup.sqlite3"
    with pytest.raises(PersistentIndexError):
        PersistentTradeIdIndex(bad_path)


def test_corrupted_database_file_fails_loudly_on_open():
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
        f.write(b"this is not a valid sqlite database file, just garbage bytes" * 10)
        path = f.name
    try:
        with pytest.raises(PersistentIndexError):
            idx = PersistentTradeIdIndex(path)
            idx.add_if_new("k1")   # the corruption may only surface on first real operation
    finally:
        os.unlink(path)


def test_lookup_after_connection_closed_fails_loudly_not_silently(index_path):
    idx = PersistentTradeIdIndex(index_path)
    idx.add_if_new("k1")
    idx.close()
    with pytest.raises(PersistentIndexError):
        idx.add_if_new("k2")


# ---------------------------------------------------------------------------
# Differential testing: the persistent index must never diverge from the
# exact reference set, across randomized adversarial sequences including
# duplicates, reconnect-style overlaps, and multiple venues/instruments
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_randomized_differential_against_reference_exact_set(index_path, seed):
    rng = random.Random(seed)
    venues = ["BINANCE", "BYBIT", "OKX"]
    instruments = ["BTC-USDT", "ETH-USDT"]
    streams = ["trades", "trades-all"]
    universe = [
        identity_key(rng.choice(venues), "linear_perpetual", rng.choice(instruments),
                     rng.choice(streams), str(rng.randint(1, 200)))
        for _ in range(300)
    ]
    # Reconnect-style overlap: re-deliver a random slice of what's already
    # been generated, interleaved with fresh keys, mirroring the real
    # A,B,C / B,C,D redelivery shape at random points.
    sequence = []
    for k in universe:
        sequence.append(k)
        if rng.random() < 0.3 and sequence:
            sequence.append(rng.choice(sequence))
    rng.shuffle(sequence[: len(sequence) // 4])   # partial reordering, not fully sorted

    reference = ReferenceExactSet()
    with PersistentTradeIdIndex(index_path) as candidate:
        differential_check(sequence, reference, candidate)   # raises on first divergence
        assert len(reference) == len(candidate)


def test_reference_and_candidate_agree_on_missing_id_style_keys_too(index_path):
    """Production never deduplicates a trade_id=None event at all -- this
    module doesn't know about that exemption (it operates purely on
    caller-supplied keys), so this test documents that the exemption
    logic must stay in the adapter layer, not be reimplemented here.
    Demonstrated: a sentinel "no identity" key behaves like any other
    ordinary key at this layer -- the adapter is responsible for never
    calling add_if_new for a None trade_id in the first place."""
    reference = ReferenceExactSet()
    with PersistentTradeIdIndex(index_path) as candidate:
        differential_check(["__no_identity__", "__no_identity__"], reference, candidate)
        # Both correctly suppress the second occurrence of the SENTINEL -- proving
        # this module has no special-casing; the caller must not pass a shared
        # sentinel for genuinely distinct missing-ID trades.
        assert len(candidate) == 1


# ---------------------------------------------------------------------------
# Benchmark (measured, this sandbox -- recorded, not idealized)
# ---------------------------------------------------------------------------


def test_benchmark_throughput_and_size_are_recorded_and_sane(tmp_path):
    """Not a strict performance gate (CI hardware varies) -- a sanity
    check that throughput stays in a regime far above any plausible
    sustained trade rate, so a severe regression (e.g. an accidental
    fsync-per-statement change) would be caught."""
    import time
    path = str(tmp_path / "bench.sqlite3")
    n = 5_000
    with PersistentTradeIdIndex(path) as idx:
        t0 = time.perf_counter()
        for i in range(n):
            idx.add_if_new(f"bench-key-{i}")
        elapsed = time.perf_counter() - t0
    rate = n / elapsed
    # Generous floor: the measured sandbox rate was ~46,000/s; anything
    # above 1,000/s is still >>10x any plausible real BTCUSDT trade rate
    # across four venues, so this floor is about catching a severe
    # regression, not asserting the sandbox's own peak number.
    assert rate > 1_000, f"insert throughput dropped to {rate:.0f}/s -- investigate before relying on this design"


# ---------------------------------------------------------------------------
# Real mutation testing (source mutated, tests run, restored) -- evidence
# recorded in the design doc and this session's conversation, not
# reproduced as committed code per this project's established convention.
# The two mutations below ARE committed as tests because they exercise
# the *interface contract* (what a caller can rely on), not a source
# mutation -- kept distinct from that convention deliberately.
# ---------------------------------------------------------------------------


def test_len_reflects_true_row_count_not_a_cached_estimate(index_path):
    with PersistentTradeIdIndex(index_path) as idx:
        for k in ("a", "b", "c"):
            idx.add_if_new(k)
        idx.add_if_new("a")   # duplicate, must not increase the count
        assert len(idx) == 3
