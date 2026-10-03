"""P0-11: timestamp resolution and clock-domain contracts.

Scope: preserve real local temporal information (ns receive stamp + monotonic
stamp at the receive boundary), never invent precision (exchange ms stays
ms), never mix clock domains, keep replay on the RECORDED stamps.

Not covered here by design (unchanged, still millisecond): canonical events,
causal alignment, dataset assembly, REST response stamps. See
docs/P0_11_TIMESTAMP_RESOLUTION.md.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import math
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from collector.collector import clock
from collector.collector import websocket_client as wsc
from collector.collector.clock import (
    EXCHANGE_TS_PRECISION,
    LOCAL_RECEIVE_PRECISION_MS,
    LOCAL_RECEIVE_PRECISION_NS,
    ReceiveStamp,
    capture_receive_stamp,
    effective_ns,
    ms_from_ns,
    ns_from_ms,
    require_epoch_ns,
)
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
from collector.collector.replay import FrameKind, ReplayFrame, ReplaySource, _optional_ns
from collector.collector.websocket_client import WebSocketClient

MS = 1_750_000_000_123                 # a millisecond
NS_A = MS * 1_000_000 + 100_000        # +100 us inside the same ms
NS_B = MS * 1_000_000 + 800_000        # +800 us inside the same ms


# ---------------------------------------------------------------- clock ----

@pytest.mark.parametrize("bad", [True, False, 1.5, 1.0, "1", None, -1])
def test_require_epoch_ns_rejects_invalid(bad):
    with pytest.raises(ValueError):
        require_epoch_ns(bad)


def test_require_epoch_ns_accepts_zero_and_huge_without_overflow():
    assert require_epoch_ns(0) == 0
    huge = 2**62  # > any real epoch ns, still exact in int64
    assert require_epoch_ns(huge) == huge


def test_ns_from_ms_is_exact_and_adds_no_precision():
    assert ns_from_ms(MS) == MS * 1_000_000
    assert ns_from_ms(MS) % 1_000_000 == 0  # nothing finer than ms exists in it
    assert EXCHANGE_TS_PRECISION == "ms"


@pytest.mark.parametrize("bad", [True, 1.5, "1", None, -5])
def test_ns_from_ms_rejects_invalid(bad):
    with pytest.raises(ValueError):
        ns_from_ms(bad)


def test_ms_from_ns_floors_and_is_consistent_with_stamp():
    assert ms_from_ns(NS_A) == MS
    assert ms_from_ns(NS_B) == MS
    stamp = ReceiveStamp(wall_ns=NS_B, mono_ns=5)
    assert stamp.wall_ms == MS


def test_clock_domains_are_kept_separate():
    stamp = capture_receive_stamp(wall_ns_fn=lambda: NS_A, mono_ns_fn=lambda: 42)
    assert stamp.wall_ns == NS_A and stamp.mono_ns == 42
    assert stamp.wall_ns != stamp.mono_ns


def test_receive_stamp_wall_must_be_a_valid_epoch_ns():
    with pytest.raises(ValueError):
        ReceiveStamp(wall_ns=-1, mono_ns=1)
    with pytest.raises(ValueError):
        ReceiveStamp(wall_ns=True, mono_ns=1)
    with pytest.raises(ValueError):
        ReceiveStamp(wall_ns=1.5, mono_ns=1)


def test_ties_stay_ties_no_synthetic_offset():
    assert effective_ns(MS, NS_A) == effective_ns(MS, NS_A)
    assert effective_ns(MS, None) == MS * 1_000_000  # legacy: start of its ms


def test_no_wall_monotonic_offset_helper_exists():
    """wall - monotonic is not stable; nothing may pretend otherwise."""
    public = set(clock.__all__)
    assert not any("offset" in n or "to_epoch" in n for n in public)


# ---------------------------------------------- receive boundary (client) ---

class _FakeSocket:
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for f in self.frames:
                yield f
        return gen()


def _drive(client, frames, *, between=None):
    async def run():
        await client._consume(_FakeSocket(frames))
        if between:
            between()
        client.running = False
        await client._process_queue()
    client.running = True
    asyncio.run(run())


def _stamps(monkeypatch, walls, monos=None):
    walls = list(walls)
    monos = list(monos) if monos is not None else list(range(1000, 1000 + len(walls)))
    seq = iter(zip(walls, monos))

    def fake(*_a, **_k):
        w, m = next(seq)
        return ReceiveStamp(wall_ns=w, mono_ns=m)
    monkeypatch.setattr(wsc, "capture_receive_stamp", fake)


def _client(hook, on_message=None):
    async def default(data, ts, connection_id=None):
        return None
    return WebSocketClient(url="wss://x", on_message=on_message or default, on_raw_frame=hook)


def test_two_frames_in_same_ms_keep_distinct_local_receive_ns(monkeypatch):
    _stamps(monkeypatch, [NS_A, NS_B])
    seen = []

    def hook(frame, *, local_receive_ts, local_receive_ns=None, receive_mono_ns=None, **kw):
        seen.append((local_receive_ts, local_receive_ns, receive_mono_ns))

    _drive(_client(hook), ['{"a":1}', '{"a":2}'])
    assert [s[0] for s in seen] == [MS, MS]          # legacy ms collapses them
    assert [s[1] for s in seen] == [NS_A, NS_B]      # ns does not
    assert seen[0][1] != seen[1][1]
    assert seen[1][2] > seen[0][2]                    # monotonic advanced


def test_receive_stamp_is_taken_before_queueing_and_survives_queue_delay(monkeypatch):
    """A slow consumer must not move the receive time."""
    _stamps(monkeypatch, [NS_A, NS_B])
    captured = []
    processed = []

    def hook(frame, *, local_receive_ts, local_receive_ns=None, receive_mono_ns=None, **kw):
        captured.append(local_receive_ns)

    async def on_message(data, ts, connection_id=None):
        # Runs only in _process_queue, after the (fake) clock is exhausted:
        # any re-stamp here would raise StopIteration from next(seq).
        processed.append(ts)

    _drive(_client(hook, on_message), ['{"a":1}', '{"a":2}'])
    assert captured == [NS_A, NS_B]
    assert processed == [MS, MS]


def test_hook_without_ns_parameters_still_works(monkeypatch):
    _stamps(monkeypatch, [NS_A])
    got = []

    def legacy_hook(frame, *, local_receive_ts, connection_id=None, connection_generation=None,
                    decode_ok=True, decode_error=None, parsed=None, control_frame=False):
        got.append(local_receive_ts)

    _drive(_client(legacy_hook), ['{"a":1}'])
    assert got == [MS]


@pytest.mark.parametrize("modname", [
    "collector.run_collector", "collector.run_bybit_collector",
    "collector.run_okx_collector", "collector.run_binance_spot_collector",
])
def test_every_production_hook_accepts_the_ns_stamps(modname):
    mod = __import__(modname, fromlist=["x"])
    cls = next(v for n, v in vars(mod).items()
               if inspect.isclass(v) and hasattr(v, "_capture_raw_frame"))
    params = inspect.signature(cls._capture_raw_frame).parameters
    assert "local_receive_ns" in params and "receive_mono_ns" in params


def test_real_binance_hook_persists_ns_end_to_end(monkeypatch):
    from collector.tests.test_no_silent_discard import _app
    app = _app()
    captured = []
    app.raw_capture = MagicMock()
    app.raw_capture.capture_wire.side_effect = captured.append
    _stamps(monkeypatch, [NS_A, NS_B])

    async def on_message(data, ts, connection_id=None):
        return None

    client = WebSocketClient(url="wss://x", on_message=on_message,
                             on_raw_frame=app._capture_raw_frame)
    _drive(client, ['{"stream":"btcusdt@depth","data":{"u":1}}', '{"stream":"btcusdt@depth","data":{"u":2}}'])
    assert [r.local_receive_ns for r in captured] == [NS_A, NS_B]
    assert all(r.receive_mono_ns is not None for r in captured)
    # monotonic never leaks into the epoch column
    assert all(r.local_receive_ns != r.receive_mono_ns for r in captured)


# ------------------------------------------------------- raw wire record ---

def _rec(**kw):
    base = dict(local_receive_ts=MS, payload="{}", venue="BINANCE")
    base.update(kw)
    return RawWireRecord(**base)


def test_row_keys_match_schema_and_version_is_1_1():
    row = _rec(local_receive_ns=NS_A, receive_mono_ns=7).to_row()
    assert set(row) == set(RAW_WIRE_SCHEMA.names)
    assert RAW_WIRE_SCHEMA.metadata[b"schema_version"] == b"1.1"
    for col in ("local_receive_ns", "receive_mono_ns"):
        assert RAW_WIRE_SCHEMA.field(col).type == pa.int64()


def test_receive_ns_is_not_replaced_by_capture_or_exchange_time():
    row = _rec(local_receive_ns=NS_A, local_capture_ts=MS + 5_000,
               exchange_event_ts=MS - 9_000).to_row()
    assert row["local_receive_ns"] == NS_A
    assert row["local_receive_ns"] != row["local_capture_ts"] * 1_000_000
    assert row["local_receive_ns"] != row["exchange_event_ts"] * 1_000_000


def test_precision_provenance_recorded():
    row = _rec(local_receive_ns=NS_A, exchange_event_ts=MS).to_row()
    assert row["local_receive_precision"] == LOCAL_RECEIVE_PRECISION_NS
    assert row["exchange_event_ts_precision"] == "ms"


def test_legacy_row_stays_visibly_ms_precision_and_never_zero():
    row = _rec().to_row()
    assert row["local_receive_ns"] is None
    assert row["receive_mono_ns"] is None
    assert row["local_receive_precision"] == LOCAL_RECEIVE_PRECISION_MS
    assert row["exchange_event_ts_precision"] is None  # no exchange ts, nothing to qualify


def test_ms_and_ns_disagreement_is_refused_not_reconciled():
    with pytest.raises(ValueError):
        _rec(local_receive_ts=MS + 1, local_receive_ns=NS_A).to_row()


@pytest.mark.parametrize("bad", [True, 1.5, -1, "1"])
def test_invalid_ns_is_rejected(bad):
    with pytest.raises(ValueError):
        _rec(local_receive_ns=bad).to_row()


def test_capture_failure_on_bad_ns_is_fail_open_and_observable():
    events = []
    cap = RawCapture(MagicMock(), quality_event_sink=events.append)
    assert cap.capture_wire(_rec(local_receive_ts=MS + 1, local_receive_ns=NS_A)) is False
    assert cap.stats()["capture_failures"] == 1 and events


# ---------------------------------------------- storage round trip / replay ---

def _write_wire(tmp_path, records):
    from collector.collector.parquet_writer import ParquetWriter
    w = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path))
    cap = RawCapture(w, None)
    for r in records:
        assert cap.capture_wire(r) is True
    w.close()


def test_ns_round_trips_exactly_through_parquet_and_replay_even_with_null_rows(tmp_path):
    """A NULL in the column makes pandas hand back float64, which cannot hold
    ~1.75e18 exactly. Replay must still return the exact ints."""
    _write_wire(tmp_path, [
        _rec(local_receive_ts=MS, local_receive_ns=NS_A, receive_mono_ns=11, payload="a"),
        _rec(local_receive_ts=MS, local_receive_ns=NS_B, receive_mono_ns=12, payload="b"),
        _rec(local_receive_ts=MS + 5, payload="legacy"),  # no ns -> NULL
    ])
    assert float(NS_A) != NS_A  # precondition: the float hazard is real here
    source = ReplaySource.from_directory(str(tmp_path))
    by_payload = {f.payload: f for f in source.frames}
    assert by_payload["a"].receive_ns == NS_A
    assert by_payload["b"].receive_ns == NS_B
    assert by_payload["legacy"].receive_ns is None
    assert by_payload["a"].receive_mono_ns == 11
    assert [f.payload for f in source.frames] == ["a", "b", "legacy"]


def test_replay_never_substitutes_wall_clock_or_read_time(tmp_path, monkeypatch):
    _write_wire(tmp_path, [_rec(local_receive_ts=MS, local_receive_ns=NS_A, payload="a")])
    import time as _t
    monkeypatch.setattr(_t, "time_ns", lambda: (_ for _ in ()).throw(AssertionError("wall clock read")))
    monkeypatch.setattr(_t, "time", lambda: (_ for _ in ()).throw(AssertionError("wall clock read")))
    frames = ReplaySource.from_directory(str(tmp_path)).frames
    assert frames[0].receive_ns == NS_A


def test_replay_orders_by_recorded_ns_not_by_row_order():
    rows = [
        {"local_receive_ts": MS, "local_receive_ns": NS_B, "payload": "late", "decode_ok": True},
        {"local_receive_ts": MS, "local_receive_ns": NS_A, "payload": "early", "decode_ok": True},
    ]
    frames = ReplaySource.from_records(wire_rows=rows).frames
    assert [f.payload for f in frames] == ["early", "late"]


def test_equal_ns_stay_tied_and_no_offset_is_added():
    """order_key is (timestamp_ms, kind_rank, ns_tiebreak, source_index) as
    of the P0-11 follow-up replay-ordering correction; the ns-tied portion
    is index 2, not 0 (see that fix's own test file for the full rationale)."""
    rows = [{"local_receive_ts": MS, "local_receive_ns": NS_A, "payload": p, "decode_ok": True}
            for p in ("x", "y")]
    f = ReplaySource.from_records(wire_rows=rows).frames
    assert f[0].order_key[:3] == f[1].order_key[:3]          # ms, kind, ns all tied exactly
    assert f[0].order_key[2] == NS_A
    assert [x.payload for x in f] == ["x", "y"]              # recorded-order secondary


def test_legacy_ms_rows_and_ns_rows_interleave_consistently():
    rows = [
        {"local_receive_ts": MS + 1, "payload": "legacy_next_ms", "decode_ok": True},
        {"local_receive_ts": MS, "local_receive_ns": NS_B, "payload": "ns_in_ms", "decode_ok": True},
        {"local_receive_ts": MS, "payload": "legacy_this_ms", "decode_ok": True},
    ]
    order = [f.payload for f in ReplaySource.from_records(wire_rows=rows).frames]
    assert order == ["legacy_this_ms", "ns_in_ms", "legacy_next_ms"]


def test_replay_refuses_ms_ns_disagreement():
    with pytest.raises(ValueError):
        ReplaySource.from_records(wire_rows=[
            {"local_receive_ts": MS + 3, "local_receive_ns": NS_A, "payload": "x"}])


@pytest.mark.parametrize("bad", [True, 1.5e18, "1", -1])
def test_replay_rejects_inexact_or_invalid_ns(bad):
    with pytest.raises(ValueError):
        _optional_ns(bad, "local_receive_ns")


def test_missing_ns_stays_missing():
    assert _optional_ns(None, "n") is None
    assert _optional_ns(float("nan"), "n") is None
    assert ReplaySource.from_records(wire_rows=[
        {"local_receive_ts": MS, "payload": "p"}]).frames[0].receive_ns is None


def test_mono_ns_is_not_range_checked_as_epoch():
    assert _optional_ns(5, "receive_mono_ns", epoch=False) == 5


def test_mono_never_used_for_ordering():
    rows = [
        {"local_receive_ts": MS, "local_receive_ns": NS_A, "receive_mono_ns": 999, "payload": "a"},
        {"local_receive_ts": MS, "local_receive_ns": NS_B, "receive_mono_ns": 1, "payload": "b"},
    ]
    assert [f.payload for f in ReplaySource.from_records(wire_rows=rows).frames] == ["a", "b"]


# ---------------------------------------- scope statements (not assumptions) ---

def test_compaction_does_not_process_raw_wire():
    from collector.scripts import compact_daily
    streams = getattr(compact_daily, "STREAM_SCHEMAS", {})
    assert "raw_wire" not in streams  # compaction never rewrites the ns column


def test_exchange_ms_timestamp_is_not_upgraded_to_measured_ns():
    row = _rec(local_receive_ns=NS_A, exchange_event_ts=MS).to_row()
    assert row["exchange_event_ts"] == MS                 # untouched, still ms
    assert row["exchange_event_ts_precision"] == "ms"     # provenance says so
