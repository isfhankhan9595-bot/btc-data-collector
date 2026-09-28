"""P0-10: receive time is the one causal clock; local_capture_ts is not.

Exercises the real WebSocketClient receive/queue/worker path, the real
RawWireRecord row builder, and the real ReplaySource -- not copies of their
logic.
"""
from __future__ import annotations

import ast
import asyncio
import json
import pathlib
from unittest import mock

import pytest

from collector.collector import raw_capture, replay
from collector.collector.raw_capture import (
    CAUSAL_RAW_WIRE_COLUMN,
    NON_CAUSAL_RAW_WIRE_COLUMNS,
    RAW_WIRE_SCHEMA,
    RawWireRecord,
)
from collector.collector.websocket_client import WebSocketClient

PKG = pathlib.Path(__file__).resolve().parents[1]


class _Socket:
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for f in self.frames:
                yield f
        return gen()


def _clock(values):
    it = iter(values)
    return lambda: next(it)


# --- queue delay must not alter receive time --------------------------------


@pytest.mark.asyncio
async def test_queue_delay_does_not_change_receive_time():
    """Frame arrives at t=1000 ms; the worker only handles it at t=5000 ms.
    receive stays 1000, processing is 5000, and they stay distinct."""
    seen = {}

    async def on_message(data, local_receive_ts, connection_id=None):
        seen["receive"] = local_receive_ts
        seen["processing"] = int(replay_time.time() * 1000)

    import time as replay_time
    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10)
    client.running = True
    with mock.patch("collector.collector.websocket_client.time.time", return_value=1.0):
        await client._consume(_Socket(['{"n":1}']))          # arrives at 1000 ms
    client.running = False
    with mock.patch.object(replay_time, "time", return_value=5.0):  # worker runs at 5000 ms
        await client._process_queue()
    assert seen["receive"] == 1000
    assert seen["processing"] == 5000
    assert seen["receive"] != seen["processing"]


@pytest.mark.asyncio
async def test_receive_time_is_stamped_before_capture_and_before_enqueue():
    order = []

    def on_raw(msg, **kw):
        order.append(("capture", kw["local_receive_ts"]))

    async def on_message(data, ts, connection_id=None):
        order.append(("process", ts))

    client = WebSocketClient("ws://x", on_message, on_raw_frame=on_raw, ingest_queue_maxsize=10)
    client.running = True
    with mock.patch("collector.collector.websocket_client.time.time", return_value=2.0):
        await client._consume(_Socket(['{"n":1}']))
    client.running = False
    await client._process_queue()
    # Same immutable receive stamp on both sides of the queue.
    assert order == [("capture", 2000), ("process", 2000)]


@pytest.mark.asyncio
async def test_rapid_frames_and_same_millisecond_keep_arrival_order():
    got = []

    async def on_message(data, ts, connection_id=None):
        got.append((data["n"], ts))

    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10)
    client.running = True
    with mock.patch("collector.collector.websocket_client.time.time", return_value=3.0):
        await client._consume(_Socket([f'{{"n":{i}}}' for i in range(4)]))
    client.running = False
    await client._process_queue()
    assert got == [(0, 3000), (1, 3000), (2, 3000), (3, 3000)]


@pytest.mark.asyncio
async def test_reconnect_generation_keeps_its_own_receive_time():
    seen = []

    def on_raw(msg, **kw):
        seen.append((kw["connection_generation"], kw["local_receive_ts"]))

    client = WebSocketClient("ws://x", lambda *a, **k: None, on_raw_frame=on_raw, ingest_queue_maxsize=10)
    client.running = True
    client._connection_serial = 1
    with mock.patch("collector.collector.websocket_client.time.time", return_value=1.0):
        await client._consume(_Socket(['{"a":1}']))
    client._connection_serial = 2
    with mock.patch("collector.collector.websocket_client.time.time", return_value=9.0):
        await client._consume(_Socket(['{"a":2}']))
    assert seen == [(1, 1000), (2, 9000)]


# --- exchange time never becomes receive time --------------------------------


@pytest.mark.asyncio
async def test_exchange_timestamp_earlier_or_later_never_replaces_receive():
    got = []

    def on_raw(msg, **kw):
        got.append(kw["local_receive_ts"])

    client = WebSocketClient("ws://x", lambda *a, **k: None, on_raw_frame=on_raw, ingest_queue_maxsize=10)
    client.running = True
    frames = ['{"E":1}', '{"E":99999999999999}']     # far past / far future exchange ts
    with mock.patch("collector.collector.websocket_client.time.time", return_value=7.0):
        await client._consume(_Socket(frames))
    assert got == [7000, 7000]


# --- raw capture row ---------------------------------------------------------


def test_raw_row_timestamp_is_receive_time_not_capture_time():
    rec = RawWireRecord(local_receive_ts=1000, payload="{}", venue="BINANCE",
                        local_capture_ts=9_999_999)
    row = rec.to_row()
    assert row["timestamp"] == 1000
    assert row["local_receive_ts"] == 1000
    assert row["local_capture_ts"] == 9_999_999      # recorded, never promoted


def test_missing_capture_ts_fallback_never_touches_receive_time():
    with mock.patch("collector.collector.raw_capture.time.time", return_value=42.0):
        row = RawWireRecord(local_receive_ts=1000, payload="{}", venue="BINANCE").to_row()
    assert row["local_receive_ts"] == 1000 and row["timestamp"] == 1000
    assert row["local_capture_ts"] == 42_000


def test_local_receive_ts_is_required_no_silent_fabrication():
    with pytest.raises(TypeError):
        RawWireRecord(payload="{}", venue="BINANCE")          # type: ignore[call-arg]


def test_causal_and_noncausal_column_contract_matches_schema():
    names = set(RAW_WIRE_SCHEMA.names)
    assert CAUSAL_RAW_WIRE_COLUMN in names
    assert NON_CAUSAL_RAW_WIRE_COLUMNS <= names
    assert CAUSAL_RAW_WIRE_COLUMN not in NON_CAUSAL_RAW_WIRE_COLUMNS


# --- replay ------------------------------------------------------------------


def test_replay_uses_historical_receive_time_not_capture_or_wall_clock():
    rows = [
        {"local_receive_ts": 1000, "timestamp": 1000, "local_capture_ts": 8_000_000,
         "payload": "{}", "decode_ok": True},
        {"local_receive_ts": 500, "timestamp": 500, "local_capture_ts": 9_000_000,
         "payload": "{}", "decode_ok": True},
    ]
    with mock.patch("time.time", return_value=1_700_000_000.0):    # replay wall clock
        src = replay.ReplaySource.from_records(wire_rows=rows)
    assert [f.timestamp_ms for f in src.frames] == [500, 1000]


def test_replay_ignores_capture_ts_even_when_receive_ts_missing_uses_timestamp_only():
    row = {"timestamp": 1234, "local_capture_ts": 9_999_999, "payload": "{}"}
    frame = replay.ReplaySource.from_records(wire_rows=[row]).frames[0]
    assert frame.timestamp_ms == 1234


# --- structural: nothing causal may read local_capture_ts --------------------


def _py_files():
    for sub in ("collector", "pipeline", "scripts"):
        yield from (PKG / sub).rglob("*.py")


def test_no_module_reads_local_capture_ts_outside_the_raw_row_builder():
    """local_capture_ts may be *written* (raw_capture, run_collector) but no
    other module may reference it, so no causal path can depend on it."""
    allowed = {"raw_capture.py"}
    offenders = []
    for path in _py_files():
        if path.name in allowed:
            continue
        if "local_capture_ts" in path.read_text():
            offenders.append(path.name)
    # run_collector.py sits at PKG root and only *writes* it (checked below).
    assert offenders == [], offenders


def test_run_collector_only_writes_local_capture_ts():
    tree = ast.parse((PKG / "run_collector.py").read_text())
    uses = [n for n in ast.walk(tree)
            if isinstance(n, ast.keyword) and n.arg == "local_capture_ts"]
    loads = [n for n in ast.walk(tree)
             if isinstance(n, ast.Attribute) and n.attr == "local_capture_ts"]
    assert uses and not loads      # passed as a constructor kwarg, never read back


def test_dataset_assembler_and_alignment_do_not_reference_capture_ts():
    for name in ("dataset_assembler.py", "cross_exchange_alignment.py"):
        assert "local_capture_ts" not in (PKG / "pipeline" / name).read_text()


@pytest.mark.asyncio
async def test_receive_time_survives_queue_delay_for_callbacks_without_connection_id():
    """Second on_message dispatch branch (callback takes no connection_id):
    the immutable receive stamp must survive queue delay there too."""
    import time as _t
    seen = {}

    async def on_message(data, local_receive_ts):          # no connection_id kwarg
        seen["receive"] = local_receive_ts

    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10)
    client.running = True
    with mock.patch("collector.collector.websocket_client.time.time", return_value=1.0):
        await client._consume(_Socket(['{"n":1}']))
    client.running = False
    with mock.patch.object(_t, "time", return_value=5.0):
        await client._process_queue()
    assert seen["receive"] == 1000
