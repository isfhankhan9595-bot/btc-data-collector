"""Multi-venue storage namespace regression tests.

Binance, Bybit and OKX run as separate OS processes writing into one data
directory. Segment sequence numbers, ``.tmp`` files and orphan recovery are all
scoped to a *stream directory*, so two venues (or two processes) sharing one
share all three. PR #13's OKX capture reused Binance's ``raw_wire`` and
``quality_events``.

These tests drive the real ``ParquetWriter``, the real runner classes and real
OS processes. None of them merely compares stream-name strings.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector import run_bybit_collector, run_collector, run_okx_capture
from collector.collector import parquet_writer as parquet_writer_module
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter, StorageWriterLockedError
from collector.collector.raw_capture import (
    RAW_REST_SCHEMA, RAW_WIRE_SCHEMA, RawCapture, RawRestRecord, RawWireRecord,
)
from collector.collector.replay import ReplayEngine, ReplaySource, replay_directory
from collector.collector.storage_layout import (
    StorageCollisionError, StorageNamespaceError, VENUE_STREAM_PREFIX, iter_segments,
    parse_segment_name, read_streams, venue_stream,
)
from collector.scripts import okx_schema_report

REPO_ROOT = Path(__file__).resolve().parents[2]
VENUES = ("BINANCE", "BYBIT", "OKX")
FROZEN_HOUR = "2026-01-01-00"
BASE_TS = 1_780_444_800_000


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _freeze_hour(monkeypatch, hour: str = FROZEN_HOUR) -> None:
    monkeypatch.setattr(ParquetWriter, "_get_current_hour_str", lambda self: hour)


def _segments(base: Path, stream: str) -> list[tuple[int, Path]]:
    found = []
    for path in iter_segments(base, stream):
        parsed = parse_segment_name(path)
        assert parsed is not None and parsed[2] is not None
        found.append((parsed[2], path))
    return sorted(found)


def _rows(base: Path, stream: str) -> list[dict]:
    rows: list[dict] = []
    for _, path in _segments(base, stream):
        rows.extend(pq.read_table(path).to_pylist())
    return rows


def _wire_row(venue: str, index: int, payload: str | None = None, ts: int | None = None) -> dict:
    return RawWireRecord(
        local_receive_ts=BASE_TS + index if ts is None else ts,
        payload=payload if payload is not None else json.dumps({"v": venue, "i": index}),
        venue=venue, connection_id=f"{venue}-1",
    ).to_row()


def _write_wire(base: Path, venue: str, frames: list[tuple[int, str]]) -> None:
    """Record ``frames`` through the venue's real namespace, writer and capture."""
    writer = ParquetWriter(
        venue_stream(venue, "raw_wire"), RAW_WIRE_SCHEMA, base_dir=str(base),
        exchange=venue, segment_rows=1000, segment_seconds=3600)
    capture = RawCapture(writer, None)
    for ts, payload in frames:
        capture.capture_wire(RawWireRecord(
            local_receive_ts=ts, payload=payload, venue=venue, connection_id=f"{venue}-1"))
    writer.close()


_CHILD = r"""
import os, sys, time
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawWireRecord

exchange, stream, base, go_file, rows = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
ParquetWriter._get_current_hour_str = lambda self: "2026-01-01-00"   # same hour, every process
is_quality = stream.endswith("quality_events")
writer = ParquetWriter(stream, QUALITY_EVENTS_SCHEMA if is_quality else RAW_WIRE_SCHEMA,
                       base_dir=base, exchange=exchange, segment_rows=5, segment_seconds=3600)
print("ready", flush=True)          # sequence allocated and first .tmp opened
deadline = time.time() + 60
while not os.path.exists(go_file):
    if time.time() > deadline:
        sys.exit(3)
    time.sleep(0.005)
for i in range(rows):
    if is_quality:
        writer.write({"timestamp": 1_780_000_000_000 + i, "exchange": exchange,
                      "stream": "orderbook", "event_type": "GAP", "reason": "%s-%d" % (exchange, i)})
    else:
        writer.write(RawWireRecord(
            local_receive_ts=1_780_000_000_000 + i, payload='{"v":"%s","i":%d}' % (exchange, i),
            venue=exchange, connection_id=exchange + "-1").to_row())
writer.close()
print("done", flush=True)
"""


def _spawn_child(exchange: str, stream: str, base: Path, go_file: Path, rows: int) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", _CHILD, exchange, stream, str(base), str(go_file), str(rows)],
        cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _kill_all(processes) -> None:
    for process in processes:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


# ---------------------------------------------------------------------------
# 1. The registry
# ---------------------------------------------------------------------------

def test_registry_gives_every_venue_a_distinct_namespace():
    kinds = ("raw_wire", "raw_rest", "quality_events", "orderbook")
    names = {(venue, kind): venue_stream(venue, kind) for venue in VENUES for kind in kinds}
    assert len(set(names.values())) == len(names), "two venues resolved to one stream directory"

    # Binance keeps the historical names: its recorded history lives there.
    assert venue_stream("BINANCE", "raw_wire") == "raw_wire"
    assert venue_stream("BINANCE", "quality_events") == "quality_events"
    assert venue_stream("okx", "raw_wire") == "okx_raw_wire"
    assert venue_stream("okx", "quality_events") == "okx_quality_events"
    assert venue_stream("Bybit", "raw_wire") == "bybit_raw_wire"
    for venue in ("BYBIT", "OKX"):
        for kind in kinds:
            assert venue_stream(venue, kind) != kind, "a non-Binance venue fell back to a shared name"

    with pytest.raises(ValueError):
        venue_stream("COINBASE", "raw_wire")          # unregistered: never a silent default


def test_read_streams_is_explicit_about_legacy_shared_history():
    assert read_streams("BINANCE", "raw_wire") == ("raw_wire",)
    assert read_streams("BYBIT", "raw_wire") == ("bybit_raw_wire",)
    # OKX's pre-namespace captures share the unprefixed directory with Binance.
    assert read_streams("OKX", "raw_wire") == ("okx_raw_wire", "raw_wire")
    assert read_streams("OKX", "quality_events") == ("okx_quality_events", "quality_events")
    assert read_streams("OKX", "orderbook") == ("okx_orderbook",)   # never shared, no legacy read
    assert set(VENUE_STREAM_PREFIX) == set(VENUES)


# ---------------------------------------------------------------------------
# 2. The actual collision: three venues, two streams, six real processes
# ---------------------------------------------------------------------------

def test_three_venues_write_concurrently_without_collision(tmp_path):
    """The worst-case interleave, on the real writer.

    Every process allocates its segment sequence and opens its first ``.tmp``
    *before any process writes* -- the exact condition under which two writers
    sharing a stream would all have chosen sequence 0 and the same ``.tmp``
    path. Then all six are released at once. Each venue's raw_wire and
    quality_events are independent namespaces, so every one must publish
    sequences 0..4 with nothing lost, mixed, or overwritten.
    """
    rows = 23                      # segment_rows=5 -> segments 0..3 full, 4 holds 3
    go_file = tmp_path / "go"
    plan = [(venue, kind) for venue in VENUES for kind in ("raw_wire", "quality_events")]
    processes = {}
    try:
        for venue, kind in plan:
            processes[(venue, kind)] = _spawn_child(
                venue, venue_stream(venue, kind), tmp_path, go_file, rows)
        for key, process in processes.items():
            assert process.stdout.readline().strip() == "ready", (key, process.stderr.read())
        go_file.write_text("go")
        for key, process in processes.items():
            out, err = process.communicate(timeout=60)
            assert process.returncode == 0, (key, err)
            assert out.strip().endswith("done")
    finally:
        _kill_all(processes.values())

    expected_dirs = sorted(venue_stream(venue, kind) for venue, kind in plan)
    assert sorted(os.listdir(tmp_path / "raw")) == expected_dirs

    for venue, kind in plan:
        stream = venue_stream(venue, kind)
        stream_dir = tmp_path / "raw" / stream
        segments = _segments(tmp_path, stream)
        assert [seq for seq, _ in segments] == [0, 1, 2, 3, 4], stream

        stored = _rows(tmp_path, stream)
        assert len(stored) == rows, stream
        if kind == "raw_wire":
            assert {row["venue"] for row in stored} == {venue}, f"{stream} holds foreign rows"
            assert [json.loads(row["payload"])["i"] for row in stored] == list(range(rows))
            assert {json.loads(row["payload"])["v"] for row in stored} == {venue}
        else:
            assert {row["exchange"] for row in stored} == {venue}
            assert [row["reason"] for row in stored] == [f"{venue}-{i}" for i in range(rows)]

        # Nothing half-written or orphaned; sidecar counts agree with content.
        assert not list(stream_dir.glob("*.tmp")), stream
        assert not list(stream_dir.glob("*.count.json")), stream
        meta_counts = [json.loads(Path(str(path) + ".meta.json").read_text())["record_count"]
                       for _, path in segments]
        assert meta_counts == [5, 5, 5, 5, 3], stream


def test_all_three_venues_on_one_raw_wire_stream_is_refused_not_interleaved(tmp_path, monkeypatch):
    """PR #13's shape: OKX and Binance both declared on ``raw_wire``."""
    with pytest.raises(StorageNamespaceError):
        ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    with pytest.raises(StorageNamespaceError):
        ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="BYBIT")
    with pytest.raises(StorageNamespaceError):
        ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    assert not (tmp_path / "raw").exists(), "a refused writer must not leave a directory behind"


# ---------------------------------------------------------------------------
# 3. The lowest layer: one live writer per stream directory
# ---------------------------------------------------------------------------

def test_duplicate_writer_on_a_live_stream_is_refused_and_the_holder_is_unharmed(tmp_path):
    """A duplicate service start on the same venue, across real processes.

    The second process must fail loudly at construction -- before it can reuse
    the holder's sequence number, open the holder's ``.tmp``, or orphan-delete
    it -- and the holder must publish every row intact.
    """
    rows = 23
    go_file = tmp_path / "go"
    holder = _spawn_child("BINANCE", "raw_wire", tmp_path, go_file, rows)
    intruder = None
    try:
        assert holder.stdout.readline().strip() == "ready", holder.stderr.read()
        live_tmp = list((tmp_path / "raw" / "raw_wire").glob("*.seg.tmp"))
        assert len(live_tmp) == 1

        intruder = _spawn_child("BINANCE", "raw_wire", tmp_path, go_file, rows)
        _, err = intruder.communicate(timeout=60)
        assert intruder.returncode != 0
        assert "StorageWriterLockedError" in err
        assert "pid=" in err, "the refusal should say who holds the stream"

        assert live_tmp[0].exists(), "the refused writer destroyed the holder's live segment"
        go_file.write_text("go")
        out, err = holder.communicate(timeout=60)
        assert holder.returncode == 0, err
    finally:
        _kill_all([p for p in (holder, intruder) if p is not None])

    stored = _rows(tmp_path, "raw_wire")
    assert [json.loads(row["payload"])["i"] for row in stored] == list(range(rows))
    assert [seq for seq, _ in _segments(tmp_path, "raw_wire")] == [0, 1, 2, 3, 4]


def test_second_writer_in_process_is_refused_without_touching_the_live_segment(tmp_path, monkeypatch):
    _freeze_hour(monkeypatch)
    first = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                          exchange="OKX", segment_rows=100, segment_seconds=3600)
    first.write(_wire_row("OKX", 0))
    first.flush()
    live_tmp, counter = first._tmp_filepath, first._counter_filepath
    assert live_tmp.exists() and counter.exists()

    events: list[dict] = []
    with pytest.raises(StorageWriterLockedError):
        ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                      exchange="OKX", quality_event_sink=events.append)

    assert live_tmp.exists() and counter.exists()
    assert events == [], "a refused writer reported a crash it did not observe"
    first.write(_wire_row("OKX", 1))
    first.close()
    assert [json.loads(r["payload"])["i"] for r in _rows(tmp_path, "okx_raw_wire")] == [0, 1]


def test_writer_lock_is_held_across_hour_rollover(tmp_path, monkeypatch):
    """close() at rollover is internal; the stream must stay owned throughout."""
    state = {"hour": "2026-01-01-00"}
    monkeypatch.setattr(ParquetWriter, "_get_current_hour_str", lambda self: state["hour"])
    writer = ParquetWriter("bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                           exchange="BYBIT", segment_rows=100, segment_seconds=3600)
    writer.write(_wire_row("BYBIT", 0))
    state["hour"] = "2026-01-01-01"
    writer.write(_wire_row("BYBIT", 1))           # rollover: hour 00 published, hour 01 opened

    published = [path.name for path in iter_segments(tmp_path, "bybit_raw_wire", date="2026-01-01", hour=0)]
    assert published == ["2026-01-01-00-000000.seg"]
    with pytest.raises(StorageWriterLockedError):
        ParquetWriter("bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="BYBIT")

    writer.close()
    ParquetWriter("bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="BYBIT").close()


def test_closed_writer_cannot_reopen_a_segment_without_its_lock(tmp_path, monkeypatch):
    """close() releases the stream. A late write that crosses an hour boundary
    must not roll over and start writing in a directory the writer no longer owns."""
    state = {"hour": "2026-01-01-00"}
    monkeypatch.setattr(ParquetWriter, "_get_current_hour_str", lambda self: state["hour"])
    writer = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    writer.write(_wire_row("OKX", 0))
    writer.close()
    successor = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    state["hour"] = "2026-01-01-01"
    before = sorted(p.name for p in (tmp_path / "raw" / "okx_raw_wire").iterdir())
    with pytest.raises(RuntimeError, match="closed"):
        writer.write(_wire_row("OKX", 1))
    assert sorted(p.name for p in (tmp_path / "raw" / "okx_raw_wire").iterdir()) == before
    successor.close()


def test_lock_is_released_after_sigkill_and_the_orphan_is_attributed_to_its_venue(tmp_path):
    """Restart-safety: a killed collector must not lock out its successor."""
    script = r"""
import sys, time
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawWireRecord
writer = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=sys.argv[1], exchange="OKX")
for i in range(3):
    writer.write(RawWireRecord(local_receive_ts=1_780_000_000_000 + i, payload="{}", venue="OKX").to_row())
writer.flush()
print("flushed", flush=True)
time.sleep(60)
"""
    process = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], cwd=str(REPO_ROOT),
                               stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "flushed"
        with pytest.raises(StorageWriterLockedError):       # while alive, it owns the stream
            ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=10)

        events: list[dict] = []
        successor = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                                  exchange="OKX", quality_event_sink=events.append)
        successor.close()
    finally:
        _kill_all([process])

    assert len(events) == 1
    assert events[0]["event_type"] == "DATA_DROP"
    assert events[0]["exchange"] == "OKX"          # not the ParquetWriter default, BINANCE
    assert events[0]["stream"] == "okx_raw_wire"
    assert events[0]["rows_lost"] == 3


def test_failed_construction_does_not_leak_the_lock(tmp_path, monkeypatch):
    _freeze_hour(monkeypatch)
    stream_dir = tmp_path / "raw" / "okx_raw_wire"
    stream_dir.mkdir(parents=True)
    (stream_dir / "2026-01-01-00.parquet").write_bytes(b"legacy")
    (stream_dir / "2026-01-01-00-000000.seg").write_bytes(b"segment")
    with pytest.raises(StorageCollisionError):
        ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    (stream_dir / "2026-01-01-00.parquet").unlink()
    # If the failed constructor had kept the lock, this would raise StorageWriterLockedError.
    ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), exchange="OKX").close()


def test_lock_file_is_invisible_to_readers_and_to_sequence_allocation(tmp_path, monkeypatch):
    _freeze_hour(monkeypatch)
    for _ in range(2):                              # second pass is a restart over the lock file
        writer = ParquetWriter("okx_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                               exchange="OKX", segment_rows=100)
        writer.write(_wire_row("OKX", 0))
        writer.close()
    stream_dir = tmp_path / "raw" / "okx_raw_wire"
    assert (stream_dir / ".writer.lock").exists()
    assert parse_segment_name(stream_dir / ".writer.lock") is None
    assert [p.name for p in iter_segments(tmp_path, "okx_raw_wire")] == [
        "2026-01-01-00-000000.seg", "2026-01-01-00-000001.seg"]


def test_restart_continues_the_venues_own_sequence_and_ignores_other_venues(tmp_path, monkeypatch):
    _freeze_hour(monkeypatch)

    def session(venue: str, count: int) -> None:
        writer = ParquetWriter(venue_stream(venue, "raw_wire"), RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                               exchange=venue, segment_rows=2, segment_seconds=3600)
        for i in range(count):
            writer.write(_wire_row(venue, i))
        writer.close()

    session("OKX", 5)              # segments 0,1 full + 2 (one row) on close
    session("BINANCE", 1)          # its own namespace starts at 0 regardless of OKX
    session("OKX", 2)              # restart continues after OKX's highest, not Binance's
    session("BYBIT", 1)

    assert [seq for seq, _ in _segments(tmp_path, "okx_raw_wire")] == [0, 1, 2, 3]
    assert [seq for seq, _ in _segments(tmp_path, "raw_wire")] == [0]
    assert [seq for seq, _ in _segments(tmp_path, "bybit_raw_wire")] == [0]
    assert len(_rows(tmp_path, "okx_raw_wire")) == 7
    assert {row["venue"] for row in _rows(tmp_path, "raw_wire")} == {"BINANCE"}


def test_hazard_two_unlocked_writers_on_one_stream_destroy_a_live_segment(tmp_path, monkeypatch):
    """Why the lock exists. Characterises the design the lock protects.

    With the lock removed, sequence allocation is scan-then-create with nothing
    reserved between. A second writer on the stream picks the *same* sequence,
    opens the *same* ``.tmp``, and its orphan recovery deletes the first
    writer's live segment and reports that as a crash. (The publish-time
    FileExistsError guard does not help: it only fires after the damage.)
    This test does not assert desirable behaviour; it pins the hazard so the
    lock cannot be dismissed as unnecessary.
    """
    monkeypatch.setattr(parquet_writer_module, "_acquire_writer_lock", lambda stream_dir: None)
    _freeze_hour(monkeypatch)
    first = ParquetWriter("shared", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), segment_rows=100)
    first.write(_wire_row("BINANCE", 0))
    first.flush()

    events: list[dict] = []
    second = ParquetWriter("shared", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                           segment_rows=100, quality_event_sink=events.append)

    assert first._seq == second._seq == 0
    assert first._tmp_filepath == second._tmp_filepath
    assert [(e["event_type"], e["rows_lost"]) for e in events] == [("DATA_DROP", 1)]
    for writer in (first, second):
        try:
            writer.close()
        except Exception:      # noqa: BLE001 - the failure mode of the hazard varies
            pass
    recovered = 0
    for _, path in _segments(tmp_path, "shared"):
        try:
            recovered += pq.read_table(path).num_rows
        except Exception:      # noqa: BLE001 - a footerless file is unreadable, that is the point
            pass
    assert recovered == 0, "the first writer's row survived; the hazard this test pins is gone"


# ---------------------------------------------------------------------------
# 4. The actual runners
# ---------------------------------------------------------------------------

def _writers_of(app) -> dict[str, ParquetWriter]:
    return {name: value for name, value in vars(app).items() if isinstance(value, ParquetWriter)}


def test_each_runner_writes_only_to_its_own_namespace(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.chdir(tmp_path)                 # the Binance runner writes to ./data
    binance = run_collector.CollectorApp()
    okx = run_okx_capture.OKXCaptureApp(["funding-rate"], "BTC-USDT-SWAP", str(data), "wss://example.invalid")
    bybit = run_bybit_collector.BybitCollectorApp(data_dir=str(data))
    apps = {"BINANCE": binance, "OKX": okx, "BYBIT": bybit}
    try:
        for venue, app in apps.items():
            assert (app.raw_wire_writer.stream_name, app.raw_wire_writer.exchange) == (
                venue_stream(venue, "raw_wire"), venue)
            assert (app.quality_writer.stream_name, app.quality_writer.exchange) == (
                venue_stream(venue, "quality_events"), venue)

        # Structural: no stream directory is used by two runners.
        owners: dict[Path, str] = {}
        for venue, app in apps.items():
            for name, writer in _writers_of(app).items():
                resolved = writer.stream_dir.resolve()
                assert resolved not in owners, (
                    f"{venue}.{name} shares {resolved} with {owners.get(resolved)}")
                owners[resolved] = venue

        # Behavioural: one frame through each runner's real capture path.
        for venue, app in apps.items():
            app.raw_capture.capture_wire(RawWireRecord(
                local_receive_ts=BASE_TS, payload=json.dumps({"venue": venue}),
                venue=venue, connection_id=f"{venue}-1"))
    finally:
        for app in apps.values():
            for writer in _writers_of(app).values():
                writer.close()

    for venue in VENUES:
        stored = _rows(data, venue_stream(venue, "raw_wire"))
        assert [row["venue"] for row in stored] == [venue]
        assert json.loads(stored[0]["payload"]) == {"venue": venue}


def test_okx_storage_faults_are_attributed_to_okx_not_binance(tmp_path):
    """The OKX writers were built without exchange="OKX", so a crashed OKX segment
    was durably recorded as a BINANCE storage fault."""
    stream_dir = tmp_path / "raw" / "okx_raw_wire"
    stream_dir.mkdir(parents=True)
    (stream_dir / "2026-01-01-00-000000.seg.tmp").write_bytes(b"partial")
    (stream_dir / "2026-01-01-00-000000.seg.count.json").write_text('{"rows": 7}')

    app = run_okx_capture.OKXCaptureApp(["funding-rate"], "BTC-USDT-SWAP", str(tmp_path), "wss://example.invalid")
    app.close()

    drops = [row for row in _rows(tmp_path, "okx_quality_events") if row["event_type"] == "DATA_DROP"]
    assert len(drops) == 1
    assert drops[0]["exchange"] == "OKX"
    assert drops[0]["rows_lost"] == "7"
    assert not (tmp_path / "raw" / "quality_events").exists()


# ---------------------------------------------------------------------------
# 5. Readers and replay
# ---------------------------------------------------------------------------

def _bybit_payload(ts: int, u: int, *, snapshot: bool = False, bid: str = "100.0") -> str:
    return json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot" if snapshot else "delta",
                       "ts": ts, "data": {"u": u, "seq": u, "cts": ts,
                                          "b": [[bid, "1.0"]], "a": [["101.0", "1.0"]]}})


def _binance_depth(ts: int, first: int, last: int, prev: int, bid: str = "100.0") -> str:
    return json.dumps({"stream": "btcusdt@depth@100ms",
                       "data": {"e": "depthUpdate", "E": ts, "T": ts, "U": first, "u": last, "pu": prev,
                                "b": [[bid, "1.0"]], "a": [["101.0", "1.0"]]}})


def _record_all_three_venues(base: Path) -> None:
    """Real recordings for three venues into one data directory."""
    _write_wire(base, "BYBIT", [
        (BASE_TS + 10, _bybit_payload(BASE_TS + 10, 100, snapshot=True)),
        (BASE_TS + 20, _bybit_payload(BASE_TS + 20, 101, bid="100.1")),
        (BASE_TS + 30, _bybit_payload(BASE_TS + 30, 102, bid="100.2")),
    ])
    _write_wire(base, "OKX", [
        (BASE_TS + 15, json.dumps({"arg": {"channel": "funding-rate"}, "data": [{"ts": str(BASE_TS + 15), "fundingRate": "0.0001"}]})),
        (BASE_TS + 25, json.dumps({"arg": {"channel": "funding-rate"}, "data": [{"ts": str(BASE_TS + 25), "fundingRate": "0.0002"}]})),
    ])
    wire = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(base))
    rest = ParquetWriter("raw_rest", RAW_REST_SCHEMA, base_dir=str(base))
    capture = RawCapture(wire, rest)
    capture.capture_wire(RawWireRecord(local_receive_ts=BASE_TS + 10, payload=_binance_depth(BASE_TS + 10, 100, 105, 99),
                                       venue="BINANCE", connection_id="public-1"))
    capture.capture_rest(RawRestRecord(
        request_ts=BASE_TS + 15, response_receive_ts=BASE_TS + 20,
        endpoint="https://fapi.binance.com/fapi/v1/depth", purpose="orderbook_snapshot",
        http_status=200, ok=True,
        payload=json.dumps({"lastUpdateId": 102, "bids": [["100.0", "5.0"]], "asks": [["101.0", "5.0"]]})))
    capture.capture_wire(RawWireRecord(local_receive_ts=BASE_TS + 30, payload=_binance_depth(BASE_TS + 30, 106, 110, 105, "100.1"),
                                       venue="BINANCE", connection_id="public-1"))
    capture.capture_wire(RawWireRecord(local_receive_ts=BASE_TS + 40, payload=_binance_depth(BASE_TS + 40, 111, 115, 110, "100.2"),
                                       venue="BINANCE", connection_id="public-1"))
    wire.close()
    rest.close()


def test_replay_reads_only_the_requested_venue_and_matches_each_venues_own_book(tmp_path):
    _record_all_three_venues(tmp_path)

    bybit_source = ReplaySource.from_directory(str(tmp_path), venue="BYBIT")
    assert len(bybit_source) == 3 and bybit_source.skipped_rows == {}
    assert all("orderbook.50.BTCUSDT" in frame.payload for frame in bybit_source)
    bybit = replay_directory(str(tmp_path), venue="BYBIT")
    assert bybit.final_state == "VALID"
    assert [u.best_bid for u in bybit.book_updates] == ["100.0", "100.1", "100.2"]

    binance_source = ReplaySource.from_directory(str(tmp_path))            # default venue
    assert len(binance_source) == 4 and binance_source.skipped_rows == {}   # 3 depth + 1 REST snapshot
    assert not any("orderbook.50" in frame.payload or "funding-rate" in frame.payload
                   for frame in binance_source)
    binance = replay_directory(str(tmp_path))
    assert binance.final_state == "VALID"
    assert binance.book_updates[-1].best_bid == "100.2"

    okx_source = ReplaySource.from_directory(str(tmp_path), venue="OKX")
    assert len(okx_source) == 2
    assert all("funding-rate" in frame.payload for frame in okx_source)
    okx = replay_directory(str(tmp_path), venue="OKX")
    # OKX has no order book in this fixture -- funding-rate is a non-book
    # canonical event (CanonicalMarkPriceEvent), never routed through
    # LocalBook, so book_updates stays empty and the events land in
    # non_book_events instead.
    assert okx.book_updates == []
    assert len(okx.non_book_events) == 2
    assert [event.funding_rate for event in okx.non_book_events] == [0.0001, 0.0002]
    assert all(event.exchange == "OKX" and event.stream == "funding-rate"
               for event in okx.non_book_events)
    with pytest.raises(ValueError):
        ReplaySource.from_directory(str(tmp_path), venue="COINBASE")


def test_legacy_shared_raw_wire_is_read_by_venue_not_by_directory(tmp_path):
    """Historical OKX frames sit in Binance's ``raw_wire``; both must stay readable
    and neither may leak into the other."""
    legacy = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), segment_rows=1000)
    for i in range(3):
        legacy.write(_wire_row("BINANCE", i, ts=BASE_TS + i))
    for i in range(2):                                   # what PR #13's OKX capture wrote
        legacy.write(_wire_row("OKX", i, payload=json.dumps({"legacy_okx": i}), ts=BASE_TS + 100 + i))
    unattributed = _wire_row("BINANCE", 9, ts=BASE_TS + 200)
    unattributed["venue"] = None
    legacy.write(unattributed)
    legacy.close()

    legacy_dir = tmp_path / "raw" / "raw_wire"
    before = {p.name: (p.stat().st_size, p.stat().st_mtime_ns) for p in legacy_dir.iterdir()}
    _write_wire(tmp_path, "OKX", [(BASE_TS + 300, json.dumps({"new_okx": 0})), (BASE_TS + 301, json.dumps({"new_okx": 1}))])
    after = {p.name: (p.stat().st_size, p.stat().st_mtime_ns) for p in legacy_dir.iterdir()}
    assert before == after, "the new OKX writer touched the legacy shared directory"

    binance = ReplaySource.from_directory(str(tmp_path), venue="BINANCE")
    assert len(binance) == 3
    assert binance.skipped_rows == {"OKX": 2, "<unattributed>": 1}

    okx = ReplaySource.from_directory(str(tmp_path), venue="OKX")
    assert [json.loads(frame.payload) for frame in okx] == [
        {"legacy_okx": 0}, {"legacy_okx": 1}, {"new_okx": 0}, {"new_okx": 1}]
    assert okx.skipped_rows == {"BINANCE": 3, "<unattributed>": 1}

    bybit = ReplaySource.from_directory(str(tmp_path), venue="BYBIT")
    assert len(bybit) == 0 and bybit.skipped_rows == {}


def test_okx_schema_report_reads_the_okx_namespace_and_legacy_but_only_okx_rows(tmp_path):
    funding = json.dumps({"arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
                          "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001"}]})
    _write_wire(tmp_path, "OKX", [(BASE_TS, funding), (BASE_TS + 1, funding)])
    legacy = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path), segment_rows=1000)
    legacy.write(_wire_row("OKX", 0, payload=funding, ts=BASE_TS - 100))
    legacy.write(_wire_row("BINANCE", 1, payload=_binance_depth(BASE_TS, 1, 2, 0), ts=BASE_TS - 50))
    legacy.write(_wire_row("BINANCE", 2, payload=_binance_depth(BASE_TS, 3, 4, 2), ts=BASE_TS - 40))
    legacy.close()

    report = okx_schema_report.collect(str(tmp_path))
    assert report["totals"]["okx_rows"] == 3
    assert report["totals"]["raw_wire_rows"] == 5
    assert report["channels"]["funding-rate"]["frames"] == 3
