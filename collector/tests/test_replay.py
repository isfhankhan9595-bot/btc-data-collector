"""Adversarial tests for deterministic replay and live/replay parity.

Invariants:

1. Same recorded input, run twice, produces an identical digest.
2. Replay cannot use information from the future to repair the past.
3. Replay and live drive the same reconstruction objects -- there is no
   second implementation free to diverge.
4. Replay never contacts the network.
5. A broken causal chain cannot become VALID by numerical coincidence.
"""
from __future__ import annotations

import json
import random
from decimal import Decimal

import pytest

from collector.collector.book_engine import LocalBook
from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.quality_events import BookQuality
from collector.collector.replay import (
    FrameKind,
    ReplayEngine,
    ReplayFrame,
    ReplaySource,
    replay_directory,
)

BASE_TS = 1_780_444_800_000


def _depth_frame(ts, U, u, pu, bid="100.0", ask="101.0", index=0):
    payload = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": ts, "T": ts, "U": U, "u": u, "pu": pu,
                 "b": [[bid, "1.0"]], "a": [[ask, "1.0"]]},
    })
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                       payload=payload, connection_id="public-1")


def _snapshot_frame(ts, last_update_id, index=0, ok=True, bid="100.0", ask="101.0"):
    payload = json.dumps({
        "lastUpdateId": last_update_id,
        "bids": [[bid, "5.0"]], "asks": [[ask, "5.0"]],
    })
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.REST_SNAPSHOT,
                       source_index=index, payload=payload, http_ok=ok,
                       endpoint="https://fapi.binance.com/fapi/v1/depth")


def _normal_session():
    """Snapshot then a clean diff chain bridging it."""
    return [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1),
        _depth_frame(BASE_TS + 30, U=106, u=110, pu=105, bid="100.1", index=2),
        _depth_frame(BASE_TS + 40, U=111, u=115, pu=110, bid="100.2", index=3),
    ]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_same_input_twice_produces_identical_digest():
    frames = _normal_session()
    first = ReplayEngine().run(ReplaySource(frames))
    second = ReplayEngine().run(ReplaySource(frames))
    assert first.digest == second.digest
    assert first.summary() == second.summary()


def test_digest_is_order_independent_of_input_shuffling():
    """Ordering is established by the total key, not by arrival order."""
    frames = _normal_session()
    baseline = ReplayEngine().run(ReplaySource(frames)).digest
    for seed in range(10):
        shuffled = frames[:]
        random.Random(seed).shuffle(shuffled)
        assert ReplayEngine().run(ReplaySource(shuffled)).digest == baseline


def test_digest_changes_when_the_book_changes():
    baseline = ReplayEngine().run(ReplaySource(_normal_session())).digest
    altered = _normal_session()
    altered[2] = _depth_frame(BASE_TS + 30, U=106, u=110, pu=105, bid="999.9", index=2)
    assert ReplayEngine().run(ReplaySource(altered)).digest != baseline


def test_digest_covers_quality_transitions_not_only_prices():
    """Same final prices via a different quality path must not compare equal."""
    clean = _normal_session()
    with_gap = [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1),
        _depth_frame(BASE_TS + 30, U=200, u=210, pu=199, bid="100.1", index=2),
    ]
    assert ReplayEngine().run(ReplaySource(clean)).digest != \
           ReplayEngine().run(ReplaySource(with_gap)).digest


def test_ties_are_broken_deterministically_by_kind_then_source_index():
    same_ms = [
        _snapshot_frame(BASE_TS, last_update_id=102, index=5),
        _depth_frame(BASE_TS, U=100, u=105, pu=99, index=1),
    ]
    ordered = ReplaySource(same_ms).frames
    # Wire before snapshot in the same millisecond, matching live.
    assert ordered[0].kind == FrameKind.WIRE
    assert ordered[1].kind == FrameKind.REST_SNAPSHOT


# ---------------------------------------------------------------------------
# Causality
# ---------------------------------------------------------------------------


def test_snapshot_is_not_available_before_it_landed():
    """A late snapshot cannot retroactively repair an earlier gap."""
    frames = _normal_session()[:3] + [
        _depth_frame(BASE_TS + 50, U=900, u=905, pu=899, index=4),      # gap
        _snapshot_frame(BASE_TS + 9_000, last_update_id=903, index=5),  # lands later
    ]
    ordered = ReplaySource(frames).frames
    assert ordered[-1].kind == FrameKind.REST_SNAPSHOT, "snapshot must sort last"

    result = ReplayEngine().run(ReplaySource(frames))
    gap_seen = [e for e in result.quality_events if e.get("event_type") == "SEQUENCE_GAP"]
    assert gap_seen, "the gap must be observed before the snapshot lands"

    # The late snapshot opens a new recovery generation. Everything it
    # bridged must come from diffs at or after the gap -- it cannot reach
    # back and re-validate the pre-gap chain.
    late_generation = max(u.recovery_generation for u in result.book_updates)
    late_bridged = [u for u in result.book_updates
                    if u.recovery_generation == late_generation
                    and u.event_kind.startswith("RECOVERY")]
    assert late_bridged, "the late snapshot should have bridged the post-gap chain"
    for update in late_bridged:
        assert update.timestamp_ms >= BASE_TS + 50


def test_failed_snapshot_request_bridges_nothing():
    frames = [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1, ok=False),
    ]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.snapshots_applied == 0
    assert result.snapshots_rejected == 1
    assert result.book_updates == []
    # Never bridged, so never VALID.
    assert result.final_state != BookQuality.VALID.value


def test_snapshot_request_with_no_response_is_excluded_entirely():
    """A request that never returned never bridged anything live."""
    source = ReplaySource.from_records(
        wire_rows=[],
        rest_rows=[{
            "purpose": "orderbook_snapshot", "response_receive_ts": None,
            "request_ts": BASE_TS, "payload": "{}", "ok": False,
        }],
    )
    assert len(source) == 0


def test_non_snapshot_rest_purposes_do_not_drive_the_book():
    source = ReplaySource.from_records(
        wire_rows=[],
        rest_rows=[{
            "purpose": "open_interest", "response_receive_ts": BASE_TS,
            "payload": json.dumps({"openInterest": "1"}), "ok": True,
        }],
    )
    assert len(source) == 0


def test_undecodable_frame_stays_undecodable_in_replay():
    """Replay must not process data the live run never saw."""
    frames = [ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE,
                          source_index=0, payload='{"stream":"x","data":{}}',
                          decode_ok=False)]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.frames_undecodable == 1
    assert result.book_updates == []


def test_frame_that_fails_to_parse_now_is_also_counted():
    frames = [ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE,
                          source_index=0, payload="{broken")]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.frames_undecodable == 1


# ---------------------------------------------------------------------------
# Reconstruction correctness under attack
# ---------------------------------------------------------------------------


def test_sequence_gap_is_detected_and_does_not_become_valid():
    frames = [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1),
        _depth_frame(BASE_TS + 30, U=500, u=510, pu=499, index=2),  # broken chain
    ]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.final_state != BookQuality.VALID.value


def test_numerically_increasing_ids_cannot_repair_a_broken_chain():
    """The attack the spec calls out explicitly."""
    frames = [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1),
        _depth_frame(BASE_TS + 30, U=900, u=905, pu=899, index=2),   # gap
        _depth_frame(BASE_TS + 40, U=906, u=910, pu=905, index=3),   # tidy, but after a gap
        _depth_frame(BASE_TS + 50, U=911, u=915, pu=910, index=4),
    ]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.final_state != BookQuality.VALID.value, (
        "a chain broken by a gap must not become VALID through later "
        "well-formed increments"
    )


def test_replayed_repeat_of_an_applied_diff_is_not_silently_accepted():
    """Binance continuity is the pu rule; a resent diff breaks it.

    BinanceSequenceComparator has no duplicate branch -- unlike Bybit, which
    compares update_id equality. A repeated diff therefore fails
    `pu == previous.u` and is treated as a gap rather than being waved
    through. That is the conservative outcome: it cannot produce a falsely
    VALID book.
    """
    applied = _depth_frame(BASE_TS + 30, U=106, u=110, pu=105, bid="100.1", index=2)
    frames = _normal_session()[:3] + [
        ReplayFrame(timestamp_ms=BASE_TS + 35, kind=FrameKind.WIRE,
                    source_index=9, payload=applied.payload),
    ]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.final_state != BookQuality.VALID.value
    assert any(e.get("event_type") == "SEQUENCE_GAP" for e in result.quality_events)


def test_malformed_snapshot_payload_is_rejected_not_applied():
    frames = [
        _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, index=0),
        ReplayFrame(timestamp_ms=BASE_TS + 20, kind=FrameKind.REST_SNAPSHOT,
                    source_index=1, payload='{"lastUpdateId": "not-a-number"}'),
    ]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.snapshots_rejected == 1
    assert result.snapshots_applied == 0


def test_empty_snapshot_is_rejected():
    frames = [ReplayFrame(
        timestamp_ms=BASE_TS, kind=FrameKind.REST_SNAPSHOT, source_index=0,
        payload=json.dumps({"lastUpdateId": 5, "bids": [], "asks": []}),
    )]
    result = ReplayEngine().run(ReplaySource(frames))
    assert result.snapshots_rejected == 1


def test_clean_session_reaches_valid_and_produces_book_updates():
    result = ReplayEngine().run(ReplaySource(_normal_session()))
    assert result.final_state == BookQuality.VALID.value
    assert result.snapshots_applied == 1
    assert len(result.book_updates) >= 2
    assert result.book_updates[-1].best_bid is not None


def test_unknown_frame_kind_is_recorded_not_ignored():
    frames = [ReplayFrame(timestamp_ms=BASE_TS, kind="mystery",
                          source_index=0, payload="{}")]
    result = ReplayEngine().run(ReplaySource(frames))
    assert any("replay_unknown_frame_kind" in str(e.get("reason"))
               for e in result.quality_events)


# ---------------------------------------------------------------------------
# Live / replay parity
# ---------------------------------------------------------------------------


def test_replay_drives_the_same_objects_as_live():
    engine = ReplayEngine()
    assert isinstance(engine.adapter, BinanceAdapter)
    assert isinstance(engine.book, LocalBook)


def test_replay_module_imports_no_network_client():
    """Replay that can reach the exchange is not replay."""
    import ast

    import collector.collector.replay as replay_module

    tree = ast.parse(open(replay_module.__file__).read())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level:
            imported.add((node.module or "").split(".")[0])

    forbidden = {"aiohttp", "requests", "urllib", "httpx", "socket", "http"}
    assert not (imported & forbidden), f"replay must not import {imported & forbidden}"


def test_replay_reconstruction_matches_a_direct_book_drive():
    """Parity: the same frames through the engine and through LocalBook alone."""
    frames = _normal_session()
    replayed = ReplayEngine().run(ReplaySource(frames))

    # Drive the production objects by hand, in the same order.
    adapter = BinanceAdapter()
    book = LocalBook("BINANCE")
    direct_bests = []
    for frame in ReplaySource(frames):
        if frame.kind == FrameKind.REST_SNAPSHOT:
            payload = json.loads(frame.payload)
            from collector.collector.canonical import CanonicalOrderBookEvent
            snapshot = CanonicalOrderBookEvent(
                "BINANCE", "orderbook", None, None, frame.timestamp_ms,
                bids=tuple((Decimal(p), Decimal(q)) for p, q in payload["bids"]),
                asks=tuple((Decimal(p), Decimal(q)) for p, q in payload["asks"]),
                update_id=int(payload["lastUpdateId"]), is_snapshot=True,
            )
            if book.binance_snapshot(snapshot.update_id, snapshot):
                for applied, _, _ in book.committed_recovery_events:
                    direct_bests.append(str(applied.bids[0][0]))
                book.committed_recovery_events = []
            continue
        for event in adapter.normalize(json.loads(frame.payload),
                                       local_receive_ts=frame.timestamp_ms):
            applied = book.apply(event)
            if applied is not None:
                direct_bests.append(str(applied.bids[0][0]))

    assert [u.best_bid for u in replayed.book_updates] == direct_bests
    assert book.state.state.value == replayed.final_state


# ---------------------------------------------------------------------------
# Reading recorded segments from disk
# ---------------------------------------------------------------------------


def test_replay_from_recorded_segments_round_trips(tmp_path):
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import (
        RAW_REST_SCHEMA, RAW_WIRE_SCHEMA, RawCapture, RawRestRecord, RawWireRecord,
    )

    wire_writer = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path))
    rest_writer = ParquetWriter("raw_rest", RAW_REST_SCHEMA, base_dir=str(tmp_path))
    capture = RawCapture(wire_writer, rest_writer)

    for frame in _normal_session():
        if frame.kind == FrameKind.WIRE:
            capture.capture_wire(RawWireRecord(
                local_receive_ts=frame.timestamp_ms, payload=frame.payload,
                venue="BINANCE", connection_id="public-1",
            ))
        else:
            capture.capture_rest(RawRestRecord(
                request_ts=frame.timestamp_ms - 5,
                response_receive_ts=frame.timestamp_ms,
                endpoint="https://fapi.binance.com/fapi/v1/depth",
                purpose="orderbook_snapshot", http_status=200, ok=True,
                payload=frame.payload,
            ))
    wire_writer.close()
    rest_writer.close()

    result = replay_directory(str(tmp_path))
    in_memory = ReplayEngine().run(ReplaySource(_normal_session()))

    assert result.final_state == in_memory.final_state
    assert [u.best_bid for u in result.book_updates] == \
           [u.best_bid for u in in_memory.book_updates]


def test_replay_of_an_empty_directory_is_not_a_false_success(tmp_path):
    result = replay_directory(str(tmp_path))
    assert result.frames_total == 0
    assert result.book_updates == []
    # An empty replay must not look like a clean reconstruction.
    assert result.snapshots_applied == 0
