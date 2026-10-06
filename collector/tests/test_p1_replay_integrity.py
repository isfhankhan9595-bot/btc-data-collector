"""P1 replay-integrity audit: executable evidence for each defect found, plus
metamorphic invariants.

Contract pinned here (see docs/REPLAY.md):

* ``ReplayResult.digest`` is an OUTPUT digest. It is deliberately unchanged by
  this work (pinned below); it does not prove the inputs were clean.
* ``ReplayResult.input_fingerprint`` identifies the recorded evidence consumed.
* ``ReplayResult.integrity_issues()`` / ``is_pristine`` report every condition
  under which a replay is not a clean, fully evidenced reconstruction.
* Missing precision stays missing: same-millisecond WIRE-vs-REST order is a
  deterministic convention, and replay reports where it relied on it.
"""
from __future__ import annotations

import json
import random

import pytest

from collector.collector.parquet_writer import ParquetWriter
from collector.collector.raw_capture import (
    RAW_REST_SCHEMA, RAW_WIRE_SCHEMA, RawCapture, RawRestRecord, RawWireRecord,
)
from collector.collector.replay import (
    FrameKind, ReplayEngine, ReplayFrame, ReplaySource, replay_directory,
)

T = 1_780_444_800_000
NS = 1_000_000


def _diff(ms, U, u, pu, *, ns=None, idx=0, bid="100.0", conn="c"):
    payload = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": ms, "T": ms, "U": U, "u": u, "pu": pu,
                 "b": [[bid, "1.0"]], "a": [["101.0", "1.0"]]},
    })
    return ReplayFrame(timestamp_ms=ms, kind=FrameKind.WIRE, source_index=idx,
                       payload=payload, connection_id=conn, receive_ns=ns)


def _snap(ms, last_update_id, idx=1):
    payload = json.dumps({"lastUpdateId": last_update_id,
                          "bids": [["100.0", "5"]], "asks": [["101.0", "5"]]})
    return ReplayFrame(timestamp_ms=ms, kind=FrameKind.REST_SNAPSHOT, source_index=idx,
                       payload=payload, http_ok=True, endpoint="e")


def _run(frames):
    return ReplayEngine().run(ReplaySource(frames))


SCENARIOS = {
    "clean": lambda: [_diff(T + 1, 100, 105, 99), _snap(T + 2, 102),
                      _diff(T + 3, 106, 110, 105, idx=2, bid="100.1")],
    "gap": lambda: [_diff(T + 1, 100, 105, 99), _snap(T + 2, 102),
                    _diff(T + 3, 200, 210, 199, idx=2, bid="100.1")],
    "same_ms": lambda: [_diff(T, 100, 105, 99, ns=T * NS + 900_000), _snap(T, 102),
                        _diff(T + 10, 106, 110, 105, idx=2, bid="100.1")],
    "undecodable": lambda: [ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0,
                                        payload="{bad", connection_id="c")],
}

# Digests produced by origin/main@1965ffb for the scenarios above. This change
# must not move them: the digest is a stable output contract.
PINNED_DIGESTS = {
    "clean": "3afd40b4dd704abe7c4ba2dacd539dbefe6999ff9ca3a922c03bb6155e564ba3",
    "gap": "9d4f66924f1d2e2c484a86efe7e2193baea998b60c93e93fbccb4b1069a38d8d",
    "same_ms": "debd749fc8aa192634503b0d4ff59c0ccf614abb264f3c5d367c803fead7eb8f",
    "undecodable": "10a6f84590ee538fcc5155b8e103657e623dbb59d6a77de0aabd30f2410ef720",
}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_output_digest_is_unchanged_by_the_integrity_work(name):
    assert _run(SCENARIOS[name]()).digest == PINNED_DIGESTS[name]


# ---------------------------------------------------------------------------
# DEFECT 1: a never-returned REST request aborted the whole replay
# ---------------------------------------------------------------------------


def _write_segments(tmp_path, wire=(), rest=(), max_payload_bytes=4 * 1024 * 1024):
    wire_writer = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path))
    rest_writer = ParquetWriter("raw_rest", RAW_REST_SCHEMA, base_dir=str(tmp_path))
    capture = RawCapture(wire_writer, rest_writer, max_payload_bytes=max_payload_bytes)
    for record in wire:
        assert capture.capture_wire(record)
    for record in rest:
        assert capture.capture_rest(record)
    wire_writer.close()
    rest_writer.close()


def _wire_rec(frame, venue="BINANCE"):
    return RawWireRecord(local_receive_ts=frame.timestamp_ms, payload=frame.payload,
                         venue=venue, connection_id="c")


def test_request_that_never_returned_does_not_abort_a_recorded_replay(tmp_path):
    """A timed-out REST request is recorded with response_receive_ts=None. Read
    back from parquet that null is NaT, which ``is None`` missed: ``_as_ms``
    then raised and ONE timeout made the whole day unreplayable, contradicting
    the documented 'excluded entirely' behaviour."""
    _write_segments(
        tmp_path,
        wire=[_wire_rec(_diff(T + 1, 100, 105, 99))],
        rest=[RawRestRecord(request_ts=T, response_receive_ts=None, endpoint="https://x/depth",
                            purpose="orderbook_snapshot", ok=False, error="timeout")],
    )
    result = replay_directory(str(tmp_path))
    assert result.frames_total == 1                       # only the wire frame
    assert result.dropped_rest_rows == {"rest_request_never_returned": 1}
    assert not result.is_pristine                         # and it is visible


def test_nat_and_nan_response_timestamps_are_both_treated_as_missing():
    import pandas as pd
    rows = [{"purpose": "orderbook_snapshot", "response_receive_ts": v, "payload": "{}", "ok": False}
            for v in (None, pd.NaT, float("nan"))]
    source = ReplaySource.from_records([], rows)
    assert len(source) == 0
    assert source.dropped_rest_rows == {"rest_request_never_returned": 3}


# ---------------------------------------------------------------------------
# DEFECT 2: excluded source rows left no trace in the result
# ---------------------------------------------------------------------------


def test_foreign_venue_rows_are_visible_in_the_result_not_only_on_the_source(tmp_path):
    clean_dir, dirty_dir = tmp_path / "clean", tmp_path / "dirty"
    good = [_wire_rec(_diff(T + 1, 100, 105, 99)),
            _wire_rec(_diff(T + 2, 106, 110, 105, bid="100.1"))]
    _write_segments(clean_dir, wire=good)
    _write_segments(dirty_dir, wire=good + [
        _wire_rec(_diff(T + 3, 1, 2, 0), venue="OKX"),
        _wire_rec(_diff(T + 4, 1, 2, 0), venue="OKX"),
    ])
    clean, dirty = replay_directory(str(clean_dir)), replay_directory(str(dirty_dir))

    # Output state is identical: the foreign rows never touched the book...
    assert clean.digest == dirty.digest
    # ...but the two runs are NOT equivalent, and the result now says so.
    assert clean.is_pristine
    assert dirty.skipped_rows == {"OKX": 2}
    assert not dirty.is_pristine
    assert dirty.summary() != clean.summary()
    assert dirty.input_fingerprint != clean.input_fingerprint


def test_unsupported_rest_purposes_are_counted_not_silently_dropped():
    rows = [
        {"purpose": "orderbook_snapshot", "response_receive_ts": T + 2, "ok": True,
         "payload": json.dumps({"lastUpdateId": 102, "bids": [["100", "1"]], "asks": [["101", "1"]]})},
        {"purpose": "exchange_info", "response_receive_ts": T + 2, "ok": True, "payload": "{}"},
        {"purpose": "exchange_info", "response_receive_ts": T + 3, "ok": True, "payload": "{}"},
        {"purpose": "future_stream", "response_receive_ts": T + 3, "ok": True, "payload": "{}"},
    ]
    source = ReplaySource.from_records([], rows)
    assert source.dropped_rest_rows == {
        "rest_unsupported_purpose:exchange_info": 2,
        "rest_unsupported_purpose:future_stream": 1,
    }
    result = ReplayEngine().run(source)
    assert result.dropped_rest_rows == source.dropped_rest_rows
    assert not result.is_pristine


# ---------------------------------------------------------------------------
# DEFECT 3: same-millisecond WIRE-vs-REST order is a convention, not evidence
# ---------------------------------------------------------------------------


def test_the_two_physically_possible_orders_give_different_outputs():
    """Pins WHY the tie matters: if the snapshot truly landed before the diff,
    the output differs from if it landed after. REST rows carry whole ms only,
    so replay cannot know which happened within a shared millisecond."""
    d = lambda: _diff(T, 100, 105, 99, ns=T * NS + 900_000)
    snap_first = _run([d(), _snap(T - 1, 102)])
    snap_last = _run([d(), _snap(T + 1, 102)])
    assert snap_first.digest != snap_last.digest


def test_a_same_millisecond_wire_rest_pair_is_reported_as_unresolved():
    result = _run(SCENARIOS["same_ms"]())
    assert result.unresolved_order_ties == {"wire_vs_rest_snapshot_same_ms": 1}
    assert not result.is_pristine
    assert "unresolved_order_ties" in result.integrity_issues()


def test_no_tie_is_reported_when_milliseconds_differ():
    result = _run(SCENARIOS["clean"]())
    assert result.unresolved_order_ties == {}
    assert result.is_pristine


def test_a_legacy_wire_frame_sharing_a_millisecond_with_ns_frames_is_a_tie():
    frames = [_diff(T, 100, 105, 99, ns=T * NS + 5, idx=0),
              _diff(T, 106, 110, 105, ns=None, idx=1)]
    assert ReplaySource(frames).unresolved_order_ties() == {"wire_ns_vs_legacy_same_ms": 1}


def test_tie_detection_does_not_change_the_deterministic_order():
    frames = [_snap(T, 102, idx=5), _diff(T, 100, 105, 99, ns=T * NS + 900_000, idx=1)]
    assert [f.kind for f in ReplaySource(frames).frames] == [FrameKind.WIRE, FrameKind.REST_SNAPSHOT]


# ---------------------------------------------------------------------------
# DEFECT 4: a clipped payload was indistinguishable from an undecodable one
# ---------------------------------------------------------------------------


def test_truncated_recorded_frame_is_refused_under_its_own_reason(tmp_path):
    frame = _diff(T + 1, 100, 105, 99)
    _write_segments(tmp_path, wire=[_wire_rec(frame)], max_payload_bytes=40)
    result = replay_directory(str(tmp_path))
    assert result.frames_truncated == 1
    assert result.frames_undecodable == 1
    assert [e["reason"] for e in result.quality_events] == ["replay_truncated_frame"]
    assert not result.is_pristine


def test_truncated_flag_defaults_to_false_for_legacy_rows():
    source = ReplaySource.from_records([{"local_receive_ts": T, "payload": "{}"}], [])
    assert source.frames[0].truncated is False


# ---------------------------------------------------------------------------
# DEFECT 5: replay quality events carried no source lineage
# ---------------------------------------------------------------------------


def test_replay_quality_events_carry_recorded_time_and_source_lineage():
    frames = [ReplayFrame(timestamp_ms=T + 7, kind=FrameKind.WIRE, source_index=3,
                          payload="{bad", connection_id="conn-9")]
    (event,) = _run(frames).quality_events
    assert event["replay_ts_ms"] == T + 7            # recorded availability, not wall clock
    assert event["replay_source_index"] == 3
    assert event["replay_frame_kind"] == FrameKind.WIRE
    assert event["replay_connection_id"] == "conn-9"


def test_lineage_never_overwrites_an_events_own_fields_and_never_moves_the_digest():
    result = _run(SCENARIOS["gap"]())
    assert all("replay_ts_ms" in e for e in result.quality_events)
    stripped = [{k: v for k, v in e.items() if not k.startswith("replay_")}
                for e in result.quality_events]
    assert [e["reason"] for e in stripped] == [e["reason"] for e in result.quality_events]
    result.quality_events = stripped
    assert result.digest == PINNED_DIGESTS["gap"]


# ---------------------------------------------------------------------------
# Input fingerprint: the missing INPUT counterpart of the output digest
# ---------------------------------------------------------------------------


def _undecodable(payload, *, conn="c", ns=None, idx=0):
    return ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=idx,
                       payload=payload, connection_id=conn, receive_ns=ns)


def test_different_ignored_inputs_share_a_digest_but_not_a_fingerprint():
    a, b = _run([_undecodable("{bad-1")]), _run([_undecodable("{completely different")])
    assert a.digest == b.digest                       # output cannot tell them apart
    assert a.input_fingerprint != b.input_fingerprint  # evidence can


@pytest.mark.parametrize("variant", [
    dict(conn="other-connection"), dict(ns=T * NS + 123), dict(idx=7),
])
def test_fingerprint_is_sensitive_to_recorded_lineage_and_precision(variant):
    base = _run([_undecodable("{bad")])
    changed = _run([_undecodable("{bad", **variant)])
    assert base.digest == changed.digest
    assert base.input_fingerprint != changed.input_fingerprint


def test_fingerprint_is_stable_and_invariant_to_supply_order():
    frames = SCENARIOS["clean"]()
    baseline = _run(frames).input_fingerprint
    assert _run(SCENARIOS["clean"]()).input_fingerprint == baseline
    for seed in range(8):
        shuffled = frames[:]
        random.Random(seed).shuffle(shuffled)
        assert _run(shuffled).input_fingerprint == baseline


# ---------------------------------------------------------------------------
# Metamorphic isolation invariants
# ---------------------------------------------------------------------------


def test_future_frames_do_not_change_an_earlier_state():
    base = _run(SCENARIOS["clean"]())
    extended = _run(SCENARIOS["clean"]() + [_diff(T + 50, 111, 115, 110, idx=3, bid="100.2")])
    assert [u.digest_tuple() for u in extended.book_updates[:len(base.book_updates)]] == \
           [u.digest_tuple() for u in base.book_updates]


def test_a_late_snapshot_cannot_repair_an_earlier_unbridged_state():
    early = [_diff(T + 1, 100, 105, 99)]
    assert _run(early).book_updates == []             # nothing bridged without a snapshot
    (update,) = _run(early + [_snap(T + 500, 102)]).book_updates
    # The buffered diff is committed only when the snapshot lands. Its own
    # receive time (T+1) is earlier, but the VALID state it represents was not
    # AVAILABLE until T+500; ``available_ts_ms`` says so.
    assert update.event_kind == "RECOVERY_BRIDGE"
    assert update.timestamp_ms == T + 1
    assert update.available_ts_ms == T + 500


def test_no_book_update_is_available_before_the_frame_that_produced_it():
    for name in SCENARIOS:
        for update in _run(SCENARIOS[name]()).book_updates:
            assert update.available_ts_ms is not None
            assert update.available_ts_ms >= update.timestamp_ms


def test_available_ts_is_not_part_of_the_digest_tuple():
    (update,) = _run([_diff(T + 1, 100, 105, 99), _snap(T + 500, 102)]).book_updates
    assert update.available_ts_ms not in update.digest_tuple()


def test_changing_a_consumed_payload_changes_the_digest():
    changed = SCENARIOS["clean"]()
    changed[2] = _diff(T + 3, 106, 110, 105, idx=2, bid="999.9")
    assert _run(changed).digest != PINNED_DIGESTS["clean"]


def test_empty_replay_is_never_pristine():
    result = ReplayEngine().run(ReplaySource([]))
    assert result.integrity_issues() == {"empty_replay": True}
    assert not result.is_pristine


def test_summary_exposes_integrity_and_fingerprint():
    summary = _run(SCENARIOS["same_ms"]()).summary()
    assert summary["pristine"] is False
    assert summary["integrity_issues"] == {"unresolved_order_ties": {"wire_vs_rest_snapshot_same_ms": 1}}
    assert len(summary["input_fingerprint"]) == 64


# ---------------------------------------------------------------------------
# Causal contract: exchange time and request time never decide availability
# ---------------------------------------------------------------------------


def test_exchange_event_time_never_decides_availability_or_order():
    """Two frames whose EXCHANGE stamps point the opposite way to their local
    receive stamps. Exchange time is source evidence, not availability."""
    first, second = _diff(T + 1, 100, 105, 99), _diff(T + 2, 106, 110, 105, idx=1)
    rows = [
        {"local_receive_ts": T + 1, "exchange_event_ts": T + 900, "payload": first.payload, "connection_id": "c"},
        {"local_receive_ts": T + 2, "exchange_event_ts": T + 100, "payload": second.payload, "connection_id": "c"},
    ]
    frames = ReplaySource.from_records(rows, []).frames
    assert [f.timestamp_ms for f in frames] == [T + 1, T + 2]
    assert [f.payload for f in frames] == [first.payload, second.payload]


def test_rest_availability_is_the_response_time_never_the_request_time():
    rows = [{"purpose": "orderbook_snapshot", "request_ts": T + 1, "response_receive_ts": T + 500,
             "ok": True, "payload": "{}", "endpoint": "e"}]
    (frame,) = ReplaySource.from_records([], rows).frames
    assert frame.timestamp_ms == T + 500


# ---------------------------------------------------------------------------
# OKX order-book replay: previously "not covered by a committed test"
# ---------------------------------------------------------------------------

_OKX_ARG = {"channel": "books", "instId": "BTC-USDT-SWAP"}


def _okx_books(action, seq, prev, bids, asks):
    entry = {"asks": asks, "bids": bids, "ts": str(T), "checksum": 0, "seqId": seq}
    if prev is not None:
        entry["prevSeqId"] = prev
    return json.dumps({"arg": _OKX_ARG, "action": action, "data": [entry]})


def _okx_wire(idx, ms, payload):
    return ReplayFrame(timestamp_ms=ms, kind=FrameKind.WIRE, source_index=idx,
                       payload=payload, connection_id="okx-1")


def _okx_run(*payloads):
    frames = [_okx_wire(i, T + 1 + i, p) for i, p in enumerate(payloads)]
    return ReplayEngine("OKX").run(ReplaySource(frames))


_OKX_SNAP = _okx_books("snapshot", 100, -1, [["67123.40", "1", "0", "1"]], [["67123.50", "2", "0", "1"]])


def test_okx_snapshot_update_and_level_delete_reconstruct_through_replay():
    result = _okx_run(
        _OKX_SNAP,
        _okx_books("update", 101, 100, [["67123.41", "1", "0", "1"]], []),
        _okx_books("update", 102, 101, [], [["67123.50", "0", "0", "0"]]),   # size 0 deletes the level
    )
    assert result.final_state == "VALID"
    assert [(u.update_id, u.best_bid, u.best_ask) for u in result.book_updates] == [
        (100, "67123.40", "67123.50"),   # price text is preserved, trailing zero included
        (101, "67123.41", "67123.50"),
        (102, "67123.41", None),
    ]


@pytest.mark.parametrize("label,payload", [
    ("prevSeqId mismatch", _okx_books("update", 150, 120, [["67123.41", "1", "0", "1"]], [])),
    ("sequence reset", _okx_books("update", 50, 49, [["67123.41", "1", "0", "1"]], [])),
    ("missing seqId/prevSeqId", json.dumps({"arg": _OKX_ARG, "action": "update", "data": [
        {"asks": [], "bids": [["1", "1", "0", "1"]], "ts": str(T)}]})),
])
def test_okx_unprovable_sequence_fails_closed_in_replay(label, payload):
    result = _okx_run(_OKX_SNAP, payload)
    assert result.final_state == "SEQUENCE_GAP", label
    assert len(result.book_updates) == 1              # only the snapshot; the bad delta is not applied


def test_okx_update_with_no_snapshot_never_becomes_a_book():
    result = _okx_run(_okx_books("update", 101, 100, [["67123.41", "1", "0", "1"]], []))
    assert result.book_updates == []
    assert result.final_state != "VALID"


def test_okx_malformed_level_is_unhandled_and_visible_never_raised():
    result = _okx_run(_OKX_SNAP, _okx_books("update", 101, 100, [["abc", "1", "0", "1"]], []))
    assert result.frames_unhandled == 1
    assert not result.is_pristine
    assert any(e["reason"] == "adapter_unhandled:malformed_payload" for e in result.quality_events)
