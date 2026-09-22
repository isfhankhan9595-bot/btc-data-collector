"""Phase H, item 14: extended storage/replay corruption matrix.

PR #44's ``test_storage_replay_corruption_resistance.py`` proved one case:
a corrupted canonical ``instrument_key`` (OKX only) does not rewrite the
identity raw replay produces. This file establishes the general mechanism
that fact is one instance of, then extends the *dynamic* proof to more
fields and a second venue.

The general mechanism (static, source-level): ``ReplaySource.from_directory``
calls ``read("raw_wire")`` and ``read("raw_rest")`` only -- confirmed below
by inspecting its own source, not assumed from its docstring. The canonical
stream names never appear as an argument to ``read()`` anywhere in the
method. Combined with PR #44's existing static check (neither
``book_observation.py`` nor ``replay.py`` references ``instrument_key`` at
all), canonical Parquet -- every column of it -- is never opened by this
code path. That is what makes every case below "ignore" by construction.

Schema note (found while building this file, worth recording): the generic
``ORDERBOOK_SCHEMA`` (used by Binance USD-M's and Bybit's real ``ob_writer``)
carries no ``exchange``, ``market_type``, ``update_id`` or ``quality_state``
columns of its own -- identity there is carried entirely by
``instrument_key``, and update-tracking / quality are not per-row order-book
columns at all. Those four fields *do* exist, for real, on
``QUALITY_EVENTS_SCHEMA`` (schema #22 of the 24-schema audit -- deliberately
excluded from compaction, but still a real writer every collector app
already has as ``quality_writer``), so the update-id/quality-state/exchange/
timestamp corruption cases below target that schema instead of inventing
fields that do not exist on the order-book one.

OKX has no order-book canonical schema in production at all (Gate 1's
finding, and confirmed again here: it is absent from the 24-schema list).
The one case below that corrupts a canonical *book row* therefore
constructs a standalone writer using the generic ``ORDERBOOK_SCHEMA`` under
an OKX-prefixed stream name -- an explicitly hypothetical simulation ("if
OKX book data were persisted with the standard schema shape, would its
corruption still be ignored"), not a claim that this schema exists for OKX
today. Marked as such in the test itself.

Classification (per the task's corruption-classification requirement): every
case below is **ignore** -- not reject, not a quality event, not recovery.
The corrupted field was never a candidate source of truth for this path, so
there is nothing to detect or react to. Compare with Phase D's compaction
layer (``test_storage_identity_read_path.py``), where a corrupted canonical
``instrument_key`` *is* rejected, because compaction's job is to read and
trust that column. Same corruption, different architectural role, different
correct response.
"""
from __future__ import annotations

import inspect
from decimal import Decimal

from collector.collector.book_observation import reconstruct_book_at
from collector.collector.config import ORDERBOOK_SCHEMA
from collector.collector.instrument import BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import BookQuality
from collector.collector.replay import FrameKind, ReplayFrame
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.run_bybit_collector import BybitCollectorApp
from collector.run_okx_collector import OKXCollectorApp
from collector.tests.test_book_observation_okx import _okx_book_frame
from collector.tests.test_replay_bybit_parity import _bybit_frame


# ---------------------------------------------------------------------------
# Static: the general mechanism, confirmed by source inspection.
# ---------------------------------------------------------------------------


def test_replay_source_from_directory_reads_only_raw_streams():
    from collector.collector import replay
    src = inspect.getsource(replay.ReplaySource.from_directory)
    assert 'read("raw_wire")' in src
    assert 'read("raw_rest")' in src
    for canonical_stream in ("orderbook", "trades", "markprice", "openinterest", "liquidation"):
        assert f'read("{canonical_stream}")' not in src


def test_book_observation_and_replay_modules_import_no_canonical_writer():
    from collector.collector import book_observation, replay
    combined = inspect.getsource(book_observation) + inspect.getsource(replay)
    for forbidden in ("parquet_writer", "compact_daily", "ParquetWriter"):
        assert forbidden not in combined


# ---------------------------------------------------------------------------
# Dynamic: OKX raw session, corrupted QUALITY_EVENTS_SCHEMA row alongside it
# (exchange, local_receive_ts, update_id, quality_state -- all real columns
# on this schema, none of which exist on the order-book one).
# ---------------------------------------------------------------------------


def _okx_frames():
    return [
        _okx_book_frame(2_000, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(2_010, seq_id=101, prev_seq_id=100, bid="65000.1", index=1),
    ]


def _replay(frames, ts, venue):
    replayed = [ReplayFrame(timestamp_ms=f.timestamp_ms, kind=FrameKind.WIRE,
                            source_index=i, payload=f.payload) for i, f in enumerate(frames)]
    return reconstruct_book_at(replayed, ts, venue=venue)


def _write_raw(app, frames, venue):
    for f in frames:
        app.raw_wire_writer.write({"timestamp": f.timestamp_ms, "connection_id": "c",
                                    "payload": f.payload, "venue": venue})
    app.raw_wire_writer.close()


def test_corrupted_canonical_exchange_does_not_rewrite_identity(tmp_path):
    """Corrupt QUALITY_EVENTS_SCHEMA's own ``exchange`` column."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    frames = _okx_frames()
    _write_raw(app, frames, "OKX")
    app.quality_writer.write({
        "timestamp": 2_010, "exchange": "BYBIT",  # corrupted: this is OKX data
        "stream": "okx_orderbook", "event_type": "SEQUENCE_GAP", "reason": "fabricated",
        "quality_state": "SEQUENCE_GAP", "local_receive_ts": 2_010,
    })
    app.quality_writer.close()

    obs = _replay(frames, 2_010, "OKX")
    assert obs.instrument == OKX_SWAP_BTCUSDT
    assert obs.quality_state == BookQuality.VALID.value   # no phantom gap from the fabricated event


def test_corrupted_canonical_timestamp_does_not_shift_causal_availability(tmp_path):
    """Corrupt QUALITY_EVENTS_SCHEMA's own ``local_receive_ts`` column to a
    wildly different value. Causal availability must remain governed only
    by the raw frames' own ReplayFrame.timestamp_ms."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    frames = _okx_frames()
    _write_raw(app, frames, "OKX")
    app.quality_writer.write({
        "timestamp": 9_999_999, "exchange": "OKX", "stream": "okx_orderbook",
        "event_type": "SEQUENCE_GAP", "reason": "fabricated", "quality_state": "SEQUENCE_GAP",
        "local_receive_ts": 9_999_999,   # corrupted: nowhere near the real frames' timestamps
    })
    app.quality_writer.close()

    at_real_ts = _replay(frames, 2_010, "OKX")
    at_far_future_ts = _replay(frames, 9_999_999, "OKX")
    assert at_real_ts.status is AlignmentStatus.AVAILABLE
    assert at_far_future_ts.last_update_local_receive_ts == 2_010


def test_corrupted_canonical_update_id_does_not_rewrite_sequence_state(tmp_path):
    """Corrupt QUALITY_EVENTS_SCHEMA's own ``update_id`` column."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    frames = _okx_frames()
    _write_raw(app, frames, "OKX")
    app.quality_writer.write({
        "timestamp": 2_010, "exchange": "OKX", "stream": "okx_orderbook",
        "event_type": "SEQUENCE_GAP", "reason": "fabricated", "quality_state": "SEQUENCE_GAP",
        "local_receive_ts": 2_010, "update_id": 999_999_999,  # corrupted
    })
    app.quality_writer.close()

    obs = _replay(frames, 2_010, "OKX")
    assert obs.quality_state == BookQuality.VALID.value
    assert obs.instrument == OKX_SWAP_BTCUSDT


def test_corrupted_canonical_quality_state_does_not_mask_a_real_gap(tmp_path):
    """Corrupt QUALITY_EVENTS_SCHEMA's own ``quality_state`` column to
    falsely claim VALID, while the raw frames genuinely contain a gap."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    frames = [
        _okx_book_frame(2_000, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(2_010, seq_id=501, prev_seq_id=500, bid="1.0", index=1),  # genuine gap
    ]
    _write_raw(app, frames, "OKX")
    app.quality_writer.write({
        "timestamp": 2_010, "exchange": "OKX", "stream": "okx_orderbook",
        "event_type": "RECOVERY", "reason": "fabricated",
        "quality_state": "VALID",  # corrupted: falsely claims VALID despite the real gap above
        "local_receive_ts": 2_010, "update_id": 501,
    })
    app.quality_writer.close()

    obs = _replay(frames, 2_020, "OKX")
    assert obs.quality_state != BookQuality.VALID.value


def test_corrupted_hypothetical_okx_book_row_does_not_leak_into_reconstruction(tmp_path):
    """OKX has no order-book canonical schema in production (Gate 1's
    finding, confirmed again by its absence from the 24-schema list) --
    this test is an explicit hypothetical: 'if a canonical OKX book row
    existed, using the standard ORDERBOOK_SCHEMA shape, would its
    corruption still be ignored'. Not a claim such storage exists today."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    frames = _okx_frames()
    _write_raw(app, frames, "OKX")

    hypothetical_ob_writer = ParquetWriter("okx_orderbook", ORDERBOOK_SCHEMA,
                                            base_dir=str(tmp_path), exchange="OKX")
    hypothetical_ob_writer.write({
        "timestamp": 2_010, "exchange_timestamp": 2_010, "local_timestamp": 2_010,
        "bids_price": [1.0], "bids_qty": [999.0], "asks_price": [2.0], "asks_qty": [999.0],
        "instrument_key": OKX_SWAP_BTCUSDT.key,
    })
    hypothetical_ob_writer.close()

    obs = _replay(frames, 2_010, "OKX")
    assert Decimal("1.0") not in dict(obs.bids)
    assert Decimal("65000.1") in dict(obs.bids)


# ---------------------------------------------------------------------------
# Second venue: Bybit, proving this is not an OKX-only property.
# ---------------------------------------------------------------------------


def test_corrupted_canonical_book_row_does_not_leak_into_bybit_reconstruction(tmp_path):
    app = BybitCollectorApp(data_dir=str(tmp_path))
    frames = [
        _bybit_frame(3_000, u=100, is_snapshot=True, index=0),
        _bybit_frame(3_010, u=101, bid="100.5", index=1),
    ]
    _write_raw(app, frames, "BYBIT")
    app.ob_writer.write({
        "timestamp": 3_010, "exchange_timestamp": 3_010, "local_timestamp": 3_010,
        "bids_price": [1.0], "bids_qty": [999.0], "asks_price": [2.0], "asks_qty": [999.0],
        "update_id": 999_999, "sequence": 999_999, "is_snapshot": False,
        "instrument_key": OKX_SWAP_BTCUSDT.key,  # corrupted: this is Bybit data
    })
    app.ob_writer.close()

    replayed = [ReplayFrame(timestamp_ms=f.timestamp_ms, kind=FrameKind.WIRE,
                            source_index=i, payload=f.payload) for i, f in enumerate(frames)]
    obs = reconstruct_book_at(replayed, 3_010, venue="BYBIT")
    assert obs.instrument == BYBIT_LINEAR_BTCUSDT
    assert obs.instrument != OKX_SWAP_BTCUSDT
    assert obs.quality_state == BookQuality.VALID.value
    assert Decimal("1.0") not in dict(obs.bids)
    assert Decimal("100.5") in dict(obs.bids)
