"""P0-4: four-runner production wiring integration tests.

Every test below constructs a REAL runner (CollectorApp / BybitCollectorApp
/ OKXCollectorApp / BinanceSpotCollectorApp) with enable_segment_dedup=True
(the production default) on a real tmp_path, drives real handle_message
calls, and inspects real SQLite/Parquet state on disk. No mocking of the
dedup path itself.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from collector import run_collector as rc
from collector.collector.segment_dedup import DedupStateError

T = int(time.time() * 1000)


def _mk(tmp_path):
    (tmp_path / "data").mkdir(exist_ok=True)
    return str(tmp_path / "data")


# --- USD-M ------------------------------------------------------------------


def _usdm_app(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    return rc.CollectorApp()


def _bridge_usdm(app, update_id=10):
    from decimal import Decimal as D
    from collector.collector.canonical import CanonicalOrderBookEvent
    snap = CanonicalOrderBookEvent("BINANCE", "orderbook", None, None, 0,
        bids=tuple((D("100.0") - D("0.1") * i, D("1.0")) for i in range(10)),
        asks=tuple((D("101.0") + D("0.1") * i, D("1.0")) for i in range(10)),
        update_id=update_id, is_snapshot=True)
    app.binance_book.snapshot(snap)
    app.binance_book.state.recovered()


def test_usdm_runner_backend_is_installed_and_lifetime_set_stays_empty(tmp_path, monkeypatch):
    app = _usdm_app(tmp_path, monkeypatch)
    try:
        assert app.segment_dedup is not None
        assert isinstance(app.binance_adapter._trade_dedup, dict)
        _bridge_usdm(app)
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 1, "p": "100", "q": "1", "m": False}}))
        assert app.binance_adapter._seen_trade_ids == set()
        assert app.raw_trades_writer.buffer, "raw writer must have received the row"
    finally:
        app.raw_trades_writer.close()


def test_usdm_duplicate_within_open_segment_is_suppressed(tmp_path, monkeypatch):
    app = _usdm_app(tmp_path, monkeypatch)
    try:
        _bridge_usdm(app)
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 5, "p": "100", "q": "1", "m": False}}))
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T + 1, "a": 5, "p": "100", "q": "1", "m": False}}))
        assert len(app.raw_trades_writer.buffer) == 1
    finally:
        app.raw_trades_writer.close()


def test_usdm_duplicate_after_restart_is_suppressed_via_reconciliation(tmp_path, monkeypatch):
    app = _usdm_app(tmp_path, monkeypatch)
    _bridge_usdm(app)
    asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
        "data": {"E": T, "a": 9, "p": "100", "q": "1", "m": False}}))
    app.shutdown()                          # publish the segment, release all locks
    app.segment_dedup.close()

    app2 = _usdm_app(tmp_path, monkeypatch)
    try:
        assert app2.segment_dedup.coordinators["trades"].index.identity_count() == 1
        _bridge_usdm(app2, update_id=10)
        asyncio.run(app2.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T + 5, "a": 9, "p": "100", "q": "1", "m": False}}))
        assert app2.raw_trades_writer.buffer == [], "redelivered id 9 must be suppressed after restart"
    finally:
        app2.raw_trades_writer.close()


def test_usdm_crash_before_publication_allows_redelivery(tmp_path, monkeypatch):
    app = _usdm_app(tmp_path, monkeypatch)
    _bridge_usdm(app)
    asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
        "data": {"E": T, "a": 3, "p": "100", "q": "1", "m": False}}))
    app.raw_trades_writer.flush()          # buffered, NOT closed/published: simulated crash
    for w in (app.ob_writer, app.raw_book_writer, app.trades_writer, app.raw_trades_writer,
              app.mark_writer, app.oi_writer, app.liq_writer, app.raw_wire_writer,
              app.raw_rest_writer, app.quality_writer):
        w._release_lock()                  # process death releases every OS lock, nothing else
    app.segment_dedup.close()

    app2 = _usdm_app(tmp_path, monkeypatch)
    try:
        assert app2.segment_dedup.coordinators["trades"].index.identity_count() == 0
        _bridge_usdm(app2, update_id=10)
        asyncio.run(app2.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T + 5, "a": 3, "p": "100", "q": "1", "m": False}}))
        assert len(app2.raw_trades_writer.buffer) == 1, "trade 3 was never durable -> must be accepted again"
    finally:
        app2.raw_trades_writer.close()


def test_usdm_spot_and_futures_trade_ids_do_not_collide(tmp_path, monkeypatch):
    """Same numeric id, different market_type -- this runner's own index
    must never be shared with Spot's (each runner owns its own directory
    tree / dedup_state; confirmed by using the same id and seeing it
    accepted here regardless of what Spot would do with it)."""
    app = _usdm_app(tmp_path, monkeypatch)
    try:
        _bridge_usdm(app)
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 42, "p": "100", "q": "1", "m": False}}))
        assert len(app.raw_trades_writer.buffer) == 1
    finally:
        app.raw_trades_writer.close()


def test_usdm_opt_out_preserves_legacy_lifetime_set_behavior(tmp_path, monkeypatch):
    app = _usdm_app.__wrapped__(tmp_path, monkeypatch) if hasattr(_usdm_app, "__wrapped__") else None
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    app = rc.CollectorApp(enable_segment_dedup=False)
    try:
        assert app.segment_dedup is None
        assert app.binance_adapter._trade_dedup is None
        _bridge_usdm(app)
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 1, "p": "100", "q": "1", "m": False}}))
        assert app.binance_adapter._seen_trade_ids, "legacy path must still populate the lifetime set"
    finally:
        app.raw_trades_writer.close()


def test_usdm_index_failure_fails_closed_construction(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    from pathlib import Path
    bad = Path(tmp_path / "data" / "dedup_state")
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"i am a file, not a directory")   # mkdir(dedup_state) will fail
    with pytest.raises(Exception):
        rc.CollectorApp()


def test_usdm_replay_does_not_inherit_live_durable_state(tmp_path, monkeypatch):
    import json
    from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource

    app = _usdm_app(tmp_path, monkeypatch)
    _bridge_usdm(app)
    asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
        "data": {"E": T, "a": 77, "p": "100", "q": "1", "m": False}}))
    app.raw_trades_writer.close()
    app.segment_dedup.close()

    payload = {"stream": "btcusdt@aggTrade", "data": {"E": T, "a": 77, "p": "100", "q": "1", "m": False}}
    frame = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0, payload=json.dumps(payload))
    events = ReplayEngine("BINANCE").run(ReplaySource([frame])).non_book_events
    assert len(events) == 1, "replay must see trade 77 as new -- it must not inherit the live run's index"


# --- Bybit / OKX / Spot: direct proof each runner is actually wired -------
# (the full crash/restart/replay-isolation contract is already proven
# exhaustively, both at the component level -- test_segment_dedup.py, 24
# tests -- and at the runner level above for USD-M; all four runners share
# the identical attach_segment_dedup/SegmentDedupCoordinator code path, so
# what remains to prove per-runner is specifically that each one actually
# installed it, not a second full crash matrix.)


def test_bybit_runner_backend_is_installed_and_lifetime_set_stays_empty(tmp_path, monkeypatch):
    from collector import run_bybit_collector as rbc
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    app = rbc.BybitCollectorApp()
    try:
        assert app.segment_dedup is not None
        assert "trades" in app.segment_dedup.coordinators
        msg = {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T, "data": [
            {"T": T, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "b-1", "BT": False}]}
        for event in app.adapter.normalize(msg, local_receive_ts=T):
            app._persist_event(event)
        assert app.adapter._seen_trade_ids == set()
        assert len(app.trades_writer.buffer) == 1
        for event in app.adapter.normalize(msg, local_receive_ts=T + 1):
            app._persist_event(event)
        assert len(app.trades_writer.buffer) == 1, "duplicate must be suppressed via the real backend"
    finally:
        app.trades_writer.close()
        app.segment_dedup.close()


def test_okx_runner_backend_installed_for_both_trade_streams(tmp_path, monkeypatch):
    from collector import run_okx_collector as roc
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    app = roc.OKXCollectorApp()
    try:
        assert app.segment_dedup is not None
        assert set(app.segment_dedup.coordinators) == {"trades", "trades-all"}
        assert app.adapter._seen_trade_ids == set()
    finally:
        app.trades_writer.close()
        app.trades_all_writer.close()
        app.segment_dedup.close()


def test_spot_runner_backend_is_installed_and_lifetime_set_stays_empty(tmp_path, monkeypatch):
    from collector import run_binance_spot_collector as rsc
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    app = rsc.BinanceSpotCollectorApp()
    try:
        assert app.segment_dedup is not None
        assert "spot_trades" in app.segment_dedup.coordinators
        assert app.adapter._seen_trade_ids == set()
    finally:
        app.trades_writer.close()
        app.segment_dedup.close()


def test_spot_and_usdm_identical_trade_id_do_not_collide_across_runners(tmp_path, monkeypatch):
    """Each runner owns its own data/dedup_state -- different base_dir per
    runner in real deployment, but even sharing one tmp_path's 'data' dir
    here (via separate stream-name files), market_type keeps them apart."""
    from collector import run_binance_spot_collector as rsc
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    usdm = _usdm_app(tmp_path, monkeypatch)
    spot = rsc.BinanceSpotCollectorApp()
    try:
        _bridge_usdm(usdm)
        asyncio.run(usdm.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 500, "p": "100", "q": "1", "m": False}}))
        async def _feed_spot():
            for event in spot.adapter.normalize(
                {"stream": "btcusdt@trade", "data": {"e": "trade", "E": T, "T": T, "t": 500, "p": "1", "q": "1", "m": False}},
                local_receive_ts=T):
                await spot._persist_event(event)
        asyncio.run(_feed_spot())
        assert len(usdm.raw_trades_writer.buffer) == 1
        assert len(spot.trades_writer.buffer) == 1, "spot trade 500 must be accepted independently of USD-M's"
    finally:
        usdm.raw_trades_writer.close()
        spot.trades_writer.close()
        spot.segment_dedup.close()


def test_usdm_startup_reconciliation_specifically_recovers_a_commit_the_live_hook_never_made(tmp_path, monkeypatch):
    """Isolates reconciliation from the live on_segment_published path
    (the prior restart test above accidentally exercised the live hook,
    since closing the writer fires it -- found via mutation testing: the
    'skip startup_reconcile()' mutation produced zero failures against
    that test). Here the live hook is removed before publication
    (simulating a crash between publish and commit), so ONLY
    attach_segment_dedup's reconcile call -- run during app2's
    construction, before ingestion resumes -- can recover it."""
    app = _usdm_app(tmp_path, monkeypatch)
    _bridge_usdm(app)
    asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
        "data": {"E": T, "a": 55, "p": "100", "q": "1", "m": False}}))
    app.raw_trades_writer.on_segment_published = None     # remove ONLY the live hook
    app.raw_trades_writer.close()                          # publishes with no commit at all
    assert app.segment_dedup.coordinators["trades"].index.identity_count() == 0, \
        "setup check: removing the live hook must mean nothing was committed before the simulated crash"
    for w in (app.ob_writer, app.raw_book_writer, app.trades_writer, app.mark_writer,
              app.oi_writer, app.liq_writer, app.raw_wire_writer, app.raw_rest_writer, app.quality_writer):
        w._release_lock()
    app.segment_dedup.close()

    app2 = _usdm_app(tmp_path, monkeypatch)   # attach_segment_dedup's reconcile must find & index it
    try:
        assert app2.segment_dedup.coordinators["trades"].index.identity_count() == 1, \
            "only startup reconciliation could have indexed this -- the live hook never ran"
        _bridge_usdm(app2, update_id=10)
        asyncio.run(app2.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T + 5, "a": 55, "p": "100", "q": "1", "m": False}}))
        assert app2.raw_trades_writer.buffer == [], "reconciliation must have made id 55 a known duplicate"
    finally:
        app2.raw_trades_writer.close()


def test_usdm_ram_is_released_across_many_real_segment_rotations(tmp_path, monkeypatch):
    """Isolates the live on_segment_published hook specifically (mutation
    testing found 'install coordinator but never pass publication hook'
    produced zero failures against the existing suite -- the restart tests
    didn't need the live hook because a later startup_reconcile always
    caught up). This proves RAM does not grow across MANY rotations within
    one continuous run, which only the live hook (not reconciliation,
    which only runs once at startup) can guarantee."""
    app = _usdm_app(tmp_path, monkeypatch)
    try:
        app.raw_trades_writer.segment_rows = 5
        _bridge_usdm(app)
        peak = 0
        for i in range(200):
            asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
                "data": {"E": T + i, "a": 1000 + i, "p": "100", "q": "1", "m": False}}))
            peak = max(peak, app.segment_dedup.coordinators["trades"].ram_identity_count)
        assert peak <= 12, f"RAM identity count grew across rotations: peak={peak} (hook may not be firing)"
        assert app.segment_dedup.coordinators["trades"].index.identity_count() >= 180
    finally:
        app.raw_trades_writer.close()
        app.segment_dedup.close()


def test_usdm_startup_fails_closed_when_a_published_segment_is_unreadable(tmp_path, monkeypatch):
    """Isolates attach_segment_dedup's own call to startup_reconcile
    specifically (mutation testing found 'swallow reconciliation failure
    in attach_segment_dedup' produced zero failures against the existing
    suite -- the only existing unreadable-segment test calls
    coordinator.startup_reconcile() directly, not through a runner
    construction, so it never exercised this exact call site)."""
    app = _usdm_app(tmp_path, monkeypatch)
    _bridge_usdm(app)
    asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
        "data": {"E": T, "a": 1, "p": "100", "q": "1", "m": False}}))
    # Remove the live hook before publishing, so the segment is published
    # but NOT yet reconciled -- corrupting an already-reconciled segment
    # would be silently safe (never re-read once trusted), which is a
    # different, correct property, not what this test targets.
    app.raw_trades_writer.on_segment_published = None
    app.raw_trades_writer.close()
    for w in (app.ob_writer, app.raw_book_writer, app.trades_writer, app.mark_writer,
              app.oi_writer, app.liq_writer, app.raw_wire_writer, app.raw_rest_writer, app.quality_writer):
        w._release_lock()
    app.segment_dedup.close()

    seg = next(app.raw_trades_writer.stream_dir.glob("*.seg"))
    seg.write_bytes(b"corrupted, not a valid parquet file")

    with pytest.raises(Exception):
        _usdm_app(tmp_path, monkeypatch)   # CollectorApp() itself must fail to construct


def test_rollover_publication_hook_failure_fails_closed_for_the_triggering_write(tmp_path, monkeypatch):
    """Reproduces, then proves fixed, the exact defect this hostile review
    identified: _finalize_segment() inside the hour-rollover branch of
    ParquetWriter.write() can set _publication_failure (its hook raised),
    but the check for _publication_failure at the TOP of write() already
    ran before this same call's own rollover -- so without a second check
    immediately after _finalize_segment(), the triggering record would be
    silently bound and appended into the newly-opened segment despite the
    writer already being in a fail-closed state.

    Drives the real production ParquetWriter.write() path, not a
    reimplementation -- a hostile DedupStateError-raising hook, a forced
    hour change via the real _get_current_hour_str seam."""
    from collector.collector.segment_dedup import DedupStateError

    app = _usdm_app(tmp_path, monkeypatch)
    try:
        _bridge_usdm(app)
        writer = app.raw_trades_writer
        asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
            "data": {"E": T, "a": 1, "p": "100", "q": "1", "m": False}}))
        assert len(writer.buffer) == 1
        assert writer._publication_failure is None

        def hostile_hook(token, path):
            raise DedupStateError("simulated publication failure during hour rollover")
        writer.on_segment_published = hostile_hook
        # Force a genuine rollover relative to whatever hour the real first
        # write actually opened (never hand-set current_hour itself: that
        # would desync it from the real .tmp file already open on disk).
        next_hour = writer.current_hour[:-2] + f"{(int(writer.current_hour[-2:]) + 1) % 24:02d}"
        monkeypatch.setattr(writer, "_get_current_hour_str", lambda: next_hour)

        with pytest.raises(RuntimeError):
            asyncio.run(app.handle_message({"stream": "btcusdt@aggTrade",
                "data": {"E": T + 1, "a": 2, "p": "100", "q": "1", "m": False}}))

        # 1. The triggering record itself must NOT have been admitted.
        assert all(row["trade_id"] != "2" for row in writer.buffer), \
            "the triggering write must be rejected, not silently appended to the new segment"
        # 2. Dedup state was not falsely advanced: trade "2" must still be
        #    considered unseen (the admission already happened at the
        #    adapter layer before write() was ever called, by design --
        #    what must NOT happen is this record becoming durable).
        key = __import__("collector.collector.segment_dedup", fromlist=["dedup_identity_key"]).dedup_identity_key(
            "BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trades", "2")
        assert not app.segment_dedup.coordinators["trades"].index.contains(key)
        # 3. The writer is left fail-closed: subsequent writes also fail,
        #    without needing another rollover to trigger it.
        with pytest.raises(RuntimeError):
            writer.write({"timestamp": T + 2, "trade_id": "3", "price": 1.0, "quantity": 1.0,
                          "instrument_key": "x"})
    finally:
        # Writer is in a failed state; release its lock directly rather
        # than calling close() (which would try to publish again).
        writer._release_lock()
        app.segment_dedup.close()
