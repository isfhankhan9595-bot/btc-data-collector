"""P0-4: lifecycle proofs through the REAL runner wiring.

Every test builds a real runner (CollectorApp / BybitCollectorApp /
OKXCollectorApp / BinanceSpotCollectorApp) with the production default
(segment dedup enabled) on a real tmp_path and drives its real message
handler. Nothing here exercises SegmentDedupCoordinator in isolation.

Streams covered (one parametrized id each): ``usdm`` (Binance USD-M,
anchored on the RAW trade writer), ``bybit``, ``okx`` (trades), ``okx_all``
(trades-all), ``spot``.
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector import run_binance_spot_collector as rsc
from collector import run_bybit_collector as rbc
from collector import run_collector as rc
from collector import run_okx_collector as roc
from collector.collector.adapters.base import ExchangeAdapter
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.segment_dedup import (
    DedupStateError, SegmentDedupCoordinator, bind_arg, dedup_identity_key)

KINDS = ["usdm", "bybit", "okx", "okx_all", "spot"]

#: Expected identity domain per trade stream, written out literally (NOT derived
#: from production code) so a wrong exchange / market_type / stream / anchor in a
#: runner's StreamSpec fails here.
META = {
    "usdm":    dict(stream="trades",     exchange="BINANCE", market="linear_perpetual",
                    inst=BINANCE_USDM_BTCUSDT.key, writer="raw_trades_writer", multi=False, id_field="native_trade_id"),
    "bybit":   dict(stream="trades",     exchange="BYBIT",   market="linear_perpetual",
                    inst=BYBIT_LINEAR_BTCUSDT.key, writer="trades_writer", multi=True, id_field="trade_id"),
    "okx":     dict(stream="trades",     exchange="OKX",     market="linear_perpetual",
                    inst=OKX_SWAP_BTCUSDT.key, writer="trades_writer", multi=True, id_field="trade_id"),
    "okx_all": dict(stream="trades-all", exchange="OKX",     market="linear_perpetual",
                    inst=OKX_SWAP_BTCUSDT.key, writer="trades_all_writer", multi=True, id_field="trade_id"),
    "spot":    dict(stream="spot_trades", exchange="BINANCE", market="spot",
                    inst=BINANCE_SPOT_BTCUSDT.key, writer="trades_writer", multi=False, id_field="trade_id"),
}


def _now() -> int:
    return int(time.time() * 1000)


def _run(coro):
    return asyncio.run(coro)


def _bridge_usdm(app, update_id=10):
    from decimal import Decimal as D
    from collector.collector.canonical import CanonicalOrderBookEvent
    snap = CanonicalOrderBookEvent("BINANCE", "orderbook", None, None, 0,
        bids=tuple((D("100.0") - D("0.1") * i, D("1.0")) for i in range(10)),
        asks=tuple((D("101.0") + D("0.1") * i, D("1.0")) for i in range(10)),
        update_id=update_id, is_snapshot=True)
    app.binance_book.snapshot(snap)
    app.binance_book.state.recovered()


def _build(kind, tmp_path, monkeypatch):
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    if kind == "usdm":
        app = rc.CollectorApp()
        _bridge_usdm(app)
        return app
    return {"bybit": rbc.BybitCollectorApp, "okx": roc.OKXCollectorApp,
            "okx_all": roc.OKXCollectorApp, "spot": rsc.BinanceSpotCollectorApp}[kind]()


def _adapter(app):
    return getattr(app, "binance_adapter", None) or app.adapter


def _writer(kind, app):
    return getattr(app, META[kind]["writer"])


def _coord(kind, app):
    return app.segment_dedup.coordinators[META[kind]["stream"]]


def _feed(kind, app, ids, ts=None):
    """Drive the REAL handler. Multi-trade venues deliver all ids in ONE message."""
    ts = ts or _now()
    ids = list(ids)
    if kind == "usdm":
        for i in ids:
            _run(app.handle_message({"stream": "btcusdt@aggTrade",
                "data": {"E": ts, "a": int(i), "p": "100", "q": "1", "m": False}}))
    elif kind == "bybit":
        _run(app._handle_message({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": ts, "data": [
            {"T": ts, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": i, "BT": False} for i in ids]}, ts))
    elif kind in ("okx", "okx_all"):
        channel = "trades" if kind == "okx" else "trades-all"
        rows = [{"instId": "BTC-USDT-SWAP", "tradeId": i, "px": "100", "sz": "1", "side": "buy", "ts": str(ts)}
                for i in ids]
        if kind == "okx_all":
            for r in rows:
                r["source"] = "0"
        _run(app._handle_message({"arg": {"channel": channel, "instId": "BTC-USDT-SWAP"}, "data": rows}, ts))
    elif kind == "spot":
        for i in ids:
            _run(app._handle_message({"stream": "btcusdt@trade", "data": {
                "e": "trade", "E": ts, "T": ts, "t": int(i), "p": "1", "q": "1", "m": False}}, ts))


def _publish_all(app):
    for v in list(vars(app).values()):
        if isinstance(v, ParquetWriter):
            try:
                v.close()
            except Exception:  # noqa: BLE001 - a poisoned writer may refuse; locks are still released
                v._release_lock()


def _shutdown(app):
    _publish_all(app)
    app.segment_dedup.close()


def _ids(kind, app):
    return [str(r[META[kind]["id_field"]]) for r in _writer(kind, app).buffer]


# --- items 1, 2, 5, 8, 9: identity, bind, durability, restart ---------------


@pytest.mark.parametrize("kind", KINDS)
def test_live_identity_is_the_exact_five_part_key_bound_on_every_write_durable_and_restart_safe(kind, tmp_path, monkeypatch):
    m = META[kind]
    app = _build(kind, tmp_path, monkeypatch)
    coord, writer = _coord(kind, app), _writer(kind, app)
    noted, binds = [], []
    orig_note, orig_write = coord.note_written, writer.write
    coord.note_written = lambda key, token: (noted.append((key, token)), orig_note(key, token))[1]
    writer.write = lambda rec, **kw: (binds.append(kw.get("bind")), orig_write(rec, **kw))[1]

    _feed(kind, app, ["11", "12"])

    expected = {dedup_identity_key(m["exchange"], m["market"], m["inst"], m["stream"], i) for i in ("11", "12")}
    assert len(binds) == 2 and all(b is not None for b in binds), "every trade write must carry a bind callback"
    assert {k for k, _ in noted} == expected, "bind attributed a different identity than exchange+market+instrument+stream+id"
    assert {t for _, t in noted} == {writer.current_segment_token()}
    assert _adapter(app)._seen_trade_ids == set(), "lifetime set must stay empty with the backend installed"

    _publish_all(app)                                  # live publication hook fires
    assert coord.index.identity_count() == 2
    assert all(coord.index.contains(k) for k in expected), \
        "identities derived from the PUBLISHED ROW must equal the live admission keys"
    assert coord.ram_identity_count == 0, "published identities must be released from RAM"
    app.segment_dedup.close()

    app2 = _build(kind, tmp_path, monkeypatch)         # restart: reconciliation + fresh RAM
    try:
        assert _coord(kind, app2).index.identity_count() == 2
        _feed(kind, app2, ["11", "12", "13"], _now() + 10)
        assert _ids(kind, app2) == ["13"], "redelivered 11/12 must be suppressed after restart; 13 is genuinely new"
    finally:
        _shutdown(app2)


# --- item 7: immediate suppression while the segment is open ----------------


@pytest.mark.parametrize("kind", KINDS)
def test_duplicates_are_suppressed_immediately_while_the_segment_is_open(kind, tmp_path, monkeypatch):
    app = _build(kind, tmp_path, monkeypatch)
    try:
        _feed(kind, app, ["5"])
        _feed(kind, app, ["5"], _now() + 1)             # cross-message duplicate
        _feed(kind, app, ["7", "7"] if META[kind]["multi"] else ["7"], _now() + 2)
        if not META[kind]["multi"]:
            _feed(kind, app, ["7"], _now() + 3)         # single-trade venues: cross-message again
        assert sorted(_ids(kind, app)) == ["5", "7"]
        assert _coord(kind, app).index.identity_count() == 0, "nothing is published yet: RAM alone suppressed these"
    finally:
        _shutdown(app)


# --- item 13: OKX trades vs trades-all are separate identity domains --------


def test_okx_trades_and_trades_all_never_share_an_identity_domain(tmp_path, monkeypatch):
    app = _build("okx", tmp_path, monkeypatch)
    try:
        _feed("okx", app, ["900"])
        _feed("okx_all", app, ["900"])                  # SAME tradeId, other stream: must be accepted
        assert _ids("okx", app) == ["900"] and _ids("okx_all", app) == ["900"]
        _feed("okx", app, ["900"], _now() + 1)
        _feed("okx_all", app, ["900"], _now() + 1)
        assert _ids("okx", app) == ["900"] and _ids("okx_all", app) == ["900"]
        c_t, c_a = app.segment_dedup.coordinators["trades"], app.segment_dedup.coordinators["trades-all"]
        assert c_t is not c_a and c_t.index.path != c_a.index.path
        _publish_all(app)
        assert (c_t.index.identity_count(), c_a.index.identity_count()) == (1, 1)
        k_t = dedup_identity_key("OKX", "linear_perpetual", OKX_SWAP_BTCUSDT.key, "trades", "900")
        k_a = dedup_identity_key("OKX", "linear_perpetual", OKX_SWAP_BTCUSDT.key, "trades-all", "900")
        assert c_t.index.contains(k_t) and not c_t.index.contains(k_a)
        assert c_a.index.contains(k_a) and not c_a.index.contains(k_t)
    finally:
        app.segment_dedup.close()


# --- item 6: rollover cannot misattribute an identity -----------------------


@pytest.mark.parametrize("kind", KINDS)
def test_row_triggered_rollover_attributes_each_identity_to_the_segment_that_holds_its_row(kind, tmp_path, monkeypatch):
    app = _build(kind, tmp_path, monkeypatch)
    coord, writer = _coord(kind, app), _writer(kind, app)
    try:
        writer.segment_rows = 3
        for n in range(1, 9):
            _feed(kind, app, [str(100 + n)])
            assert coord._admitted == set(), "no admitted-but-unattributed identity may outlive a message"
            open_rows = writer.record_count + len(writer.buffer)
            assert len(coord._pending_index) == open_rows, \
                f"after trade {n}: RAM pending identities must be exactly the open segment's rows"
            assert set(coord._pending_by_token) <= {writer.current_segment_token()}
        segments = sorted(writer.stream_dir.glob("*.seg"))
        assert len(segments) == 2
        counts = [row[0] for row in coord.index._conn.execute(
            "SELECT identity_count FROM reconciled_segments ORDER BY segment_key")]
        assert counts == [3, 3] and coord.index.identity_count() == 6
        for seg in segments:                            # file contents == what its commit recorded
            assert pq.read_table(str(seg)).num_rows == 3
    finally:
        _shutdown(app)


@pytest.mark.parametrize("kind", KINDS)
def test_hour_rollover_binds_the_triggering_trade_to_the_new_segment(kind, tmp_path, monkeypatch):
    app = _build(kind, tmp_path, monkeypatch)
    coord, writer = _coord(kind, app), _writer(kind, app)
    try:
        _feed(kind, app, ["1"])
        _feed(kind, app, ["2"], _now() + 1)
        next_hour = writer.current_hour[:-2] + f"{(int(writer.current_hour[-2:]) + 1) % 24:02d}"
        monkeypatch.setattr(writer, "_get_current_hour_str", lambda: next_hour)
        _feed(kind, app, ["3"], _now() + 2)
        assert writer.current_segment_token()[0] == next_hour
        assert coord.index.identity_count() == 2, "old-hour segment must be published and indexed"
        assert list(coord._pending_by_token) == [writer.current_segment_token()]
        assert len(coord._pending_index) == 1, "ONLY the triggering trade is pending, under the NEW token"
        assert _ids(kind, app) == ["3"]
    finally:
        _shutdown(app)


# --- lifecycle: admitted -> raw accepted -> canonical rejects (USD-M) -------


def test_usdm_canonical_rejection_after_raw_accept_still_anchors_identity_in_the_raw_segment(tmp_path, monkeypatch):
    """Design intent (see run_collector.py comment): the RAW writer is the anchor
    because it receives every admitted trade BEFORE canonical validation. A
    canonically-rejected trade is therefore durable in the raw segment and its
    redelivery after a restart is suppressed -- it is not silently re-admitted."""
    app = _build("usdm", tmp_path, monkeypatch)
    coord = _coord("usdm", app)
    assert app.raw_trades_writer.on_segment_published == coord.on_segment_published
    assert [v for v in vars(app).values()
            if isinstance(v, ParquetWriter) and v.on_segment_published is not None] == [app.raw_trades_writer], \
        "the raw trade writer must be the ONLY writer with a dedup publication hook"

    _feed("usdm", app, ["50"])
    _feed("usdm", app, ["40"], _now() + 1)              # regressive id: canonical validator rejects
    big = str(2 ** 63)                                  # not lossless -> canonical skipped via `continue`
    _feed("usdm", app, [big], _now() + 2)

    assert [r["native_trade_id"] for r in app.raw_trades_writer.buffer] == ["50", "40", big]
    assert [r["trade_id"] for r in app.trades_writer.buffer] == [50], "canonical rejected 40 and the non-lossless id"
    assert app.stream_counters["trades"]["rejected"] == 2
    assert coord._admitted == set() and len(coord._pending_index) == 3
    _publish_all(app)
    assert coord.index.identity_count() == 3
    app.segment_dedup.close()

    app2 = _build("usdm", tmp_path, monkeypatch)
    try:
        _feed("usdm", app2, ["40", big], _now() + 20)
        assert app2.raw_trades_writer.buffer == [], "rejected-but-raw-durable trades stay suppressed after restart"
    finally:
        _shutdown(app2)


# --- lifecycle: handler exits early / raises --------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_handler_exit_before_the_write_never_leaves_an_admitted_identity_that_eats_a_redelivery(kind, tmp_path, monkeypatch):
    """Regression for the end_message() skip: normalize() admits identities up
    front; if the handler exits before they are written (writer raises), they
    must not remain in RAM and be mistaken for duplicates on redelivery."""
    app = _build(kind, tmp_path, monkeypatch)
    coord, writer = _coord(kind, app), _writer(kind, app)
    ids = ["21", "22"] if META[kind]["multi"] else ["21"]
    real, state = writer.write, {"n": 0}

    def flaky(rec, **kw):
        if state["n"] == 0:
            state["n"] = 1
            raise RuntimeError("simulated write failure before bind")
        return real(rec, **kw)
    writer.write = flaky
    try:
        with pytest.raises(RuntimeError):
            _feed(kind, app, ids)
        assert coord._admitted == set() and coord._pending_index == {}
        assert _ids(kind, app) == []
        _feed(kind, app, ids, _now() + 1)               # venue redelivers: nothing was ever written
        assert sorted(_ids(kind, app)) == ids, "unwritten trades must be accepted again, not dropped as duplicates"
    finally:
        _shutdown(app)


def test_transient_index_failure_mid_message_does_not_turn_unwritten_trades_into_duplicates(tmp_path, monkeypatch):
    app = _build("bybit", tmp_path, monkeypatch)
    coord = _coord("bybit", app)
    real, calls = coord.index.contains, {"n": 0}

    def flaky(key):
        calls["n"] += 1
        if calls["n"] == 2:
            raise DedupStateError("transient sqlite busy")
        return real(key)
    coord.index.contains = flaky
    try:
        with pytest.raises(DedupStateError):
            _feed("bybit", app, ["b1", "b2"])
        assert _ids("bybit", app) == [] and coord._admitted == set()
        coord.index.contains = real
        _feed("bybit", app, ["b1", "b2"], _now() + 1)
        assert sorted(_ids("bybit", app)) == ["b1", "b2"]
    finally:
        _shutdown(app)


# --- unidentified trades (trade_id is None) ---------------------------------


def test_trade_with_no_id_is_written_and_never_deduplicated_through_the_real_runner(tmp_path, monkeypatch):
    """The adapter deliberately keeps trade_id=None trades. bind_for() used to
    raise TypeError (len(None)) for them, silently losing the trade."""
    app = _build("bybit", tmp_path, monkeypatch)
    try:
        ts = _now()
        msg = {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": ts, "data": [
            {"T": ts, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "BT": False},
            {"T": ts, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "BT": False}]}
        events = app.adapter.normalize(msg, local_receive_ts=ts)
        assert [e.trade_id for e in events] == [None, None]
        assert all(bind_arg(app.segment_dedup, e) is None for e in events)
        _run(app._handle_message(msg, ts))
        assert [r["trade_id"] for r in app.trades_writer.buffer][-2:] == [None, None]
        _publish_all(app)
        assert _coord("bybit", app).index.identity_count() == 0
    finally:
        app.segment_dedup.close()


# --- items 10, 11, 12: fail-closed behaviours through the runner ------------


@pytest.mark.parametrize("kind", KINDS)
def test_sqlite_failure_at_runtime_fails_closed_without_writing_or_admitting(kind, tmp_path, monkeypatch):
    app = _build(kind, tmp_path, monkeypatch)
    coord = _coord(kind, app)
    try:
        coord.index._conn.close()                      # the durable state is now unreachable
        if kind == "usdm":
            # F5: the Binance USD-M handler boundary classifies a dedup failure
            # (typed fatal, component "dedup") and ISOLATES the trades route
            # instead of letting it surface as an ordinary processing error.
            _feed(kind, app, ["61"])
            record = app.isolated_routes["trades"]
            assert (record.component, record.stream, record.verdict) == ("dedup", "trades", "isolate_route")
            assert app.terminal_failure is None, "a derived dedup failure must not kill the process"
        else:
            with pytest.raises(DedupStateError):
                _feed(kind, app, ["61"])
        assert _ids(kind, app) == [], "an unreadable index must not be mapped to 'new'"
        assert coord._admitted == set() and coord._pending_index == {}
    finally:
        _publish_all(app)


@pytest.mark.parametrize("kind", KINDS)
def test_unreadable_published_segment_aborts_runner_construction(kind, tmp_path, monkeypatch):
    app = _build(kind, tmp_path, monkeypatch)
    writer = _writer(kind, app)
    _feed(kind, app, ["71"])
    writer.on_segment_published = None                 # publish WITHOUT a live commit: only reconcile can see it
    _publish_all(app)
    app.segment_dedup.close()
    next(writer.stream_dir.glob("*.seg")).write_bytes(b"not a parquet file")
    with pytest.raises(DedupStateError):
        _build(kind, tmp_path, monkeypatch)


@pytest.mark.parametrize("kind", KINDS)
def test_publication_hook_failure_cannot_let_a_later_trade_continue_and_restart_restores_state(kind, tmp_path, monkeypatch):
    m = META[kind]
    app = _build(kind, tmp_path, monkeypatch)
    coord, writer = _coord(kind, app), _writer(kind, app)
    writer.segment_rows = 2

    def hostile(token, path):
        raise DedupStateError("simulated index commit failure")
    writer.on_segment_published = hostile

    _feed(kind, app, ["31", "32"])                     # 2nd row closes+publishes the segment; hook fails
    assert writer._publication_failure is not None
    for attempt in ("33", "34"):                       # never continues silently, however often retried
        if kind == "usdm":
            # F5: refused by ISOLATION (typed fatal classified at the handler
            # boundary, then the route is short-circuited), not by an exception.
            _feed(kind, app, [attempt], _now() + 5)
        else:
            with pytest.raises(RuntimeError):
                _feed(kind, app, [attempt], _now() + 5)
    if kind == "usdm":
        assert "trades" in app.isolated_routes and app.terminal_failure is None
    assert writer.buffer == [] and coord._admitted == set()
    k33 = dedup_identity_key(m["exchange"], m["market"], m["inst"], m["stream"], "33")
    assert not coord.index.contains(k33) and k33 not in coord._pending_index
    assert len(coord._pending_index) == 2, "31/32 are durable on disk but not indexed: RAM keeps suppressing them"
    _publish_all(app)
    app.segment_dedup.close()

    app2 = _build(kind, tmp_path, monkeypatch)         # restart: reconcile finishes what the hook could not
    try:
        assert _coord(kind, app2).index.identity_count() == 2
        _feed(kind, app2, ["31", "32", "33"], _now() + 20)
        assert _ids(kind, app2) == ["33"]
    finally:
        _shutdown(app2)


# --- item 3 / 4: construction order -----------------------------------------


@pytest.mark.parametrize("kind", ["usdm", "bybit", "okx", "spot"])
def test_reconcile_runs_after_the_hook_is_installed_and_before_the_adapter_backend_and_any_websocket_client(
        kind, tmp_path, monkeypatch):
    from collector.collector import websocket_client as wsc
    events, created = [], []

    real_init = ParquetWriter.__init__
    monkeypatch.setattr(ParquetWriter, "__init__",
                        lambda self, *a, **k: (real_init(self, *a, **k), created.append(self))[0])
    real_rec = SegmentDedupCoordinator.startup_reconcile

    def spy_reconcile(self, stream_dir):
        w = next(w for w in created if Path(w.stream_dir) == Path(stream_dir))
        events.append(("reconcile", w.on_segment_published == self.on_segment_published))
        return real_rec(self, stream_dir)
    monkeypatch.setattr(SegmentDedupCoordinator, "startup_reconcile", spy_reconcile)
    real_set = ExchangeAdapter.set_trade_dedup
    monkeypatch.setattr(ExchangeAdapter, "set_trade_dedup",
                        lambda self, b: (events.append(("install", None)), real_set(self, b))[1])
    real_ws = wsc.WebSocketClient.__init__
    monkeypatch.setattr(wsc.WebSocketClient, "__init__",
                        lambda self, *a, **k: (events.append(("ws_client", None)), real_ws(self, *a, **k))[1])

    app = _build(kind, tmp_path, monkeypatch)
    try:
        names = [e[0] for e in events]
        assert names.count("install") == 1 and "reconcile" in names
        assert all(hook for name, hook in events if name == "reconcile"), "hook must be installed BEFORE reconcile"
        last_reconcile = max(i for i, n in enumerate(names) if n == "reconcile")
        assert last_reconcile < names.index("install"), "adapter backend is installed last"
        for i, n in enumerate(names):
            if n == "ws_client":
                assert i > names.index("install"), "no websocket client may exist before dedup is fully established"
    finally:
        _shutdown(app)


# --- item 15: replay never inherits live dedup state -------------------------


def test_replay_adapters_are_unaffected_by_live_runners_in_the_same_process(tmp_path, monkeypatch):
    from collector.collector.replay import _ADAPTER_CLASSES, ReplayEngine
    app = _build("bybit", tmp_path, monkeypatch)
    try:
        assert app.adapter._trade_dedup is not None
        for venue, cls in _ADAPTER_CLASSES.items():
            assert cls()._trade_dedup is None, f"{venue}: fresh adapters must default to no durable backend"
            assert ReplayEngine(venue).adapter._trade_dedup is None, f"{venue}: replay must not receive a live backend"
    finally:
        _shutdown(app)
