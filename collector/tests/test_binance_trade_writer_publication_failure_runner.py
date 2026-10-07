"""P0: Binance USD-M trade writers report storage failure through the DURABLE
quality path, via the REAL ``CollectorApp`` wiring.

``trades`` (canonical) and ``binance_trades_raw`` (the dedup anchor) used to be
constructed without a ``quality_event_sink``: a failed or crashed trade segment
produced a log line and nothing durable. Nothing here re-implements the runner.
"""
from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from collector.collector import parquet_writer as pw_module
from collector.collector.segment_dedup import dedup_identity_key
from collector.tests.test_p0_4_runner_lifecycle import (
    META, _build, _coord, _feed, _now, _publish_all)
from collector.tests.test_parquet_writer_publication_failure import _fail_replace_to

WRITERS = [("raw_trades_writer", "binance_trades_raw"), ("trades_writer", "trades")]


def _durable_quality_rows(app):
    """Publish the quality writer's open segment and read every durable row."""
    app.quality_writer.publish_open_segment()
    rows = []
    for seg in sorted(app.quality_writer.stream_dir.glob("*.seg")):
        rows += pq.read_table(seg).to_pylist()
    return rows


def test_trade_writers_are_wired_to_the_durable_quality_sink(tmp_path, monkeypatch):
    app = _build("usdm", tmp_path, monkeypatch)
    try:
        for name, _ in WRITERS:
            assert getattr(app, name).quality_event_sink == app._persist_quality_event, name
        assert app.quality_writer.quality_event_sink is None, "quality_writer must not report into itself"
    finally:
        _publish_all(app)
        app.segment_dedup.close()


@pytest.mark.parametrize("attr,stream", WRITERS)
def test_trade_segment_publication_failure_is_durable_fails_closed_and_restart_recovers(
        attr, stream, tmp_path, monkeypatch):
    app = _build("usdm", tmp_path, monkeypatch)
    writer, coord = getattr(app, attr), _coord("usdm", app)
    _feed("usdm", app, ["1", "2", "3"])
    writer.flush()
    assert writer.record_count == 3

    with monkeypatch.context() as m:                   # fault scoped to the injection only
        _fail_replace_to(m, ".seg")
        with pytest.raises(OSError):
            writer.publish_open_segment()

    # fail closed through the real handler: the trade is refused, never buffered
    with pytest.raises(RuntimeError):
        _feed("usdm", app, ["4"], _now() + 5)
    assert writer.buffer == []
    meta = META["usdm"]
    k4 = dedup_identity_key(meta["exchange"], meta["market"], meta["inst"], meta["stream"], "4")
    assert not coord.index.contains(k4)
    if attr == "raw_trades_writer":
        # The raw writer IS the dedup anchor: a refused trade must leave no identity behind.
        assert k4 not in coord._pending_index
    # (When only the canonical writer failed the healthy raw anchor legitimately keeps capturing.)
    assert list(writer.stream_dir.glob("*.seg")) == []

    rows = _durable_quality_rows(app)                  # DURABLE, not just logged
    failed = [r for r in rows if r["event_type"] == "STORAGE_PUBLICATION_FAILED" and r["stream"] == stream]
    assert len(failed) == 1 and failed[0]["exchange"] == "BINANCE" and "rename" in failed[0]["reason"]
    assert not [r for r in rows if r["event_type"] == "DATA_DROP" and r["stream"] == stream], \
        "flushed rows are accounted once, by the restart's orphan recovery"

    _publish_all(app)                                  # shutdown with the failure outstanding
    app.segment_dedup.close()

    app2 = _build("usdm", tmp_path, monkeypatch)       # restart: existing orphan-recovery contract
    try:
        rows2 = _durable_quality_rows(app2)
        drops = [r for r in rows2 if r["event_type"] == "DATA_DROP" and r["stream"] == stream]
        assert len(drops) == 1 and drops[0]["rows_lost"] == "3"
        assert drops[0]["reason"] == "crashed_segment_discarded" and drops[0]["exchange"] == "BINANCE"
        if attr == "raw_trades_writer":                 # nothing published => no identity in the index
            coord2 = _coord("usdm", app2)
            for tid in ("1", "2", "3", "4"):
                key = dedup_identity_key(meta["exchange"], meta["market"], meta["inst"], meta["stream"], tid)
                assert not coord2.index.contains(key), tid
    finally:
        _publish_all(app2)
        app2.segment_dedup.close()
