"""P0-1 closure audit: writer-close failures during shutdown must not cost
every OTHER writer's still-buffered data.

Before this fix, CollectorApp.shutdown() (and the three async runners' own
shutdown) called each writer's close() with no error handling. Any real
disk-full/fsync/rename failure -- ParquetWriter._close_segment's core publish
steps (flush, the underlying pyarrow writer's close, fsync, os.replace) have
no failure handling of their own, only the metadata sidecar step does -- would
propagate straight out of shutdown() and abort every writer still left in the
list, including quality_writer itself.

These tests drive the real CollectorApp / BybitCollectorApp against real
ParquetWriter instances backed by real files, injecting a real OSError at the
exact point _close_segment would hit a genuine disk failure (os.replace) --
not a mock of the runner's own logic.
"""
from __future__ import annotations

import os

import pytest

import run_bybit_collector
import run_collector
from collector.collector.parquet_writer import ParquetWriter


def _real_app(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)
    return run_collector.CollectorApp()


def _write_one_row(writer: ParquetWriter, ts: int) -> None:
    """Push a real row into a real writer's buffer so close() has genuine
    work to do (flush + publish), not the empty-buffer early-return path."""
    row = {field.name: None for field in writer.schema}
    for key, value in (("timestamp", ts), ("local_receive_ts", ts), ("exchange", "BINANCE")):
        if key in row:
            row[key] = value
    writer.write(row)


def test_one_writers_close_failure_does_not_abort_the_rest_of_shutdown(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    for writer in (app.ob_writer, app.raw_book_writer, app.trades_writer, app.raw_trades_writer,
                  app.mark_writer, app.oi_writer, app.liq_writer):
        _write_one_row(writer, 1_780_000_000_000)

    real_replace = os.replace
    failed_once = {"done": False}

    def flaky_replace(src, dst, *a, **kw):
        # Fail exactly the ob_writer's publish (the first writer in the
        # shutdown order with real buffered data) -- a genuine OSError at
        # the exact rename step _close_segment performs with no handling.
        if not failed_once["done"] and str(app.ob_writer.stream_name) in str(dst):
            failed_once["done"] = True
            raise OSError("ENOSPC: simulated disk full during segment rename")
        return real_replace(src, dst, *a, **kw)

    monkeypatch.setattr(os, "replace", flaky_replace)
    app.shutdown()   # must not raise

    assert failed_once["done"], "the injected failure was never reached -- test did not exercise the real path"
    # Every OTHER writer with buffered data must still have been finalized:
    # once closed, a ParquetWriter's own .writer handle is released.
    assert app.trades_writer.writer is None
    assert app.mark_writer.writer is None
    assert app.liq_writer.writer is None
    assert app.quality_writer.writer is None, "quality_writer itself must still be closed last"


def test_the_close_failure_is_reported_as_a_quality_event_not_silently_lost(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _write_one_row(app.ob_writer, 1_780_000_000_000)

    def raising_replace(*a, **kw):
        raise OSError("ENOSPC: simulated disk full")
    monkeypatch.setattr(os, "replace", raising_replace)

    reported = []
    original = app._persist_quality_event
    def spy(event):
        reported.append(event)
        return original(event)
    monkeypatch.setattr(app, "_persist_quality_event", spy)

    app.shutdown()

    failures = [e for e in reported if str(e.get("reason", "")).startswith("storage_shutdown_close_failed")]
    assert len(failures) >= 1
    assert failures[0]["event_type"] == "ERROR"
    assert "OSError" in failures[0]["reason"]


def test_a_failing_writer_does_not_prevent_shutdown_from_completing_or_double_running(tmp_path, monkeypatch):
    """shutdown() is also guarded by self._closed; a raised exception inside
    the old, unguarded loop could in principle have left _closed=True with
    the rest of the sequence skipped. Confirms the whole method returns
    normally and is idempotent afterward."""
    app = _real_app(tmp_path, monkeypatch)

    # quality_writer has segment_rows=1, so writing even one row triggers an
    # immediate segment rename INSIDE write() itself -- must happen before
    # the failure is installed, exactly like the passing test above.
    _write_one_row(app.ob_writer, 1_780_000_000_000)

    real_replace = os.replace

    def raising_replace(src, dst, *a, **kw):
        # Only the final segment rename (".seg.tmp" -> ".seg"), not the
        # counter sidecar's own replace on every write() -- that would fail
        # before shutdown() is even reached.
        if str(src).endswith(".seg.tmp"):
            raise OSError("disk full")
        return real_replace(src, dst, *a, **kw)
    monkeypatch.setattr(os, "replace", raising_replace)

    app.shutdown()
    assert app._closed is True
    app.shutdown()   # idempotent: must not raise or attempt to close again


def test_bybit_runner_quality_writer_closes_last_so_other_failures_are_reportable(tmp_path):
    """The three async runners previously closed quality_writer FIRST, which
    would have made reporting any OTHER writer's failure impossible even
    with a try/except (the sink itself already gone). Confirms the reorder:
    quality_writer must be the last writer this method attempts to close."""
    app = run_bybit_collector.BybitCollectorApp(data_dir=str(tmp_path))
    try:
        import ast
        import inspect
        import textwrap
        source = textwrap.dedent(inspect.getsource(run_bybit_collector.BybitCollectorApp.shutdown))
        tree = ast.parse(source)
        calls = [n.args[1].value for n in ast.walk(tree)
                if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "_close_writer_reporting_failure"
                and isinstance(n.args[1], ast.Constant)]
        assert calls[-1] == "quality_writer", f"quality_writer must close last, got order: {calls}"
    finally:
        for w in (app.quality_writer, app.raw_wire_writer, app.ob_writer, app.trades_writer,
                 app.mark_writer, app.oi_writer, app.liq_writer):
            try:
                w.close()
            except Exception:
                pass
