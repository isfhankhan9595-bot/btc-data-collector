"""Phase 2: what ``asyncio.run`` teardown does with a cancellation-resistant task, and what the runners do about it.

Reproduced on Python 3.11.15 and 3.12.3 (identical outcomes; see docs/EVENT_LOOP_TEARDOWN.md):

* ``asyncio.run`` cancels what is left and then waits for it WITHOUT A LIMIT. A task that swallows
  ``CancelledError`` makes it hang; meanwhile the runners' SIGTERM/SIGINT handlers only set an event that is
  already set, so only SIGKILL ends the process and the exit status the supervisor latched is lost.
* a writer whose ``close()`` raised (healthy writer, failed final publication) did not change the exit status:
  the process exited 0 with the buffered rows stranded in ``.tmp``;
* ``run_okx_capture`` closed its quality writer beneath a live ingest worker.

Every potentially hanging scenario runs in a CHILD process in its own process group, with a hard deadline, in
a throw-away directory with a scrubbed environment: a hang can neither stall this suite nor touch the real
repository or data. The deadline is only an upper bound; nothing here is timing-sensitive.

What these tests do NOT claim: that a stuck task can be stopped (it cannot), or that storage held open beneath
one is finalized (it is not: it is preserved, the failure is reported and the exit status is non-zero).
"""
import ast
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

pytestmark = pytest.mark.skipif(os.name != "posix", reason="process groups / real SIGTERM are POSIX-only")

TESTS = Path(__file__).resolve().parent
COLLECTOR = TESTS.parent
CHILD = TESTS / "_teardown_child.py"
UPPER_BOUND_S = 60.0                         # reached only when something hangs
VENUES = ["bybit", "spot", "okx"]


class Run:
    def __init__(self, rc, timed_out, out, err, elapsed):
        self.rc, self.timed_out, self.err, self.elapsed = rc, timed_out, err, elapsed
        self.events = [json.loads(line[3:]) for line in out.splitlines() if line.startswith("EV ")]

    def of(self, kind):
        return [e for e in self.events if e["ev"] == kind]

    def closes_beneath_live_worker(self):
        """Writers closed while the worker was running, excluding the receive-loop-only writers (by design)."""
        return [e["writer"] for e in self.of("close") if e["worker_running"] and not e["ingress_only"]]


def _spawn(argv, cwd, deadline_s=UPPER_BOUND_S):
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "LANG", "LC_ALL")}
    env.update(PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1")     # no tokens, no alert credentials
    t0 = time.monotonic()
    proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True, text=True)
    timed_out = False
    try:
        out, err = proc.communicate(timeout=deadline_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(proc.pid, signal.SIGKILL)          # the child and every descendant; never the test process
        out, err = proc.communicate()
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return Run(proc.returncode, timed_out, out, err, time.monotonic() - t0)


def run_runner(tmp_path, venue, *extra):
    data = tmp_path / "data"
    data.mkdir()
    run = _spawn([sys.executable, str(CHILD), venue, str(data), *extra], cwd=tmp_path)
    run.data = data
    return run


def tmp_files(data, stream=""):
    return [p for p in data.rglob("*.tmp") if stream in str(p)]


def published_rows(data, stream):
    total = 0
    for p in data.rglob("*"):
        if p.suffix in (".seg", ".parquet") and stream in str(p):
            total += pq.read_metadata(p).num_rows
    return total


# --------------------------------------------------------------------------------------------- runners
@pytest.mark.parametrize("venue", VENUES)
def test_control_a_cancellable_worker_exits_zero_and_every_row_is_published(tmp_path, venue):
    run = run_runner(tmp_path, venue, "--resist", "none")
    assert not run.timed_out and run.rc == 0, (run.rc, run.err[-400:])
    assert not tmp_files(run.data) and published_rows(run.data, "trades") == 3
    assert not run.of("write_after_close")


@pytest.mark.parametrize("venue", VENUES)
def test_a_worker_that_never_ends_does_not_hang_the_process_and_exits_nonzero(tmp_path, venue):
    """Defect before the fix: ``asyncio.run`` never returned, the harness had to SIGKILL (status -9)."""
    run = run_runner(tmp_path, venue, "--resist", "forever", "--teardown", "1.0")
    assert not run.timed_out, "event-loop teardown hung on a cancellation-resistant worker"
    assert run.rc == 1 and run.rc != -signal.SIGKILL
    assert not run.of("returned") or run.of("returned")[0]["rc"] == 1
    # the safety invariant: nothing the worker can reach was closed beneath it, and nothing was written late
    assert run.closes_beneath_live_worker() == []
    assert not run.of("write_after_close")
    # ... and its storage was preserved, not finalized (unpublished rows stay in the .tmp, not lost silently)
    assert tmp_files(run.data, "trades") and published_rows(run.data, "trades") == 0
    assert b"event_loop_teardown_deadline_exceeded" in run.err.encode()      # a visible, named failure


@pytest.mark.parametrize("venue", VENUES)
def test_b_worker_ending_after_the_first_shutdown_failed_is_still_finalized(tmp_path, venue):
    run = run_runner(tmp_path, venue, "--resist", "finite", "--resist-s", "1.0", "--teardown", "10")
    assert not run.timed_out and run.rc == 1                    # a late close is not a clean shutdown
    assert run.closes_beneath_live_worker() == []
    assert not tmp_files(run.data) and published_rows(run.data, "trades") == 3


@pytest.mark.parametrize("venue", VENUES)
def test_c_worker_outliving_the_cancellation_deadline_but_inside_the_teardown_bound_is_finalized(tmp_path, venue):
    run = run_runner(tmp_path, venue, "--resist", "finite", "--resist-s", "2.5", "--teardown", "10")
    assert not run.timed_out and run.rc == 1
    assert not tmp_files(run.data) and published_rows(run.data, "trades") == 3


@pytest.mark.parametrize("venue", VENUES)
def test_c_worker_outliving_the_teardown_bound_is_not_waited_for_and_not_finalized(tmp_path, venue):
    """The explicit trade-off: after the bound the process stops waiting; the chance of a later late-finalize is
    given up, storage stays open/unfinalized, and the status is non-zero."""
    run = run_runner(tmp_path, venue, "--resist", "finite", "--resist-s", "30", "--teardown", "1.0")
    assert not run.timed_out and run.rc == 1
    assert run.closes_beneath_live_worker() == [] and tmp_files(run.data, "trades")


@pytest.mark.parametrize("venue", VENUES)
def test_f_worker_that_ends_on_the_loops_own_teardown_cancel_is_finalized_by_its_callback(tmp_path, venue):
    run = run_runner(tmp_path, venue, "--resist", "second_cancel", "--teardown", "10")
    assert not run.timed_out and run.rc == 1
    assert len(run.of("worker_cancel")) == 2                    # supervisor's request + the teardown's
    assert run.closes_beneath_live_worker() == []
    assert not tmp_files(run.data) and published_rows(run.data, "trades") == 3


@pytest.mark.parametrize("venue", VENUES)
def test_e_failed_writer_finalization_is_not_reported_as_success(tmp_path, venue):
    """Defect before the fix: the close raised, the rows stayed in ``.tmp`` and the process exited 0."""
    run = run_runner(tmp_path, venue, "--resist", "none", "--close-fail", "trades_writer")
    assert not run.timed_out
    assert run.rc == 1, "a writer that could not publish its final segment must not exit 0"
    assert tmp_files(run.data, "trades") and published_rows(run.data, "trades") == 0
    assert published_rows(run.data, "quality") >= 1             # the failure itself is reported in the data


# --------------------------------------------------------------------------------------- OKX capture-only
def test_okx_capture_healthy_worker_exits_zero(tmp_path):
    run = run_runner(tmp_path, "okx_capture", "--resist", "none")
    assert not run.timed_out and run.rc == 0, (run.rc, run.err[-400:])
    assert not tmp_files(run.data)


def test_okx_capture_does_not_close_the_quality_writer_beneath_a_live_worker(tmp_path):
    """Defect before the fix: ``close()`` ran in ``finally`` with the worker alive, then asyncio.run hung."""
    run = run_runner(tmp_path, "okx_capture", "--resist", "forever", "--teardown", "1.0")
    assert not run.timed_out, "capture runner hung in event-loop teardown"
    assert run.rc not in (0, -signal.SIGKILL)
    assert "quality_writer" not in run.closes_beneath_live_worker()
    assert run.closes_beneath_live_worker() == []
    assert tmp_files(run.data, "quality")                       # preserved for restart recovery


# --------------------------------------------------------------------------------- run_bounded, bare asyncio
_BARE = r'''
import asyncio, json, sys, time
sys.path[:0] = [sys.argv[1], sys.argv[2]]
from collector.collector.loop_teardown import run_bounded, teardown_exit_status
MODE = sys.argv[3]
async def stubborn(end_after):
    end = None
    while True:
        try: await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            if end is None: end = time.monotonic() + end_after
        if end is not None and time.monotonic() >= end: return
async def main():
    if MODE == "cancellable":
        asyncio.ensure_future(asyncio.sleep(3600))
    elif MODE == "finite":
        asyncio.ensure_future(stubborn(0.2))
    elif MODE == "forever":
        asyncio.ensure_future(stubborn(1e9))
    elif MODE == "raises":
        asyncio.ensure_future(stubborn(1e9)); raise ValueError("boom")
    await asyncio.sleep(0.05)
    return 0
try:
    result, stuck = run_bounded(main(), teardown_deadline_s=0.6)
    print(json.dumps({"result": result, "stuck": stuck, "status": teardown_exit_status(result, stuck)}), flush=True)
except ValueError as e:
    print(json.dumps({"raised": str(e)}), flush=True)
'''


def _bare_json(tmp_path, mode):
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME")}
    proc = subprocess.Popen([sys.executable, "-c", _BARE, str(COLLECTOR.parent), str(COLLECTOR), mode], cwd=tmp_path,
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, text=True)
    try:
        out, err = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        out, err = proc.communicate()
        pytest.fail("run_bounded hung")
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return proc.returncode, (json.loads(out.strip().splitlines()[-1]) if out.strip() else None), err


def test_run_bounded_closes_a_loop_whose_tasks_all_end(tmp_path):
    rc, out, _ = _bare_json(tmp_path, "cancellable")
    assert rc == 0 and out == {"result": 0, "stuck": [], "status": 0}


def test_run_bounded_waits_for_a_task_that_ends_inside_the_deadline(tmp_path):
    rc, out, _ = _bare_json(tmp_path, "finite")
    assert rc == 0 and out["stuck"] == [] and out["status"] == 0


def test_run_bounded_returns_instead_of_hanging_and_never_reports_success_for_a_stuck_task(tmp_path):
    rc, out, err = _bare_json(tmp_path, "forever")
    assert rc == 0                                              # the helper returned (asyncio.run would hang)
    assert out["stuck"] == ["stubborn"] and out["result"] == 0 and out["status"] == 1
    assert "event_loop_teardown_deadline_exceeded" in err


def test_run_bounded_re_raises_what_the_main_coroutine_raised_after_the_bounded_teardown(tmp_path):
    rc, out, _ = _bare_json(tmp_path, "raises")
    assert out == {"raised": "boom"}


def test_teardown_exit_status_never_turns_a_stuck_teardown_into_zero():
    from collector.collector.loop_teardown import teardown_exit_status
    assert teardown_exit_status(0, []) == 0
    assert teardown_exit_status(0, ["t"]) == 1
    assert teardown_exit_status(70, ["t"]) == 70            # a worse, already-latched status keeps priority
    assert teardown_exit_status(69, []) == 69


# ---------------------------------------------------------------------------------- entry points + USD-M
@pytest.mark.parametrize("name", ["run_bybit_collector.py", "run_binance_spot_collector.py", "run_okx_collector.py",
                                  "run_okx_capture.py", "run_collector.py"])
def test_no_entry_point_uses_the_unbounded_asyncio_run(name):
    tree = ast.parse((COLLECTOR / name).read_text())
    runs = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "run" and isinstance(n.func.value, ast.Name) and n.func.value.id == "asyncio"]
    assert not runs, f"{name}: asyncio.run has an unbounded teardown; use loop_teardown.run_bounded"
    assert any(isinstance(n, ast.Name) and n.id == "run_bounded" for n in ast.walk(tree))


def test_writer_already_failed_distinguishes_a_new_close_failure_from_a_repeat():
    from collector.collector.standalone_failure_policy import writer_already_failed

    class Healthy:
        def failure_snapshot(self): return None

    class Failed:
        def failure_snapshot(self): return object()

    class Odd:
        pass
    assert not writer_already_failed(Healthy()) and writer_already_failed(Failed()) and not writer_already_failed(Odd())


def test_usd_m_failed_writer_close_latches_a_nonzero_exit(tmp_path, monkeypatch):
    import run_collector
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    app = run_collector.CollectorApp()

    def failing_close():
        raise OSError(28, "no space left on device")
    app.trades_writer.close = failing_close
    app.shutdown()
    assert app.shutdown_failure == "writer_close_failed:trades_writer"
    assert app.exit_code == 1


# ---------------------------------------------------- late finalization waits for EVERY live storage user
# Mutation evidence (docs/EVENT_LOOP_TEARDOWN.md): replacing a runner's ``_storage_users_stopped`` by ``True``
# survived the single-worker tests, because with one live user "the last user ended" and "the worker ended"
# coincide. A worker can end while ``start()``'s tail is still running (it emits quality events after the
# close), so the late callback of one user must not close what another still uses; and a client that never
# started has no user at all, so its writers must still close.
def _runner_apps(tmp_path):
    import asyncio  # noqa: F401  (Event construction needs no running loop on 3.10+)
    import run_binance_spot_collector as spot
    import run_bybit_collector as bybit
    import run_okx_capture as capture
    import run_okx_collector as okx
    from collector.collector.adapters.okx import OKXAdapter

    def new(name, factory):
        d = tmp_path / name
        d.mkdir()
        return name, factory(str(d))

    return [
        new("bybit", lambda d: bybit.BybitCollectorApp(data_dir=d)),
        new("spot", lambda d: spot.BinanceSpotCollectorApp(data_dir=d)),
        new("okx", lambda d: okx.OKXCollectorApp(data_dir=d)),
        new("okx_capture", lambda d: capture.OKXCaptureApp(sorted(OKXAdapter().declared_channels()), "BTC-USDT-SWAP", d,
                                                           "wss://fake.invalid")),
    ]


def _client(app):
    return app.capture.client if hasattr(app, "capture") else app.client


def _spy_writers(app):
    closes = []
    for key, writer in list(vars(app).items()):
        if key.endswith("_writer") and hasattr(writer, "close"):
            real = writer.close
            writer.close = (lambda key=key, real=real: (closes.append(key), real())[1])
    return closes


@pytest.mark.parametrize("which", ["bybit", "spot", "okx", "okx_capture"])
def test_late_finalization_does_not_close_while_start_tail_is_still_live_then_closes_once(tmp_path, which):
    import asyncio
    app = dict(_runner_apps(tmp_path))[which]
    client, closes = _client(app), _spy_writers(app)
    client._lifecycle_started, client._lifecycle_finished = True, asyncio.Event()      # start() tail still running
    assert client.lifecycle_idle is False
    assert app._storage_users_stopped() is False
    assert app._late_finalizer.retry() is False and closes == []     # another user is alive: nothing closes
    client._lifecycle_finished.set()                                  # the last user ends
    assert app._storage_users_stopped() is True
    assert app._late_finalizer.retry() is True
    once = sorted(closes)
    assert once and len(once) == len(set(once)), f"each writer closes exactly once, got {closes}"
    assert app._late_finalizer.retry() is False and sorted(closes) == once      # a repeat or overlap closes nothing


@pytest.mark.parametrize("which", ["bybit", "spot", "okx", "okx_capture"])
def test_a_client_that_never_started_cannot_touch_storage_and_its_writers_still_close(tmp_path, which):
    app = dict(_runner_apps(tmp_path))[which]
    client, closes = _client(app), _spy_writers(app)
    assert client.lifecycle_idle is True and app._storage_users_stopped() is True
    app._finalize_storage()
    assert closes and len(closes) == len(set(closes)), f"every writer closes once, got {closes}"
    quality = [c for c in closes if "quality" in c]
    assert quality, "the quality writer must close too: a never-started client has no user to wait for"
    app._finalize_storage()
    assert len(closes) == len(set(closes)), "a second finalization must not close anything again"
