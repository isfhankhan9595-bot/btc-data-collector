"""Can the process return the latched non-zero status while a REST producer ignores cancellation?

Real ``run_collector.main()`` (real ``asyncio.run``, real SIGTERM handler, real ``start()`` /
``_async_shutdown()`` / ``shutdown()``) in a child process, with one REST producer that swallows
every cancel until the parent releases it. The parent asserts the state under test BEFORE it
releases the producer, so nothing here depends on timing: the child cannot exit earlier than the
release, and the only timeouts are upper bounds that turn a hang into a failure.

What this pins, for a startup recovery and for the OI poller alike:
  * the application latches a non-zero status and keeps open exactly the storage the live
    producer can reach;
  * the process does NOT return that status while the task exists (``asyncio.run`` cannot
    finish: for the startup recovery start() itself is parked, for the OI poller start() returns
    and ``asyncio.run``'s task teardown waits for the task);
  * once the producer ends, the late retry finalizes every remaining writer (inside that same
    teardown) and the process exits with the latched status -- a late close is not a clean one;
  * a startup-recovery exception still fails the process (traceback, status 1) but only after
    the finalization ran.
It does NOT claim, and cannot show, that such a coroutine can be stopped: with a producer that
never ends only process-level termination (SIGKILL) ends the process, and then the exit status
is the kill, not the latched one (``test_a_producer_that_never_ends...``).
"""
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "posix", reason="real SIGTERM handling is POSIX-only")

_TESTS = Path(__file__).resolve().parent
_COLLECTOR_DIR = _TESTS.parent
_REPO_ROOT = _COLLECTOR_DIR.parent
_UPPER_BOUND_S = 60.0                      # only ever reached when something hangs


class _Lines:
    """Line reader on the child's stdout with a deadline (select + os.read; no buffered readline).

    Every line read is kept, so ``until`` finds a marker whatever order the child printed them in."""

    def __init__(self, proc):
        self._fd, self._partial, self._proc, self.seen = proc.stdout.fileno(), b"", proc, []

    def until(self, prefix, timeout=_UPPER_BOUND_S):
        deadline = time.monotonic() + timeout
        while True:
            for line in self.seen:
                if line.startswith(prefix):
                    return line
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"timed out waiting for {prefix!r}; seen={self.seen[-5:]!r}")
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if not ready:
                continue
            chunk = os.read(self._fd, 65536)
            if not chunk:
                raise AssertionError(f"child closed stdout before {prefix!r}; rc={self._proc.poll()}")
            *complete, self._partial = (self._partial + chunk).split(b"\n")
            self.seen.extend(line.decode(errors="replace").strip() for line in complete)


def _env():
    return dict(os.environ, PYTHONPATH=os.pathsep.join([str(_REPO_ROOT), str(_COLLECTOR_DIR)]),
                COLLECTOR_ALLOW_UNVERIFIED_FS="1")


def _fields(line):
    return dict(part.split("=", 1) for part in line.split()[1:] if "=" in part)


@pytest.mark.parametrize("producer", ["startup_recovery", "oi_poller"])
def test_the_process_cannot_return_its_latched_status_while_a_cancel_ignoring_producer_exists(tmp_path, producer):
    (tmp_path / "data").mkdir()
    flag = tmp_path / "release.flag"
    env = _env()
    proc = subprocess.Popen([sys.executable, str(_TESTS / "_runner_exit_child.py"), producer, str(flag)],
                            cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        lines = _Lines(proc)
        lines.until("PRODUCER_ENTERED")
        proc.send_signal(signal.SIGTERM)                       # the real systemd stop signal
        shutdown = _fields(lines.until("SHUTDOWN_TASK_DONE"))

        # --- state under test: the producer is still alive and has NOT been released ----------
        assert shutdown["shutdown_failure"] == "rest_producer_not_stopped"
        assert shutdown["exit_code"] == "1"                    # the status is latched in the app ...
        assert proc.poll() is None                             # ... but the process has not exited
        assert shutdown["raw_rest_closed"] == "False" and shutdown["quality_closed"] == "False"
        assert shutdown["oi_closed"] == ("True" if producer == "startup_recovery" else "False")
        assert shutdown["shutdown_done"] == "False"
        if producer == "startup_recovery":
            assert shutdown["start_done"] == "False"           # start() is parked on the producer
        else:
            # start() RETURNS (its finalization ran and kept the held storage open); the runner is now
            # inside asyncio.run's teardown, blocked on the live poller. Release only after that, so
            # the finalization below can only be the late retry, never start()'s own tail.
            returned = _fields(lines.until("START_RETURNED"))
            assert proc.poll() is None
            assert returned["shutdown_done"] == "False" and returned["shutdown_failure"] == "rest_producer_not_stopped"
            assert returned["raw_rest_closed"] == "False" and returned["oi_closed"] == "False"
            assert returned["quality_closed"] == "False"
        # ----------------------------------------------------------------------------------------

        flag.touch()                                           # only now does the producer end
        final = _fields(lines.until("MAIN_RETURNED"))
        assert proc.wait(timeout=_UPPER_BOUND_S) == 1          # the latched status is what is returned
        assert final["code"] == "1"
        # the late retry finalized everything that had been held -- for BOTH producers
        assert final["shutdown_done"] == "True"
        assert (final["raw_rest_closed"], final["oi_closed"], final["quality_closed"]) == ("True",) * 3
        assert final["shutdown_failure"] == "rest_producer_not_stopped"      # not reported as clean
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@pytest.mark.parametrize("producer", ["startup_recovery", "oi_poller"])
def test_a_producer_that_never_ends_needs_process_level_termination_and_nothing_is_closed_beneath_it(
        tmp_path, producer):
    (tmp_path / "data").mkdir()
    flag = tmp_path / "never-created.flag"                     # the producer is never released
    proc = subprocess.Popen([sys.executable, str(_TESTS / "_runner_exit_child.py"), producer, str(flag)],
                            cwd=tmp_path, env=_env(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        lines = _Lines(proc)
        lines.until("PRODUCER_ENTERED")
        proc.send_signal(signal.SIGTERM)
        shutdown = _fields(lines.until("SHUTDOWN_TASK_DONE"))
        assert shutdown["shutdown_failure"] == "rest_producer_not_stopped" and shutdown["exit_code"] == "1"
        with pytest.raises(subprocess.TimeoutExpired):         # the bounded window: it does not exit
            proc.wait(timeout=1.5)
        assert shutdown["shutdown_done"] == "False"
        assert shutdown["raw_rest_closed"] == "False" and shutdown["quality_closed"] == "False"
        proc.kill()                                            # what systemd's SIGKILL would do
        proc.wait(timeout=_UPPER_BOUND_S)
        assert proc.returncode == -signal.SIGKILL              # the kill, NOT the latched status 1
        assert b"MAIN_RETURNED" not in proc.stdout.read()      # it never reported a finished shutdown
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_a_startup_recovery_exception_fails_the_process_only_after_the_finalization_ran(tmp_path):
    (tmp_path / "data").mkdir()
    proc = subprocess.Popen([sys.executable, str(_TESTS / "_runner_exit_child.py"), "startup_recovery_raises",
                             str(tmp_path / "unused.flag")],
                            cwd=tmp_path, env=_env(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        raised = _Lines(proc).until("MAIN_RAISED")
        fields = _fields(raised)
        assert raised.split()[1] == "type=ValueError"          # visible: the real main() re-raised it
        assert proc.wait(timeout=_UPPER_BOUND_S) == 1          # traceback exit status, as before
        assert fields["shutdown_done"] == "True"               # ... but the finalization ran first
        assert (fields["raw_rest_closed"], fields["oi_closed"], fields["quality_closed"]) == ("True",) * 3
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
