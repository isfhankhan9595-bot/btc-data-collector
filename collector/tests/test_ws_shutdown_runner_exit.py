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
  * once the producer ends, the process exits with the latched status.
It does NOT claim, and cannot show, that such a coroutine can be stopped: with a producer that
never ends only process-level termination (SIGKILL) ends the process, and then the exit status
is the kill, not the latched one. Which writers are finalised AFTER a late producer end is asserted
only for the startup recovery (start() resumes and completes shutdown).
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
    """Line reader on the child's stdout with a deadline (select + os.read; no buffered readline)."""

    def __init__(self, proc):
        self._fd, self._buf, self._proc = proc.stdout.fileno(), b"", proc

    def until(self, prefix, timeout=_UPPER_BOUND_S):
        deadline = time.monotonic() + timeout
        while True:
            while b"\n" in self._buf:
                line, self._buf = self._buf.split(b"\n", 1)
                text = line.decode(errors="replace").strip()
                if text.startswith(prefix):
                    return text
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"timed out waiting for {prefix!r}; unread={self._buf[-300:]!r}")
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if not ready:
                continue
            chunk = os.read(self._fd, 65536)
            if not chunk:
                raise AssertionError(f"child closed stdout before {prefix!r}; rc={self._proc.poll()}")
            self._buf += chunk


def _fields(line):
    return dict(part.split("=", 1) for part in line.split()[1:] if "=" in part)


@pytest.mark.parametrize("producer", ["startup_recovery", "oi_poller"])
def test_the_process_cannot_return_its_latched_status_while_a_cancel_ignoring_producer_exists(tmp_path, producer):
    (tmp_path / "data").mkdir()
    flag = tmp_path / "release.flag"
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(_REPO_ROOT), str(_COLLECTOR_DIR)]),
               COLLECTOR_ALLOW_UNVERIFIED_FS="1")
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
        # ----------------------------------------------------------------------------------------

        flag.touch()                                           # only now does the producer end
        final = _fields(lines.until("MAIN_RETURNED"))
        assert proc.wait(timeout=_UPPER_BOUND_S) == 1          # the latched status is what is returned
        assert final["code"] == "1"
        if producer == "startup_recovery":                     # start() resumed and finished the close
            assert final["shutdown_done"] == "True"
            assert (final["raw_rest_closed"], final["oi_closed"], final["quality_closed"]) == ("True",) * 3
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
