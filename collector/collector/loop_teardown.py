"""``asyncio.run`` with a BOUNDED teardown.

Why this exists (reproduced, Python 3.11.15 and 3.12.3; see docs/EVENT_LOOP_TEARDOWN.md)
----------------------------------------------------------------------------------------
When the main coroutine returns, ``asyncio.run`` cancels every task still alive and then waits for them
with an UNBOUNDED ``gather``. A task that swallows ``CancelledError`` (a stuck ingest worker, an
``except BaseException`` in a handler) therefore makes ``asyncio.run`` never return: the supervisor's
carefully bounded shutdown is followed by an unbounded wait. While it waits, the runners' SIGTERM/SIGINT
handlers only set an already-set event, so the process ignores both; only SIGKILL ends it, and the exit
status the supervisor latched is lost (the status becomes "killed").

What this does
--------------
``run_bounded`` runs the coroutine exactly as ``asyncio.run`` does (``asyncio.Runner``), then cancels what
is left and waits at most ``teardown_deadline_s``. Tasks that end inside the deadline behave as before,
including the late-finalization done-callbacks (they run while this wait runs the loop). If a task is
still alive at the deadline the helper:

* does NOT try to stop it (Python offers no safe way to) and does NOT close the loop beneath it, so no
  callback or task can run afterwards and nothing is finalized on its behalf;
* reports it by name (a visible error log) and returns the names to the caller, which must not exit 0;
* leaves every writer exactly as the supervisor left it: open, with its ``.tmp`` / WAL for restart
  recovery.

What it cannot do
-----------------
It cannot end a task, and it cannot end a non-daemon THREAD (e.g. a blocked ``asyncio.to_thread`` call):
the interpreter still joins those at exit. It also gives up the chance that a task which would have ended
LATER (after the deadline, before an external SIGKILL) finalizes storage through its done-callback; that
trade is explicit and bounded by ``TEARDOWN_DEADLINE_S``.
"""
from __future__ import annotations

import asyncio
from typing import Any, Coroutine, List, Optional, Tuple

from .utils import logger

#: Longest the process waits, after the supervisor has returned, for tasks it just cancelled. Typical
#: tasks end within one loop turn; the late-finalization callbacks need the loop to keep running only until
#: the worker ends. This is a grace period ON TOP of the supervisor's own bounds and only ever elapses in
#: full when a task is stuck.
TEARDOWN_DEADLINE_S = 10.0

#: Status returned when the loop could not be torn down and nothing worse was latched.
EXIT_TEARDOWN_STUCK = 1


async def _cancel_remaining(deadline_s: float) -> List[asyncio.Task]:
    me = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks() if t is not me]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=deadline_s)
    loop = asyncio.get_running_loop()
    for task in tasks:                       # same reporting asyncio.run gives a task that failed at teardown
        if task.done() and not task.cancelled() and task.exception() is not None:
            loop.call_exception_handler({
                "message": "unhandled exception during bounded event-loop teardown",
                "exception": task.exception(), "task": task})
    return [t for t in tasks if not t.done()]


def _name(task: "asyncio.Task") -> str:
    coro = task.get_coro()
    return getattr(coro, "__qualname__", None) or repr(coro)


def run_bounded(main: Coroutine[Any, Any, Any], *,
                teardown_deadline_s: Optional[float] = None) -> Tuple[Any, List[str]]:
    """Run ``main`` like ``asyncio.run``; return ``(result, stuck_task_names)``.

    Anything ``main`` raises (``KeyboardInterrupt`` included) is re-raised AFTER the bounded teardown, as
    ``asyncio.run`` would. ``stuck_task_names`` is empty when the loop was torn down and closed normally;
    otherwise the caller must turn it into a non-zero exit status (``teardown_exit_status``)."""
    deadline = TEARDOWN_DEADLINE_S if teardown_deadline_s is None else teardown_deadline_s
    runner = asyncio.Runner()
    result: Any = None
    error: Optional[BaseException] = None
    try:
        result = runner.run(main)
    except BaseException as exc:  # noqa: BLE001 - re-raised below, after teardown
        error = exc
    stuck: List[str] = []
    try:
        left = runner.get_loop().run_until_complete(_cancel_remaining(deadline))
        stuck = [_name(t) for t in left]
    except BaseException as exc:  # noqa: BLE001 - a broken teardown must not mask the real outcome
        logger.error("event_loop_teardown_failed", error=f"{type(exc).__name__}: {exc}")
        stuck = stuck or ["<teardown failed>"]
    if stuck:
        # Deliberately NOT closed: closing would cancel/await again (unbounded). Nothing runs on this loop
        # any more, so the stuck tasks cannot touch storage; the process is about to exit.
        logger.error("event_loop_teardown_deadline_exceeded", deadline_s=deadline, tasks=stuck,
                     note="tasks still alive; storage they can reach was left open and unfinalized; "
                          "exiting non-zero without waiting further")
    else:
        runner.close()
    if error is not None:
        raise error
    return result, stuck


def teardown_exit_status(status: int, stuck: List[str]) -> int:
    """``status`` unchanged when the teardown was clean; never 0 when tasks were left alive."""
    if stuck and not status:
        return EXIT_TEARDOWN_STUCK
    return status
