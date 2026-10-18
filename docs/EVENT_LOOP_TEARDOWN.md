# Event-loop teardown with a cancellation-resistant task

Scope: what happens between "the supervisor returned" and "the process exits" when a task swallowed its
cancellation. Evidence is from subprocess experiments on CPython 3.11.15 and 3.12.3; nothing here was
verified against a live exchange, a real systemd stop, or power loss.

## Request versus confirmed termination

`stop()`, `Task.cancel()` and a bounded wait are **requests and observations**. Only `task.done()` is
confirmation. Python has no safe way to force a task to end, and this code does not try. A writer is closed
only when every task that can reach it is confirmed done; otherwise it stays open and the failure is latched.

## What `asyncio.run` does (identical on 3.11 and 3.12)

1. The main coroutine returns (for these runners: the supervisor's exit status).
2. `_cancel_all_tasks` cancels every remaining task, then `run_until_complete(gather(...))` with **no timeout**.
3. `shutdown_asyncgens()`, then `shutdown_default_executor()`: unbounded on 3.11; 3.12 passes a 300 s timeout.
4. `loop.close()`.

Consequences, each reproduced:

* A task that swallows `CancelledError` and later ends: `asyncio.run` waits for it, then returns normally.
* A task that never ends: `asyncio.run` never returns. The runners' SIGTERM/SIGINT handlers only set an
  already-set event, so both signals are absorbed; only SIGKILL ends the process, and the exit status the
  supervisor latched (1) is replaced by "killed" (-9).
* A second cancel arrives (supervisor's, then `asyncio.run`'s): cleanup written as `except CancelledError`
  runs twice. The runners' own finalization does not (`CloseOnce`, `LateFinalizer.ran`).
* Task done-callbacks DO run during step 2, so the late finalizer works under `asyncio.run`. A callback issued
  from another thread after `loop.close()` raises `RuntimeError` and is lost (the project has no such path).
* A blocked executor thread (the disk-pressure poll is the only one; it touches no writer) delays exit on both
  versions. 3.12's join timeout lets `asyncio.run` return, but the interpreter still joins the thread at exit.
* Python 3.11 and 3.12 differ only in the executor-join timeout. Differences seen in individual runs were
  scheduling (a synchronous writer close blocked the loop), not semantics.

## Change: `loop_teardown.run_bounded`

All five entrypoints use it instead of `asyncio.run`. It runs the coroutine with `asyncio.Runner`, then cancels
what is left and waits at most `TEARDOWN_DEADLINE_S` (10 s, in addition to the supervisor's own bounds).

* Tasks that end in time: unchanged behaviour, including late finalization.
* A task still alive at the deadline: logged by name (`event_loop_teardown_deadline_exceeded`), the loop is NOT
  closed (closing would wait again), nothing is finalized on its behalf, the status is never 0
  (`teardown_exit_status`: a worse latched status such as 70 keeps priority).

Trade-off, stated plainly: after the bound the process stops waiting, so a worker that would have ended later
can no longer finalize its storage through the late-finalization callback. The previous behaviour (wait forever)
kept that chance only until an external SIGKILL, and lost the exit status and the visible record.

## What is left, and the state it leaves

| Situation | Storage | Exit status | Visible |
|---|---|---|---|
| Worker never ends | Worker-reachable writers open; rows in `.tmp`; Bybit quality events in the WAL; restart recovery applies | 1 (70/69 keep priority) | `runner_shutdown_terminal_failure`, `ingest_worker_retirement_failed`, `event_loop_teardown_deadline_exceeded` |
| Worker ends within the bound | Finalized late, once, in venue order | 1 (a late close is not clean) | `late_shutdown_finalization` |
| A healthy writer fails to close | Its rows stay in `.tmp` | 1 (`writer_close_failed:<name>`) | `storage_shutdown_close_failed` + quality event |
| A writer that had already failed fails to close | unchanged | unchanged (already classified when it failed) | unchanged |
| Blocked executor thread | Finalized normally | the supervisor's | none; exit is delayed until the thread returns |

## What `run_bounded` does and does not bound (each reproduced, Python 3.11.15 and 3.12.3)

| Waiting on | Bounded by `run_bounded`? | Evidence |
|---|---|---|
| A task that yields but ignores cancellation | **Yes**, at `TEARDOWN_DEADLINE_S` | never-ending worker: exit 1, no hang (Bybit, Spot, OKX, OKX capture) |
| A task that blocks the loop SYNCHRONOUSLY (no `await`) | **No** | deadline 0.5 s, returned after 8.1 s: the timer cannot fire while the loop is blocked |
| A blocked executor thread (`asyncio.to_thread(StoragePressureMonitor.poll)`) | **No** | see "Executor thread" below |
| The supervisor's own waits before `main` returns | No (separate bounds, below) | |
| USD-M `_run_startup_recovery` (`asyncio.wait({task})`, no timeout) | **No** | pre-existing, documented in DEPLOYMENT.md: `start()` parks there, so `main()` never reaches teardown |

## Shutdown budget: UNRESOLVED

Fact: the shutdown waits are independent RELATIVE timeouts that add up. There is no shared absolute deadline
(`shutdown_deadline_s` is a sum of constants; every step calls `asyncio.wait(..., timeout=constant)`).

| Wait (all runners unless noted) | Bound | Notes |
|---|---|---|
| `await_client_idle` -> `wait_stopped(shutdown_deadline_s)` | 47 s | 10 close handshake + 2 slack + 30 drain + 5 worker retirement |
| USD-M hard-cancel fallback | +6 s | `worker_retire_timeout_s + WS_FALLBACK_SLACK_S` |
| USD-M quality-consumer stop (before the websocket stop) | <= 2.5 s | only when the loop is stalled |
| Spot book-recovery cancels | 2 x 1 s | `REST_PRODUCER_CANCEL_BOUND_S` |
| Standalone supervisor `_bounded_wait(run_task)` | +15 s | only when `run_task` is still alive after `shutdown()` |
| **`run_bounded` teardown (this change)** | **+10 s** | elapses in full only when a task is stuck |
| Writer finalization (~7-10 synchronous closes with fsync) | **unbounded in code, unmeasured on a host** | a first `raw_wire` close took 0.3 s (3.12) to 1.3 s (3.11, cold) in the sandbox |
| `await stopped` after `client.stop()` | not a wait with the real client (`stop()` is synchronous) | unbounded only for a non-standard client |
| USD-M `_run_startup_recovery` | **unbounded** | see above |
| Blocked executor thread at exit | **unbounded** | see below |

Arithmetic for the one unit that has a limit (`btc-collector.service`, `TimeoutStopSec=60`): worst case
2.5 + 47 + 6 = 55.5 s, plus 10 s teardown, plus finalization = **>= 65.5 s**. The sum does NOT fit in 60 s, so
the invariant "shutdown finishes inside `TimeoutStopSec`" is NOT proven. In that worst case (unresponsive peer
AND a stuck worker) systemd's SIGKILL arrives first, exactly as before this change: the status is the kill,
not the latched 1. Nothing here makes that case worse; nothing guarantees it is better.

Measured (development sandbox, NOT a production host; Bybit/Spot/OKX standalone runners, fake socket, a worker
that never ends, production timeouts: drain 30 s, retire 5 s, teardown 10 s, supervisor 15 s): SIGTERM to exit
was **45.2-45.6 s in 7 of 8 runs** (30 s drain + 5 s retirement + 10 s teardown + ~0.3 s). One Python 3.11 Bybit
run took 50.4 s (first cancel 3.2 s late); two repeats gave 45.4 and 45.5 s and the cause was not determined.
The USD-M stuck-worker path was NOT measured; analytically it is about 35 s of client wait + quality stop +
finalization + the 10 s teardown (~47 s), which is arithmetic, not a measurement.

Decision: `TEARDOWN_DEADLINE_S` stays 10 s. The measurements support it in the typical stuck-worker case (>= 9.6 s
of margin below 60 s), and no measurement supports a different value. This is NOT a production-bounded claim.
Follow-up (not done): derive the teardown wait from the REMAINING budget (a stop timestamp threaded from the
signal handler), and measure writer finalization on the real host.

## Executor thread (`asyncio.to_thread(self.poll)`): follow-up, not fixed here

* Implementation: `disk_pressure.py` calls `asyncio.to_thread`, i.e. the loop's default `ThreadPoolExecutor`.
  Its worker threads are non-daemon and joined at interpreter exit, so they cannot be daemonised; a daemon
  thread would need a dedicated thread plus a future bridge, i.e. a rewrite of the monitor (F5-QA-04).
* `poll()` calls `shutil.disk_usage` and `os.scandir` on the data/WAL directories only. It touches no writer.
* Reproduced on the integrated tree (Bybit): poll blocked 4 s -> exit 0 after 4.5 s, all 7 writers closed;
  poll blocked 30 s with a 12 s deadline -> still running at 12 s, ended only by SIGKILL (-9), all 7 writers
  reported closed OK (the on-disk `.tmp` state was not inspected in this run). Same on 3.11 and 3.12 (3.12's 300 s join timeout does not help: the
  interpreter still joins the thread at exit).
* Verdict: it can violate the process deadline and report a clean shutdown as killed (a false failure, not a
  false success). It leaves no unsafe storage reachable. Its realistic trigger is a hung data filesystem, on
  which the synchronous writer `fsync` calls would stall first. Therefore not changed in Phase 2.

## Mutation evidence (scratch copies; the working tree was never mutated)

Each mutant is one asserted replacement; its control (same tests, unmutated) was green.

| Mutant | Result |
|---|---|
| close gate removed (Bybit `_storage_users_stopped` -> True) | first SURVIVED (test gap); now killed by `test_late_finalization_does_not_close_while_start_tail_is_still_live_then_closes_once` |
| same, OKX capture | first SURVIVED; now killed by the same test |
| live Spot recovery producer ignored by the gate | killed (`test_spot_book_recovery_task_alive_holds_ob_and_quality_until_it_ends`) |
| late finalization disabled (`arm` no-op) | killed (4 tests) |
| shutdown-failure latch never stored | killed (6 tests) |
| new close-failure latch removed | killed (`test_e_failed_writer_finalization_is_not_reported_as_success`) |
| teardown wait made unbounded | killed (2 tests; the hang case runs to its 60 s upper bound) |
| stuck tasks no longer force a non-zero status | killed (2 tests) |
| never-started client reported not idle | first killed only on OKX capture; now killed on all four runners |
| `CloseOnce` memo removed (double close) | killed (17 tests) |
| a late callback's error turning failure into success | **equivalent / untestable**: the finalizer holds no reference to the policy and the latch is first-wins and never cleared; the only route is removing the latch, covered above |

## Guarantee and its limit

The gate "close a writer only when its users are confirmed stopped" held in every experiment for the ingest
worker, REST producers and the OKX capture runner. It is a per-runner rule, not a structural guarantee: a new
task or thread that writes would bypass it until added to that runner's reach set. A stuck task is never
stopped by this code; the safe outcome is storage preserved, a named failure, and a non-zero status.
