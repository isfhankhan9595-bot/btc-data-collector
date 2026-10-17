"""F5 failure policy for the standalone venue runners.

Binance Spot, Bybit, OKX (collector) and OKX (capture-only) share the
``WebSocketClient`` with the Binance USD-M collector, but not its application.
Before this module they had NO supervised path for a typed storage fatal: a
derived-writer ENOSPC put the shared client in terminal discard mode, the
client's task ended with ``FatalStorageError``, and ``_main`` never observed
that task -- the process stayed alive collecting nothing. (On the pre-F5 base
the same failure was isolated by the ordinary worker-error path and raw capture
kept going: that is the regression this closes.)

The policy implemented here is the USD-M contract, with the classification
table built from each runner's OWN writers (see ``build_stream_table``):

    raw_wire / raw_rest writer failure -> TERMINATE: controlled shutdown and a
                                          non-zero exit (``EXIT_FATAL_STORAGE``)
    derived writer failure             -> ISOLATE that route only; raw capture
                                          and every healthy route keep running
    quality writer failure             -> DEGRADE the quality channel only
    unknown typed fatal                -> TERMINATE (default-deny)
    ordinary exception                 -> not handled here (P0-1 unchanged)

Classification is ``isinstance(exc, FatalStorageError)`` plus the failed
writer's own ``stream``; never ``RuntimeError``, never message text.

Threading / task model
----------------------
``StandaloneFailurePolicy.on_fatal`` is called synchronously from the
websocket worker (or from the handler the worker runs). It only LATCHES state
and puts the clients in discard mode; it never shuts anything down. The
controlled shutdown is performed by ``supervise_standalone_runner`` from the
runner's main task, which is never the worker, so nothing here cancels or
awaits itself.
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from .failure_topology import (
    EXIT_FATAL_STORAGE,
    VERDICT_DEGRADE_QUALITY,
    VERDICT_ISOLATE,
    VERDICT_TERMINATE,
    FailureRecord,
    classify_stream,
)
from .storage_errors import FatalStorageError
from .utils import logger, send_telegram_alert
from .websocket_client import WorkerRetirementError

__all__ = [
    "EXIT_FATAL_STORAGE", "EXIT_RUN_TASK_CRASHED", "SHUTDOWN_TASK_TIMEOUT_S",
    "DEDUP_DEFAULT_STREAM", "ROUTE_TAG_ATTRIBUTE", "tag_route", "build_stream_table",
    "StandaloneFailurePolicy", "supervise_standalone_runner", "await_client_idle", "cancel_and_wait", "REST_PRODUCER_CANCEL_BOUND_S",
    "NO_QUALITY_WAL_NOTE", "UNCONFIRMED_QUALITY_EVENT_NOTE", "is_bounded_report_point",
    "DegradedQualityLossReporting",
]

#: Exit status when the application task died of an exception that is NOT a
#: typed storage fatal. Non-zero so systemd restarts it; distinct from 70 so an
#: operator can tell "storage evidence lost" from "the task crashed".
EXIT_RUN_TASK_CRASHED = 1

#: Upper bound on waiting for a cancelled application task during shutdown. A
#: websocket close handshake can take seconds; it must never make a terminal
#: shutdown unbounded.
SHUTDOWN_TASK_TIMEOUT_S = 15.0

#: ``DedupStateError`` names this stream by default (``segment_dedup``): the
#: dedup index guards the trade route whichever venue it runs on.
DEDUP_DEFAULT_STREAM = "trades"

Table = Dict[str, Tuple[str, Optional[str]]]

#: Attribute a handler sets on a propagating ``FatalStorageError`` to record
#: which route it was serving. Handlers do NOT catch typed fatals (the P0-4
#: contract is that they propagate to the websocket worker boundary, where the
#: client calls ``on_fatal``); they only tag and re-raise, so the classifier can
#: still tell two trade routes of one venue apart.
ROUTE_TAG_ATTRIBUTE = "f5_route"


def tag_route(exc: BaseException, route: Optional[str]) -> None:
    """Record ``route`` on a propagating fatal (first tag wins; never raises)."""
    if route is None or getattr(exc, ROUTE_TAG_ATTRIBUTE, None) is not None:
        return
    try:
        setattr(exc, ROUTE_TAG_ATTRIBUTE, route)
    except Exception:  # noqa: BLE001 - a tag is advisory; classification still works by stream
        pass


def build_stream_table(*, raw: Iterable[Any], derived: Mapping[Any, str],
                       quality: Iterable[Any],
                       dedup_route: Optional[str] = None) -> Table:
    """Stream -> ``(verdict, route)`` for ONE runner, from its own writers.

    Each argument names writers either by the writer object or by its stream
    name (``writer.stream_name`` is what a ``FatalStorageError`` carries). The
    names are read from the live writers rather than spelled out, so a runner
    can never drift from the stream names it actually writes, and the Binance
    USD-M names (``trades``, ``orderbook`` ...) are never copied onto a venue
    whose streams are ``spot_*`` / ``bybit_*`` / ``okx_*``.

    ``derived`` maps writer -> route. ``dedup_route`` additionally maps
    ``DEDUP_DEFAULT_STREAM`` (what ``DedupStateError`` names) to that route.
    A stream absent from the result is TERMINATE: default-deny.
    """
    def name(writer: Any) -> str:
        return writer if isinstance(writer, str) else writer.stream_name

    table: Table = {}
    for writer in raw:
        table[name(writer)] = (VERDICT_TERMINATE, None)
    for writer, route in derived.items():
        table[name(writer)] = (VERDICT_ISOLATE, route)
    for writer in quality:
        table[name(writer)] = (VERDICT_DEGRADE_QUALITY, None)
    if dedup_route is not None:
        table.setdefault(DEDUP_DEFAULT_STREAM, (VERDICT_ISOLATE, dedup_route))
    return table


class StandaloneFailurePolicy:
    """Latched failure state + classifier for one standalone runner.

    ``clients`` is a callable returning the websocket clients to put in discard
    mode on a terminal failure (a callable, because the client is built after
    the policy). ``report`` is an optional extra reporter, called with each NEW
    ISOLATE / TERMINATE record; it must not raise into the policy (it is
    guarded) and is never called for a quality failure, because the quality
    writer may be the failed component.
    """

    def __init__(self, *, venue: str, streams: Mapping[str, Tuple[str, Optional[str]]],
                 clients: Callable[[], Iterable[Any]] = lambda: (),
                 report: Optional[Callable[[FailureRecord], None]] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.venue = venue
        self._streams: Table = dict(streams)
        self._clients = clients
        self._report_extra = report
        self._clock = clock
        #: ``component:stream[@route]`` -> FailureRecord (first failure wins).
        self.failed_components: Dict[str, FailureRecord] = {}
        #: route -> FailureRecord for routes cut off from their writer.
        self.isolated_routes: Dict[str, FailureRecord] = {}
        #: First TERMINATE-verdict failure; once set the process must exit non-zero.
        self.terminal_failure: Optional[FailureRecord] = None
        #: First quality-channel failure (telemetry degraded, market data intact).
        self.quality_degraded: Optional[FailureRecord] = None
        #: Repeat observations of an already-latched component (counted, not re-reported).
        self.failure_repeats: Dict[str, int] = {}
        #: Events short-circuited per isolated route (a counter, never an event per frame).
        self.route_short_circuits: Dict[str, int] = {}
        #: Set the moment a TERMINATE verdict latches; the supervisor waits on it.
        self.terminal_event = asyncio.Event()
        self._reporting = False
        #: First explicit shutdown failure (a websocket ingest worker that could
        #: not be retired). Distinct from ``terminal_failure``: no storage verdict
        #: was reached, but a shutdown that could not run its ordering must never
        #: exit 0.
        self.shutdown_failure: Optional[str] = None

    def record_shutdown_failure(self, reason: str) -> None:
        """Latch a terminal shutdown failure (first reason wins). Never raises."""
        if self.shutdown_failure is None:
            self.shutdown_failure = reason
        logger.error("runner_shutdown_terminal_failure", venue=self.venue, reason=reason,
                     exit_status=EXIT_RUN_TASK_CRASHED)

    # -- classification ------------------------------------------------------

    @property
    def exit_code(self) -> int:
        if self.terminal_failure is not None:
            return EXIT_FATAL_STORAGE
        return EXIT_RUN_TASK_CRASHED if self.shutdown_failure is not None else 0

    def on_fatal(self, exc: BaseException, origin: str = "handler", *,
                 route: Optional[str] = None) -> str:
        """Classify and latch one typed storage fatal; return the verdict.

        Idempotent and non-raising for every ``FatalStorageError``. ``route`` is
        the route whose handler was running when the writer raised: for an
        ISOLATE verdict it names the route to cut off (a venue with two trade
        streams, or a ``DedupStateError`` whose stream is the generic
        ``trades``, would otherwise be ambiguous). It can never turn a
        TERMINATE into an ISOLATE.
        """
        if not isinstance(exc, FatalStorageError):
            raise TypeError(f"on_fatal requires a FatalStorageError, got {type(exc).__name__}")
        if route is None:
            # The handler tagged the exception with the route it was serving as
            # it propagated to the client's worker boundary (see ``tag_route``).
            route = getattr(exc, ROUTE_TAG_ATTRIBUTE, None)
        record = FailureRecord.from_exception(
            exc, origin=origin, now_ms=int(self._clock() * 1000), table=self._streams)
        if record.verdict == VERDICT_ISOLATE and route is not None and record.route != route:
            record = dataclasses.replace(record, route=route)
        return self._latch(record)

    def is_quality_channel_failure(self, exc: BaseException) -> bool:
        """True only for a TYPED fatal this runner's OWN table maps to the quality
        channel (venue-prefixed names such as ``bybit_quality_events``; never the
        USD-M default table, never message text). Pure: latches nothing."""
        return (isinstance(exc, FatalStorageError)
                and classify_stream(exc.stream, table=self._streams)[0] == VERDICT_DEGRADE_QUALITY)

    def _latch(self, record: FailureRecord) -> str:
        key = record.key if record.verdict != VERDICT_ISOLATE else f"{record.key}@{record.route}"
        existing = self.failed_components.get(key)
        if existing is not None:
            # Already latched: count, do not re-report. This is what stops an
            # error flood when every later frame trips the same dead writer.
            self.failure_repeats[key] = self.failure_repeats.get(key, 0) + 1
            return existing.verdict
        self.failed_components[key] = record
        if record.verdict == VERDICT_ISOLATE and record.route is not None:
            self.isolated_routes.setdefault(record.route, record)
        elif record.verdict == VERDICT_DEGRADE_QUALITY:
            if self.quality_degraded is None:
                self.quality_degraded = record
        else:  # VERDICT_TERMINATE, including every unmapped typed fatal
            if self.terminal_failure is None:
                self.terminal_failure = record
            self._enter_terminal_mode()
        self._report(record)
        return record.verdict

    def _enter_terminal_mode(self) -> None:
        """Stop normal ingestion and wake the supervisor; shut nothing down here.

        Discard mode keeps each client's worker draining (``task_done`` exactly
        once per item), so no producer blocks and ``queue.join()`` completes."""
        try:
            clients = tuple(self._clients())
        except Exception as exc:  # noqa: BLE001 - latching must never raise into a worker
            logger.error("terminal_mode_client_lookup_failed", venue=self.venue,
                         error=f"{type(exc).__name__}: {exc}")
            clients = ()
        for client in clients:
            try:
                client.enter_discard_mode("app_terminal_failure")
            except Exception as exc:  # noqa: BLE001
                logger.error("terminal_mode_client_stop_failed", venue=self.venue,
                             error=f"{type(exc).__name__}: {exc}")
        self.terminal_event.set()

    def _report(self, record: FailureRecord) -> None:
        """Recursion-safe, never raises. Structured log first; an operator alert;
        then the runner's own reporter -- but never through the quality writer
        for a quality failure (it may be the failed component)."""
        # ``_reporting`` stops a NON-quality report from nesting inside another
        # one (the runner's reporter writes to the quality stream). A quality
        # record may nest: it only logs and alerts -- ``_report_extra`` is never
        # called for it, so nothing here can reach the failed quality writer. On
        # a full disk the quality write made by a raw/derived failure's reporter
        # fails too; without this the quality latch would be recorded silently.
        nested = self._reporting
        if nested and record.verdict != VERDICT_DEGRADE_QUALITY:
            return
        self._reporting = True
        try:
            logger.error("storage_failure_latched", venue=self.venue, **record.as_dict())
            try:
                send_telegram_alert(
                    f"STORAGE FAILURE [{record.verdict}] venue={self.venue} stream={record.stream} "
                    f"component={record.component} stage={record.stage} "
                    f"durability={record.durability} origin={record.origin}")
            except Exception as exc:  # noqa: BLE001 - alerting fails open
                logger.error("storage_failure_alert_failed", error=f"{type(exc).__name__}: {exc}")
            if self._report_extra is not None and record.verdict != VERDICT_DEGRADE_QUALITY:
                try:
                    self._report_extra(record)
                except Exception as exc:  # noqa: BLE001 - a broken reporter must not abort the caller
                    logger.error("storage_failure_report_failed", venue=self.venue,
                                 error=f"{type(exc).__name__}: {exc}")
        finally:
            self._reporting = nested

    # -- route isolation -----------------------------------------------------

    def route_is_isolated(self, route: Optional[str]) -> bool:
        return route in self.isolated_routes

    def note_short_circuit(self, route: str) -> None:
        self.route_short_circuits[route] = self.route_short_circuits.get(route, 0) + 1


#: Said in every per-event loss report (``<prefix>_quality_event_lost``); the status
#: carries ``quality_wal: False``. "Lost" means the event was not durably published
#: through the supported quality-storage path. These runners (Binance Spot, OKX, OKX
#: capture-only) provide no supported quality WAL for replay or recovery, and an
#: unpublished ``.tmp`` file that may remain is not a durable record: it is not
#: guaranteed to be complete, recoverable or authoritative. The text makes no claim
#: beyond that. The final shutdown report does NOT reuse this note: it is built from
#: the counters by ``_final_quality_note`` so it never says "counted as lost" when
#: no event was counted lost.
NO_QUALITY_WAL_NOTE =("counted as lost: not durably published through the supported quality-storage "
                       "path; this runner provides no supported quality WAL for replay or recovery; "
                       "an unpublished .tmp file may remain but is not guaranteed to be complete, "
                       "recoverable or authoritative")

#: An ``unconfirmed`` event is never given a definite outcome.
UNCONFIRMED_QUALITY_EVENT_NOTE = ("durability could not be established: the event is neither "
                                  "confirmed lost nor confirmed retained")

#: Clauses of the final shutdown report. Each describes one counter and is included only
#: when that counter is non-zero; the no-WAL clause is always included. Presentation
#: only: no counter, cadence, latch or control flow depends on this text.
_FINAL_LOST_CLAUSE = ("lost events: not durably published through the supported quality-storage path")
_FINAL_UNCONFIRMED_CLAUSE = ("unconfirmed events: durability could not be established, so each is "
                             "neither confirmed lost nor confirmed retained")
_FINAL_NO_WAL_CLAUSE = ("this runner provides no supported quality WAL for replay or recovery; an "
                        "unpublished .tmp file may remain but is not guaranteed to be complete, "
                        "recoverable or authoritative")


def _final_quality_note(lost: int, unconfirmed: int) -> str:
    """The final-report note for these two counter values (a pure string builder).

    A lost clause appears only when ``lost`` is non-zero, so an event whose durability
    was merely unconfirmed is never described as lost; likewise the unconfirmed clause
    appears only when ``unconfirmed`` is non-zero."""
    clauses = []
    if lost:
        clauses.append(_FINAL_LOST_CLAUSE)
    if unconfirmed:
        clauses.append(_FINAL_UNCONFIRMED_CLAUSE)
    clauses.append(_FINAL_NO_WAL_CLAUSE)
    return "; ".join(clauses)

#: Longest ``reason`` copied into a loss log line (a log line must stay bounded).
_LOSS_LOG_REASON_MAX = 200


def is_bounded_report_point(count: int) -> bool:
    """True for 1, 2, 4, 8, ...: the counts at which a loss is logged. The first
    loss is always reported, and the number of reports grows with log2 of the
    losses, never with the losses themselves."""
    return count > 0 and (count & (count - 1)) == 0


class DegradedQualityLossReporting:
    """Operator visibility for a degraded quality channel on a runner that provides no
    supported quality WAL for replay or recovery.

    Shared by Binance Spot, OKX and OKX capture-only so the three cannot drift. The
    host class provides ``quality_events_lost``, ``quality_events_unconfirmed``,
    ``failure_policy`` and a ``quality_log_prefix`` (the log-event prefix, e.g.
    ``spot``). The counters must exist before any writer does: a writer may emit a
    quality event while it is being constructed.

    Reporting is separate from accounting. The increment always happens; logging a
    count is best-effort and never raises into market data, a latch or a shutdown.
    This class never touches the failed quality writer, the failure policy's
    verdicts or an exit code, and sends no alert: the one-time latch alert stays the
    policy's.
    """

    quality_log_prefix: str = "runner"

    def _count_quality_event_lost(self, event: Optional[Mapping[str, Any]] = None) -> None:
        """One quality event demonstrably not durably published (no supported quality WAL)."""
        self.quality_events_lost += 1
        count = self.quality_events_lost
        if is_bounded_report_point(count):
            self._log_quality_loss("quality_event_lost", count, event, NO_QUALITY_WAL_NOTE)

    def _count_quality_event_unconfirmed(self, event: Optional[Mapping[str, Any]] = None) -> None:
        """One quality event whose durability could not be established either way."""
        self.quality_events_unconfirmed += 1
        count = self.quality_events_unconfirmed
        if is_bounded_report_point(count):
            self._log_quality_loss("quality_event_unconfirmed", count, event,
                                   UNCONFIRMED_QUALITY_EVENT_NOTE)

    def _log_quality_loss(self, name: str, count: int, event: Optional[Mapping[str, Any]],
                          note: str) -> None:
        try:
            event = event or {}
            event_type = event.get("event_type")
            reason = event.get("reason")
            logger.error(
                f"{self.quality_log_prefix}_{name}", count=count,
                quality_events_lost=self.quality_events_lost,
                quality_events_unconfirmed=self.quality_events_unconfirmed,
                quality_wal=False, stream=event.get("stream"),
                event_type=None if event_type is None else str(getattr(event_type, "value", event_type)),
                reason=None if reason is None else str(reason)[:_LOSS_LOG_REASON_MAX], note=note)
        except Exception:  # noqa: BLE001 - reporting a loss must never raise into market data
            pass

    def _report_quality_channel_final(self) -> None:
        """Log the final counters once at shutdown, if the channel ever degraded.

        Called AFTER every writer has been closed, so the counts include anything the
        close path itself reported. It never raises, so a failure here cannot mask an
        existing failure or prevent cleanup. A healthy runner logs nothing, and an
        unchanged snapshot is not logged twice. The note names lost events only when
        ``quality_events_lost`` is non-zero and unconfirmed events only when
        ``quality_events_unconfirmed`` is non-zero (``_final_quality_note``)."""
        try:
            lost = self.quality_events_lost
            unconfirmed = self.quality_events_unconfirmed
            policy = getattr(self, "failure_policy", None)
            degraded = policy is not None and policy.quality_degraded is not None
            if not (degraded or lost or unconfirmed):
                return
            snapshot = (degraded, lost, unconfirmed)
            if snapshot == getattr(self, "_quality_final_reported", None):
                return
            self._quality_final_reported = snapshot
            logger.error(
                f"{self.quality_log_prefix}_quality_channel_final", quality_degraded=degraded,
                quality_events_lost=lost, quality_events_unconfirmed=unconfirmed, quality_wal=False,
                note=_final_quality_note(lost, unconfirmed))
        except Exception:  # noqa: BLE001 - a final report must never mask a failure or stop cleanup
            pass


async def await_client_idle(client: Any, policy: "StandaloneFailurePolicy") -> bool:
    """Wait (bounded) until ``client`` can no longer touch storage.

    Call it AFTER ``client.stop()`` and BEFORE closing any writer the client's
    ingest worker or callbacks can reach. ``stop()`` only requests the shutdown;
    the receive loop, the queue drain and the worker's retirement all happen in
    the client's own ``start()`` task, which the supervisor does not cancel until
    ``shutdown()`` has returned -- so closing writers straight after ``stop()``
    put storage beneath a still-live worker. Returns False (and latches an
    explicit ``shutdown_failure`` on ``policy``, which makes the exit status
    non-zero) when the client is not idle within its own documented bound; the
    caller must then NOT close those writers. A client double without
    ``wait_stopped`` counts as idle. Never raises.
    """
    waiter = getattr(client, "wait_stopped", None)
    if waiter is None:
        return True
    try:
        idle = bool(await waiter(getattr(client, "shutdown_deadline_s", 60.0)))
    except Exception as exc:  # noqa: BLE001 - a broken wait must fail CLOSED, not skip the check
        logger.error("runner_client_wait_failed", venue=policy.venue,
                     error=f"{type(exc).__name__}: {exc}")
        idle = False
    if not idle:
        policy.record_shutdown_failure("ws_client_worker_not_retired")
    return idle


async def _bounded_wait(task: "asyncio.Future[Any]", timeout: float, what: str) -> None:
    """Wait for ``task`` without ever blocking forever, and retrieve its outcome
    so it can never become an unobserved background task."""
    if task.done():
        return
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        logger.error("runner_task_did_not_stop_in_time", task=what, timeout_s=timeout)


#: How long a runner waits for a REST producer (book-recovery task) to end after cancelling it. The
#: same bound as run_collector.WS_FALLBACK_SLACK_S (tests/test_ws_rest_producer_reach.py asserts the
#: two stay equal). A producer that outlives it is reported, never assumed gone.
REST_PRODUCER_CANCEL_BOUND_S = 1.0


async def cancel_and_wait(task: "Optional[asyncio.Future[Any]]", timeout: float, what: str) -> bool:
    """Cancel ``task`` and wait (at most ``timeout``) for it; True iff it has ended.

    Built on ``_bounded_wait`` so the existing "did not stop in time" logging is reused. It
    never swallows a cancellation of the CALLER (``asyncio.wait`` re-raises it), and it never
    returns True for a task that is still running -- a cancellation-resistant producer is the
    caller's to report."""
    if task is None:
        return True
    if not task.done():
        task.cancel()
        await _bounded_wait(task, timeout, what)
    if task.done() and not task.cancelled():
        task.exception()                    # mark retrieved: never an unobserved background failure
    return task.done()


class CloseOnce:
    """At-most-once close registry: a writer's close is ATTEMPTED once per process.

    ``shutdown()`` of a standalone runner can now be reached more than once (the supervisor's call, an
    explicit retry, and the late-finalization callback below), and a ``ParquetWriter.close()`` is not a
    safe thing to repeat: a second call after a failed first one would re-raise the latched storage
    failure and report it again, and re-running a half-finished finalization is not something this
    code has ever demonstrated to be safe. The first attempt's outcome is therefore recorded and every
    later request for the same writer returns it WITHOUT calling ``close()`` again.
    """

    def __init__(self) -> None:
        self._results: Dict[str, bool] = {}

    def attempted(self, name: str) -> bool:
        return name in self._results

    def run(self, name: str, close: Callable[[], bool]) -> bool:
        if name in self._results:
            return self._results[name]
        # Recorded BEFORE the call returns control to anything else: a re-entrant request (the close
        # path reporting a failure, a callback firing from a nested await) must see "attempted".
        self._results[name] = False
        self._results[name] = bool(close())
        return self._results[name]


class LateFinalizer:
    """Finalize storage when the LAST live user of it ends after ``shutdown()`` already returned.

    ``shutdown()`` never closes a writer beneath a live user (an ingest worker that swallowed its
    cancel, a book-recovery task that did) and returns early, latching an explicit shutdown failure.
    ``stop()`` and the supervisor's wait cannot make such a user end, and once ``shutdown()`` has
    returned nothing else would ever close what it held. This arms a done-callback on every still-live
    user; when one ends, ``finalize`` runs -- but only if ``users_stopped()`` says EVERY user is now
    stopped (the callback of one user never closes storage another still uses; that user's own end
    retries). ``finalize`` runs at most once even when callbacks overlap.

    It does not clear the latched failure (a late close is not a clean shutdown) and it cannot make a
    worker that never ends end: that worker keeps its storage open and the failure stays latched.
    """

    def __init__(self, *, venue: str, finalize: Callable[[], None],
                 users_stopped: Callable[[], bool],
                 watched: Callable[[], Iterable[Any]]) -> None:
        self.venue = venue
        self._finalize = finalize
        self._users_stopped = users_stopped
        self._watched = watched
        self._armed: set = set()
        #: Set once ``finalize`` has been started by a late callback (never reset).
        self.ran = False

    def arm(self) -> int:
        """Register on every watched task that is still alive and not yet armed (idempotent)."""
        count = 0
        for task in list(self._watched()):
            if task is None or task.done() or task in self._armed:
                continue
            self._armed.add(task)
            task.add_done_callback(self._on_user_ended)
            count += 1
        return count

    def _on_user_ended(self, task: Any) -> None:
        try:
            if not task.cancelled():
                error = task.exception()        # retrieved here: reported, never an unobserved failure
                if error is not None:
                    logger.error("late_storage_user_failed_after_shutdown", venue=self.venue,
                                 error=f"{type(error).__name__}: {error}")
        except Exception:  # noqa: BLE001 - observing a task must never raise into the loop
            pass
        self.retry()

    def retry(self) -> bool:
        """Run ``finalize`` if every user has stopped and it has not run yet. Never raises."""
        if self.ran:
            return False
        try:
            stopped = bool(self._users_stopped())
        except Exception as exc:  # noqa: BLE001 - an unreadable state must fail CLOSED (keep storage open)
            logger.error("late_finalization_state_check_failed", venue=self.venue,
                         error=f"{type(exc).__name__}: {exc}")
            return False
        if not stopped:
            return False                        # another user is alive: ITS end retries
        self.ran = True
        logger.info("late_shutdown_finalization", venue=self.venue)
        try:
            self._finalize()
        except Exception as exc:  # noqa: BLE001 - per-writer failures are reported inside finalize
            logger.error("late_shutdown_finalization_failed", venue=self.venue,
                         error=f"{type(exc).__name__}: {exc}")
        return True


async def supervise_standalone_runner(app: Any, stop: asyncio.Event, *,
                                      task_timeout_s: float = SHUTDOWN_TASK_TIMEOUT_S) -> int:
    """Run ``app`` until a signal, a terminal storage failure, or a crash; shut
    it down in a controlled way; return the process exit status.

    ``app`` provides ``run()`` and ``shutdown()`` coroutines and a
    ``failure_policy`` (``StandaloneFailurePolicy``). The application task is
    ALWAYS observed: before this existed ``_main`` abandoned it, so a task that
    ended with ``FatalStorageError`` was invisible and the process lingered
    while collecting nothing.

    Exit status: ``EXIT_FATAL_STORAGE`` if a TERMINATE verdict latched (whatever
    else happened), else ``EXIT_RUN_TASK_CRASHED`` if the task died of an
    ordinary exception, else 0 for a signalled stop. Never ``os._exit``: the
    caller returns this from the main thread.
    """
    policy: StandaloneFailurePolicy = app.failure_policy
    run_task = asyncio.ensure_future(app.run())
    stop_task = asyncio.ensure_future(stop.wait())
    terminal_task = asyncio.ensure_future(policy.terminal_event.wait())
    crashed = False
    try:
        # stop_task / terminal_task stay in the waiting set for the whole loop,
        # so it only ends through one of the explicit ``break``s below.
        waiting = {run_task, stop_task, terminal_task}
        while True:
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            if policy.terminal_failure is not None or stop.is_set():
                break
            # Neither a signal nor a latched failure: the application task ended.
            waiting.discard(run_task)
            if run_task.cancelled():
                logger.error("runner_task_cancelled_unexpectedly", venue=policy.venue)
                crashed = True
                break
            exc = run_task.exception()
            if isinstance(exc, FatalStorageError):
                # Nobody classified it (the client only re-raises when it has no
                # hook): default-deny, it is terminal.
                policy.on_fatal(exc, origin="run_task")
                break
            if exc is not None:
                logger.error("runner_task_crashed", venue=policy.venue,
                             error=f"{type(exc).__name__}: {exc}")
                crashed = True
                break
            # Normal completion with no stop requested (for example the client's
            # reconnect budget ran out). The pre-existing behaviour -- keep
            # waiting for a signal -- is kept, but it is no longer silent.
            logger.error("runner_task_ended_without_stop_request", venue=policy.venue)
    finally:
        # Controlled shutdown, in this task (never the worker). Each step is
        # guarded: a failure in one must not prevent the next.
        for helper in (stop_task, terminal_task):
            if not helper.done():
                helper.cancel()
        try:
            await app.shutdown()
        except Exception as exc:  # noqa: BLE001 - shutdown is best-effort per writer; keep going
            logger.error("runner_shutdown_failed", venue=policy.venue,
                         error=f"{type(exc).__name__}: {exc}")
        if not run_task.done():
            run_task.cancel()
        await _bounded_wait(run_task, task_timeout_s, "run_task")
        if run_task.done() and not run_task.cancelled():
            late = run_task.exception()     # observe, so it is never "never retrieved"
            if isinstance(late, FatalStorageError) and policy.terminal_failure is None:
                policy.on_fatal(late, origin="run_task")
            elif late is not None:
                logger.error("runner_task_failed_during_shutdown", venue=policy.venue,
                             error=f"{type(late).__name__}: {late}")
                if isinstance(late, WorkerRetirementError):
                    policy.record_shutdown_failure("ws_client_worker_not_retired")
        await asyncio.gather(stop_task, terminal_task, return_exceptions=True)
    if policy.terminal_failure is not None:
        return EXIT_FATAL_STORAGE
    return EXIT_RUN_TASK_CRASHED if (crashed or policy.shutdown_failure is not None) else 0
