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

__all__ = [
    "EXIT_FATAL_STORAGE", "EXIT_RUN_TASK_CRASHED", "SHUTDOWN_TASK_TIMEOUT_S",
    "DEDUP_DEFAULT_STREAM", "ROUTE_TAG_ATTRIBUTE", "tag_route", "build_stream_table",
    "StandaloneFailurePolicy", "supervise_standalone_runner",
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

    # -- classification ------------------------------------------------------

    @property
    def exit_code(self) -> int:
        return EXIT_FATAL_STORAGE if self.terminal_failure is not None else 0

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
        if self._reporting:
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
            self._reporting = False

    # -- route isolation -----------------------------------------------------

    def route_is_isolated(self, route: Optional[str]) -> bool:
        return route in self.isolated_routes

    def note_short_circuit(self, route: str) -> None:
        self.route_short_circuits[route] = self.route_short_circuits.get(route, 0) + 1


async def _bounded_wait(task: "asyncio.Future[Any]", timeout: float, what: str) -> None:
    """Wait for ``task`` without ever blocking forever, and retrieve its outcome
    so it can never become an unobserved background task."""
    if task.done():
        return
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        logger.error("runner_task_did_not_stop_in_time", task=what, timeout_s=timeout)


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
        await asyncio.gather(stop_task, terminal_task, return_exceptions=True)
    if policy.terminal_failure is not None:
        return EXIT_FATAL_STORAGE
    return EXIT_RUN_TASK_CRASHED if crashed else 0
