"""Child process for test_ws_shutdown_runner_exit.py: the REAL ``run_collector.main()`` (so the real
``asyncio.run``) with ONE REST producer that swallows every cancel until a flag file appears.

Only the network edge is replaced (websocket client double, producer, quiet neighbours); the
signal handler, ``_async_shutdown``, ``start()`` and ``shutdown()`` are the production code.
Usage: python _runner_exit_child.py <startup_recovery|oi_poller> <release-flag-path>
"""
import asyncio
import os
import sys

os.environ["COLLECTOR_ALLOW_UNVERIFIED_FS"] = "1"
import run_collector  # noqa: E402

MODE, FLAG = sys.argv[1], sys.argv[2]


class _Ws:
    url = "ws://fake"
    shutdown_deadline_s = 0.05
    worker_retire_timeout_s = 0.05
    lifecycle_idle = True
    ingress_stopped = True

    def stop(self):
        pass

    async def start(self):
        await asyncio.Event().wait()

    async def wait_connected(self, timeout_seconds=0.0):
        return True

    async def wait_stopped(self, timeout_s):
        return True


async def _quiet(*a, **k):
    await asyncio.Event().wait()


async def _stubborn(*a, **k):
    print("PRODUCER_ENTERED", flush=True)
    while not os.path.exists(FLAG):                  # ends only when the parent says so
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass                                      # the cancel is swallowed, every time


def _states(app):
    return ("shutdown_failure=%s exit_code=%s shutdown_done=%s raw_rest_closed=%s oi_closed=%s quality_closed=%s" % (
        getattr(app, "shutdown_failure", None), app.exit_code, getattr(app, "_shutdown_done", False),
        app.raw_rest_writer._closed, app.oi_writer._closed, app.quality_writer._closed))


def _factory():
    run_collector.WS_FALLBACK_SLACK_S = 0.1
    app = run_collector.CollectorApp()
    app.ws_clients = [_Ws()]

    async def no_preflight():
        return False
    app._storage_pressure_preflight_failed = no_preflight
    app.health_monitor.start = _quiet
    app._storage_pressure_loop = _quiet
    app._verify_startup_streams = _quiet
    if MODE == "startup_recovery":
        app._recover_binance_book = _stubborn
        app._poll_openinterest = _quiet
    else:
        async def recovered(reason):
            return True
        app._recover_binance_book = recovered
        app._poll_openinterest = _stubborn
    real_shutdown = app._async_shutdown

    async def reporting_shutdown(*args, **kwargs):
        await real_shutdown(*args, **kwargs)
        starts = [t for t in asyncio.all_tasks() if t.get_coro().__qualname__.endswith("CollectorApp.start")]
        print("SHUTDOWN_TASK_DONE start_done=%s %s" % (all(t.done() for t in starts), _states(app)), flush=True)
    app._async_shutdown = reporting_shutdown        # looked up at signal time by the real handler
    _factory.app = app
    return app


code = run_collector.main(_factory)
print("MAIN_RETURNED code=%s %s" % (code, _states(_factory.app)), flush=True)
sys.exit(code)
