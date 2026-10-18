"""Child process for test_event_loop_teardown.py: a REAL runner entry point, a fake socket, a controllable worker.

Everything the production process does is real (signal handlers, supervisor, ``shutdown()``, late finalizer,
the bounded event-loop teardown, the exit status). Only the network is replaced and the ingest callback is
swapped for a worker that can swallow cancellation. Evidence goes to stdout as ``EV {json}`` lines, flushed
immediately, so it survives a parent-side SIGKILL. It only ever writes under ``--data-dir`` (a temp dir).

Usage: _teardown_child.py VENUE DATA_DIR [--resist none|finite|forever|second_cancel] [--resist-s S]
       [--close-fail WRITER_ATTR] [--teardown S]       VENUE: bybit | spot | okx | okx_capture
"""
import argparse
import asyncio
import datetime
import importlib
import json
import os
import signal
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.dirname(os.path.dirname(HERE)), os.path.dirname(HERE)]
os.environ.setdefault("COLLECTOR_ALLOW_UNVERIFIED_FS", "1")

ap = argparse.ArgumentParser()
ap.add_argument("venue", choices=["bybit", "spot", "okx", "okx_capture"])
ap.add_argument("data_dir")
ap.add_argument("--resist", default="none", choices=["none", "finite", "forever", "second_cancel"])
ap.add_argument("--resist-s", type=float, default=0.0)
ap.add_argument("--close-fail", default="")
ap.add_argument("--teardown", type=float, default=1.5)
A = ap.parse_args()

T0 = time.monotonic()
def ev(kind, **kw):
    kw.update(ev=kind, t=round(time.monotonic() - T0, 3))
    print("EV " + json.dumps(kw, default=str), flush=True)

import pyarrow as pa  # noqa: E402
import collector.collector.websocket_client as wsc  # noqa: E402

try:  # absent on a tree without the bounded teardown: the same scenarios then run against plain asyncio.run
    import collector.collector.loop_teardown as lt  # noqa: E402
    lt.TEARDOWN_DEADLINE_S = A.teardown
except ImportError:
    lt = None
SPEC = {
    "bybit": ("run_bybit_collector", "BybitCollectorApp", ["ob_writer", "trades_writer", "mark_writer", "oi_writer", "liq_writer"]),
    "spot": ("run_binance_spot_collector", "BinanceSpotCollectorApp", ["trades_writer", "ob_writer"]),
    "okx": ("run_okx_collector", "OKXCollectorApp", ["trades_writer", "mark_writer", "oi_writer", "liq_writer"]),
    "okx_capture": ("run_okx_capture", "OKXCaptureApp", []),
}
modname, clsname, MARKET = SPEC[A.venue]
mod = importlib.import_module(modname)
Cls = getattr(mod, clsname)
#: writers only the receive loop can reach (documented): closing them with a live worker is by design
INGRESS_ONLY = {"raw_wire_writer", "raw_rest_writer"}


def row_for(schema):
    now = datetime.datetime.now(datetime.timezone.utc)
    out = {}
    for f in schema:
        t = f.type
        out[f.name] = (now if pa.types.is_timestamp(t) else 1 if pa.types.is_integer(t) else 1.0 if pa.types.is_floating(t)
                       else True if pa.types.is_boolean(t) else [] if pa.types.is_list(t) else "x")
    return out


class Conn:
    async def __aenter__(self): return self
    async def __aexit__(self, *e): return False
    async def send(self, _m): return None
    def __aiter__(self):
        async def gen():
            yield json.dumps({"n": 1})
            await asyncio.Event().wait()
        return gen()


wsc.websockets.connect = lambda url, **k: Conn()
S = dict(running=False, cancels=0, first=None, app=None)


async def handler(data, ts, connection_id=None):
    if S["running"]:
        return
    S["running"] = True
    app = S["app"]
    ev("worker_entered")
    for name in MARKET:
        w = getattr(app, name)
        for _ in range(3):
            w.write(row_for(w.schema))
    try:
        app._persist_quality_event({"exchange": "X", "stream": "probe", "event_type": "ERROR", "reason": "probe"})
    except Exception:
        pass
    asyncio.get_running_loop().call_later(0.25, os.kill, os.getpid(), signal.SIGTERM)
    try:
        while True:
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                S["cancels"] += 1
                ev("worker_cancel", n=S["cancels"], swallowed=A.resist != "none")
                if A.resist == "none" or (A.resist == "second_cancel" and S["cancels"] >= 2):
                    raise
                if S["first"] is None:
                    S["first"] = time.monotonic()
            if A.resist == "finite" and S["first"] and time.monotonic() - S["first"] >= A.resist_s:
                return
    finally:
        S["running"] = False
        ev("worker_exited")


orig_init = Cls.__init__


def init(self, *a, **k):
    orig_init(self, *a, **k)
    S["app"] = self
    client = self.client if A.venue != "okx_capture" else self.capture.client
    client.on_message, client._on_message_takes_connection = handler, True
    client.shutdown_drain_timeout_s, client.worker_retire_timeout_s = 0.2, 0.3
    if A.venue == "spot":
        async def no_rest(_r):
            return True
        self._recover_book = no_rest
    for n in MARKET + ["quality_writer", "raw_wire_writer", "raw_rest_writer"]:
        w = getattr(self, n, None)
        if w is None:
            continue
        def close(n=n, real=w.close):
            ev("close", writer=n, worker_running=S["running"], ingress_only=n in INGRESS_ONLY)
            if n == A.close_fail:
                raise OSError(28, "injected: no space left on device")
            return real()
        def write(*aa, n=n, w=w, real=w.write, **kk):
            if getattr(w, "_closed", False):
                ev("write_after_close", writer=n)
            return real(*aa, **kk)
        w.close, w.write = close, write


Cls.__init__ = init
ev("start", py=sys.version.split()[0])
if A.venue == "okx_capture":
    mod.TERMINAL_STOP_GRACE_S = 0.5
    rc = mod.main(["--forever", "--data-dir", A.data_dir, "--url", "wss://fake.invalid"])
elif hasattr(mod, "_entry"):
    rc = mod._entry(A.data_dir, "wss://fake.invalid/ws")
else:
    rc = asyncio.run(mod._main(A.data_dir, "wss://fake.invalid/ws"))
ev("returned", rc=rc, worker_running=S["running"])
raise SystemExit(rc)
