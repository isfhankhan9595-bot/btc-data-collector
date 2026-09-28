"""P0-4: segment-granularity exact dedup -- crash state machine, memory bound,
identity isolation, replay isolation, fail-closed behaviour.

Everything here runs the REAL ``ParquetWriter`` (segment publication, orphan
recovery) and the REAL ``ExchangeAdapter`` dedup seam. A "crash" is simulated
by abandoning the writer/index objects without close() and releasing only the
OS-level stream lock (what process death does), then constructing fresh
objects on the same directory (a restart).
"""
from __future__ import annotations

import json

import pyarrow as pa
import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.collector.segment_dedup import (
    UNIDENTIFIED, DedupStateError, SegmentDedupCoordinator, SegmentDedupIndex, dedup_identity_key,
)

SCHEMA = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"
STREAM = "dedup_test_trades"


def key_for(trade_id, *, exchange="BINANCE", market="linear_perpetual", instr=INSTR, stream="trades"):
    return dedup_identity_key(exchange, market, instr, stream, trade_id)


def row_identity(row):
    if row["trade_id"] is None:
        return None
    return key_for(row["trade_id"], instr=row["instrument_key"] or UNIDENTIFIED)


class Pipe:
    """A runner-shaped wiring of coordinator + real writer on one directory."""

    def __init__(self, base, *, segment_rows=5_000, defer_commit=False, hook=None):
        self.base, self.segment_rows = str(base), segment_rows
        self.events, self.deferred = [], []
        self.defer_commit, self.custom_hook = defer_commit, hook
        self.start()

    def start(self):
        self.index = SegmentDedupIndex(self.base + "/dedup.sqlite3")
        self.co = SegmentDedupCoordinator(self.index, row_identity)
        self.writer = ParquetWriter(STREAM, SCHEMA, base_dir=self.base, segment_rows=self.segment_rows,
                                    segment_seconds=3600, quality_event_sink=self.events.append,
                                    on_segment_published=self._hook)
        self.co.startup_reconcile(self.writer.stream_dir)   # before ingestion resumes

    def _hook(self, token, path):
        if self.custom_hook is not None:
            return self.custom_hook(self, token, path)
        if self.defer_commit:
            self.deferred.append((token, path))
            return None
        return self.co.on_segment_published(token, path)

    def offer(self, trade_id, *, instr=INSTR):
        k = key_for(trade_id, instr=instr)
        if not self.co.check_and_admit(k):
            return False
        self.writer.write({"timestamp": 1, "instrument_key": instr, "trade_id": trade_id},
                          bind=lambda t: self.co.note_written(k, t))
        self.co.end_message()
        return True

    def crash(self):
        self.writer._release_lock()      # process death releases the OS lock, nothing else
        self.index.close()

    def restart(self):
        self.crash()
        self.events.clear()
        self.start()


@pytest.fixture
def pipe(tmp_path):
    return Pipe(tmp_path)


# --- crash state machine (task states A-G) --------------------------------

def test_state_A_pending_only_is_accepted_again_after_restart(pipe):
    assert pipe.offer("A")
    pipe.restart()
    assert pipe.offer("A"), "nothing durable existed -> redelivery must be accepted"


def test_state_B_unpublished_tmp_segment_is_dropped_and_identity_not_indexed(pipe):
    assert pipe.offer("A")
    pipe.writer.flush()                                   # rows inside the open .tmp writer
    assert list(pipe.writer.stream_dir.glob("*.seg.tmp"))
    pipe.restart()
    assert any(e["event_type"] == "DATA_DROP" for e in pipe.events), "orphan discard must be observable"
    assert pipe.index.identity_count() == 0, "an unpublished segment must never create a durable identity"
    assert pipe.offer("A")


def test_state_C_published_but_index_not_committed_is_reconciled_at_startup(tmp_path):
    p = Pipe(tmp_path, defer_commit=True)                 # crash before the index commit
    assert p.offer("A")
    p.writer.close()                                      # segment durably published
    assert p.deferred and p.index.identity_count() == 0
    p.restart(); p.defer_commit = False
    assert p.index.identity_count() == 1, "startup reconciliation must index the published segment"
    assert not p.offer("A"), "redelivery after restart must be suppressed"


def test_state_D_committed_then_crash_restart_is_idempotent(pipe):
    assert pipe.offer("A")
    pipe.writer.close()
    assert pipe.index.identity_count() == 1
    pipe.restart()
    assert pipe.co.startup_reconcile(pipe.writer.stream_dir) == 0     # nothing left to do
    assert pipe.co.startup_reconcile(pipe.writer.stream_dir) == 0     # and again
    assert not pipe.offer("A") and pipe.index.identity_count() == 1


def test_state_E_crash_during_index_transaction_is_atomic_and_rerunnable(tmp_path):
    p = Pipe(tmp_path, defer_commit=True)
    p.offer("A"); p.offer("B")
    p.writer.close()
    _, seg = p.deferred[0]
    import sqlite3

    class Boom:
        def __init__(self, conn): self._c = conn
        def execute(self, *a): return self._c.execute(*a)
        def executemany(self, *a): raise sqlite3.OperationalError("simulated crash mid-transaction")
    real = p.index._conn
    p.index._conn = Boom(real)
    with pytest.raises(DedupStateError):
        p.co.index.commit_segment("x/y.seg", [key_for("A"), key_for("B")])
    p.index._conn = real
    assert p.index.identity_count() == 0 and not p.index.is_segment_reconciled("x/y.seg")
    p.restart(); p.defer_commit = False
    assert p.index.identity_count() == 2 and not p.offer("A") and not p.offer("B")


def test_state_F_corrupt_database_fails_closed(tmp_path):
    (tmp_path / "dedup.sqlite3").write_bytes(b"not a sqlite database" * 50)
    with pytest.raises(DedupStateError):
        SegmentDedupIndex(str(tmp_path / "dedup.sqlite3"))


def test_state_F_unavailable_index_never_maps_to_new_or_duplicate(pipe):
    pipe.offer("A")
    pipe.index.close()
    with pytest.raises(DedupStateError):
        pipe.co.check_and_admit(key_for("Z"))            # neither True (new) nor False (dup)


def test_state_G_duplicate_in_post_publication_pre_index_window_is_suppressed(tmp_path):
    p = Pipe(tmp_path, defer_commit=True)
    assert p.offer("A")
    p.writer.close()                                      # published; commit deferred (window open)
    assert not p.offer("A"), "pending RAM authority must still cover the window"
    token, seg = p.deferred[0]
    p.co.on_segment_published(token, seg)                # window closes
    assert p.co.ram_identity_count == 0 and not p.offer("A"), "now the index covers it"


# --- ordering / segment semantics ------------------------------------------

def test_hook_runs_only_after_the_segment_is_durably_published(tmp_path):
    seen = {}
    def hook(pipe, token, path):
        seen["final_exists"] = path.exists()
        seen["tmp_exists"] = (path.parent / (path.name + ".tmp")).exists()
    p = Pipe(tmp_path, hook=hook)
    p.offer("A"); p.writer.close()
    assert seen == {"final_exists": True, "tmp_exists": False}


def test_release_happens_only_after_the_index_commit(tmp_path):
    p = Pipe(tmp_path)
    p.offer("A")
    order = []
    real_commit = p.index.commit_segment
    p.index.commit_segment = lambda *a, **k: (order.append(("commit", p.co.ram_identity_count)), real_commit(*a, **k))[1]
    p.writer.close()
    assert order == [("commit", 2)], "identity must still be in RAM at the moment of the commit"
    assert p.co.ram_identity_count == 0


def test_hook_failure_fails_closed_on_the_next_write(tmp_path):
    def bad(pipe, token, path):
        raise DedupStateError("disk full")
    p = Pipe(tmp_path, segment_rows=2, hook=bad)
    p.offer("A"); p.offer("B")                            # rotation -> publication -> hook raises
    assert any(e["event_type"] == "DEDUP_STATE_FAILED" for e in p.events)
    with pytest.raises(RuntimeError):
        p.offer("C")


def test_hour_rollover_attributes_identity_to_the_segment_that_receives_it(tmp_path, monkeypatch):
    p = Pipe(tmp_path)
    p.writer.current_hour = "2026-01-01-00"
    before = p.writer.current_segment_token()
    monkeypatch.setattr(p.writer, "_get_current_hour_str", lambda: "2026-01-01-01")
    p.offer("A")                                           # the hour changes inside this very write()
    token = p.co._pending_index[key_for("A")]
    assert before[0] == "2026-01-01-00" and token[0] == "2026-01-01-01", \
        "admission-time tagging would have used the OLD segment; bind() must follow the receiving one"


def test_unwritten_admitted_identity_is_forgotten_not_leaked(pipe):
    k = key_for("REJECTED")
    assert pipe.co.check_and_admit(k)                     # admitted, validator then rejects the row
    pipe.co.end_message()
    assert pipe.co.ram_identity_count == 0
    assert pipe.co.check_and_admit(k), "never durable -> redelivery must be accepted"


# --- exactness across boundaries -------------------------------------------

def test_duplicate_within_open_segment_and_across_rotation_and_after_restart(tmp_path):
    p = Pipe(tmp_path, segment_rows=10)
    assert p.offer("A") and not p.offer("A")               # open segment
    for i in range(30):
        p.offer(f"T{i}")                                   # several rotations
    assert not p.offer("A") and not p.offer("T5")          # historical -> index
    p.writer.close(); p.restart()
    assert not p.offer("A") and not p.offer("T29")         # after restart
    assert p.offer("NEW")


def test_reconnect_overlap_A_B_C_then_B_C_D(pipe):
    accepted = [t for t in ["A", "B", "C", "B", "C", "D"] if pipe.offer(t)]
    assert accepted == ["A", "B", "C", "D"]


def test_repeated_reconciliation_is_idempotent(tmp_path):
    p = Pipe(tmp_path, defer_commit=True)
    for t in "ABC":
        p.offer(t)
    p.writer.close(); p.restart(); p.defer_commit = False
    n = p.index.identity_count()
    assert p.co.startup_reconcile(p.writer.stream_dir) == 0
    assert p.index.identity_count() == n == 3


def test_missing_index_is_rebuilt_from_published_segments(tmp_path):
    p = Pipe(tmp_path)
    for t in "ABC":
        p.offer(t)
    p.writer.close(); p.crash()
    for f in tmp_path.glob("dedup.sqlite3*"):
        f.unlink()
    p.start()
    assert p.index.identity_count() == 3 and not p.offer("B")


def test_unreadable_published_segment_fails_closed_at_startup(tmp_path):
    p = Pipe(tmp_path)
    p.offer("A"); p.writer.close(); p.crash()
    seg = next(p.writer.stream_dir.glob("*.seg"))
    seg.write_bytes(b"garbage")
    for f in tmp_path.glob("dedup.sqlite3*"):
        f.unlink()
    with pytest.raises(DedupStateError):
        p.start()


def test_zero_trade_segment_and_none_ids_are_handled(tmp_path):
    p = Pipe(tmp_path)
    p.writer.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": None})
    p.writer.write({"timestamp": 2, "instrument_key": INSTR, "trade_id": None})
    p.writer.close(); p.restart()
    assert p.index.identity_count() == 0 and p.index.is_segment_reconciled(f"{STREAM}/" + next(p.writer.stream_dir.glob("*.seg")).name)


# --- identity isolation -----------------------------------------------------

def test_identity_isolation_across_every_component():
    base = key_for("1")
    assert len({base, key_for("1", exchange="BYBIT"), key_for("1", market="spot"),
                key_for("1", instr="BINANCE|linear_perpetual|ETH-USDT|ETHUSDT"),
                key_for("1", stream="trades-all"), key_for("12"), key_for("1", instr=UNIDENTIFIED)}) == 7


def test_identity_encoding_is_structurally_unambiguous():
    assert dedup_identity_key("A", "B", "C", "D", "E") != dedup_identity_key("A", "B", "C", "D:E", "")
    assert dedup_identity_key("1:A", "B", "C", "D", "E") != dedup_identity_key("1", "A", "B", "C", "D")


# --- memory bound -----------------------------------------------------------

def test_ram_is_bounded_by_open_segment_not_by_process_lifetime(tmp_path):
    p = Pipe(tmp_path, segment_rows=50)
    peak = 0
    for i in range(2_000):
        assert p.offer(f"ID{i}")
        peak = max(peak, p.co.ram_identity_count)
    assert peak <= 2 * 51, f"RAM identity count grew with lifetime: peak={peak}"   # each id counted in 2 structures
    assert p.co.ram_segment_token_count <= 2, "per-segment registries must be released, not accumulated"
    assert p.index.identity_count() >= 1_950
    assert all(not p.offer(f"ID{i}") for i in range(0, 2_000, 37)), "exactness must survive the release"


# --- adapter seam & replay isolation ---------------------------------------

def _agg(ts, agg_id):
    return {"stream": "btcusdt@aggTrade", "data": {"E": ts, "a": agg_id, "p": "100", "q": "1", "m": False}}


def test_adapter_with_backend_never_touches_the_lifetime_set(tmp_path):
    p = Pipe(tmp_path)
    adapter = BinanceAdapter()
    adapter.set_trade_dedup(p.co)
    out = [adapter.normalize(_agg(1000 + i, a), local_receive_ts=1000 + i) for i, a in enumerate([1, 2, 1, 3, 2])]
    assert [len(x) for x in out] == [1, 1, 0, 1, 0]
    assert adapter._seen_trade_ids == set(), "the unbounded lifetime set must not be used with a backend"


def test_default_adapter_and_replay_do_not_inherit_persistent_live_state(tmp_path):
    p = Pipe(tmp_path)
    live = BinanceAdapter(); live.set_trade_dedup(p.co)
    live.normalize(_agg(1000, 7), local_receive_ts=1000)
    p.writer.close()                                       # live index now knows aggTrade 7
    frame = ReplayFrame(timestamp_ms=1000, kind=FrameKind.WIRE, source_index=0, payload=json.dumps(_agg(1000, 7)))
    engine = ReplayEngine("BINANCE")
    assert engine.adapter._trade_dedup is None
    a = engine.run(ReplaySource([frame])).non_book_events
    b = ReplayEngine("BINANCE").run(ReplaySource([frame])).non_book_events
    assert len(a) == len(b) == 1, "replay must not be suppressed by a previous live run's durable state"
