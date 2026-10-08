"""C1: the persistent dedup index must be exactly auditable against the confirmed segments.

Invariant under test (for every confirmed segment S):

    identities owned by S in ``seen``  ==  identities actually contained in S's bytes

Evidence chain: segment bytes -> marker (sha256/size) -> identity evidence file
(count + order-independent digest, bound to the marker) -> ``seen`` membership and
the ``reconciled_segments`` row. SQLite alone is never the reference.

Everything drives the real ``ParquetWriter`` / ``SegmentDedupCoordinator`` /
``SegmentDedupIndex``. Corruption is injected by editing the SQLite file / evidence
files directly while the process is "down" (the Rig's ``crash()``), then restarting.
Power-loss is not simulated here; process death is (``Crash`` is a BaseException).
"""
from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector import publication as pub
from collector.collector import segment_dedup as sd
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.publication import DEEP_VERIFY_ENV, UNVERIFIED_FS_ENV, marker_path, read_marker
from collector.collector.segment_dedup import (
    UNIDENTIFIED, DedupStateError, SegmentDedupCoordinator, SegmentDedupIndex, dedup_identity_key,
    identity_set_digest,
)

SCHEMA = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"
STREAM = "c1_trades"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv(UNVERIFIED_FS_ENV, "1")
    monkeypatch.delenv(DEEP_VERIFY_ENV, raising=False)


@pytest.fixture
def fault_mp():
    mp = pytest.MonkeyPatch()
    try:
        yield mp
    finally:
        mp.undo()


class Crash(BaseException):
    """Simulated process death (never caught by ``except Exception``)."""


def key_for(trade_id, instr=INSTR):
    return dedup_identity_key("BINANCE", "linear_perpetual", instr, "trades", trade_id)


def row_identity(row):
    if row["trade_id"] is None:
        return None
    return key_for(row["trade_id"], row["instrument_key"] or UNIDENTIFIED)


class Rig:
    def __init__(self, base, *, segment_rows=2):
        self.base, self.segment_rows = str(base), segment_rows
        self.events = []
        self.writer = self.index = self.co = None
        self.start()

    def start(self):
        self.index = SegmentDedupIndex(self.base + "/dedup.sqlite3")
        self.co = SegmentDedupCoordinator(self.index, row_identity)
        self.writer = ParquetWriter(STREAM, SCHEMA, base_dir=self.base, segment_rows=self.segment_rows,
                                    segment_seconds=3600, quality_event_sink=self.events.append,
                                    on_segment_published=self.co.on_segment_published)
        self.co.startup_reconcile(self.writer.stream_dir)

    def offer(self, trade_id):
        k = key_for(trade_id)
        if not self.co.check_and_admit(k):
            return False
        self.writer.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": trade_id},
                          bind=lambda t: self.co.note_written(k, t))
        self.co.end_message()
        return True

    def force(self, trade_id):
        """Write a row WITHOUT consulting dedup (a trade the live path would have rejected)."""
        self.writer.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": trade_id})

    def crash(self):
        self.writer._release_lock()
        self.index.close()

    def restart(self):
        self.crash()
        self.events.clear()
        self.start()

    @property
    def db(self):
        return self.base + "/dedup.sqlite3"

    @property
    def stream_dir(self):
        return self.writer.stream_dir

    def segs(self):
        return sorted(self.stream_dir.glob("*.seg"))

    def seg_key(self, seg):
        return f"{STREAM}/{Path(seg).name}"


def five(tmp_path):
    """A, B | C, D | E  -> three published, indexed segments."""
    r = Rig(tmp_path)
    for t in "ABCDE":
        assert r.offer(t)
    r.writer.close()
    assert len(r.segs()) == 3
    return r


def tamper(r, *statements):
    """Edit the SQLite index while the process is down."""
    r.crash()
    conn = sqlite3.connect(r.db)
    try:
        for sql, *params in statements:
            conn.execute(sql, params[0] if params else ())
        conn.commit()
    finally:
        conn.close()


def sql_one(r, sql, params=()):
    conn = sqlite3.connect(r.db)
    try:
        return conn.execute(sql, params).fetchone()
    finally:
        conn.close()


def seg_id(r, seg):
    return sql_one(r, "SELECT segment_id FROM reconciled_segments WHERE segment_key=?", (r.seg_key(seg),))[0]


def assert_membership_exact(r):
    """THE property: for every confirmed segment, persistent membership == identities in its bytes."""
    conn = sqlite3.connect(r.db)
    try:
        total = 0
        for seg in r.segs():
            want = {key_for(t) for t in pq.read_table(seg).column("trade_id").to_pylist() if t is not None}
            got = {k for (k,) in conn.execute(
                "SELECT identity_key FROM seen JOIN reconciled_segments USING(segment_id) WHERE segment_key=?",
                (r.seg_key(seg),))}
            assert got == want, f"{seg.name}: membership {sorted(got)} != segment {sorted(want)}"
            total += len(want)
        assert conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0] == total
        assert conn.execute("SELECT COUNT(*) FROM reconciled_segments").fetchone()[0] == len(r.segs())
        assert conn.execute("SELECT COUNT(*) FROM seen WHERE segment_id NOT IN "
                            "(SELECT segment_id FROM reconciled_segments)").fetchone()[0] == 0
    finally:
        conn.close()


def assert_reports_rebuild(r, fragment=None):
    rep = r.co.last_report
    assert rep.rebuilt, "corruption was NOT detected"
    if fragment is not None:
        assert any(fragment in x for x in rep.rebuild_reasons), rep.rebuild_reasons
    assert_membership_exact(r)


def evidence_file(r, seg):
    return r.co._evidence_path(r.seg_key(seg))


# ===================================================================== baseline: the property holds after a normal run

def test_normal_run_membership_is_exact_and_evidence_is_bound(tmp_path):
    r = five(tmp_path)
    assert_membership_exact(r)
    for seg in r.segs():
        ev = json.loads(evidence_file(r, seg).read_text())
        m = read_marker(seg).publication
        assert (ev["segment_sha256"], ev["segment_size"]) == (m["sha256"], m["size_bytes"])
        want = [key_for(t) for t in pq.read_table(seg).column("trade_id").to_pylist()]
        assert (ev["identity_count"], ev["identity_digest"]) == identity_set_digest(want)


# ===================================================================== 1-4, 6: the five hostile index states

def test_1_deleted_identity_is_detected_and_repaired(tmp_path):
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("A"),)))
    r.start()
    assert_reports_rebuild(r, "differs")
    assert r.index.contains(key_for("A")) and not r.offer("A"), "the deleted trade is suppressed again"


def test_1b_every_identity_of_a_segment_deleted_is_detected(tmp_path):
    """Segment 3 holds only E: with its rows gone it has no membership entry at all (nothing to compare)."""
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("E"),)))
    r.start()
    assert_reports_rebuild(r, "has no identities")
    assert not r.offer("E")


def test_2_fake_identity_is_detected_and_repaired(tmp_path):
    r = five(tmp_path)
    sid = seg_id(r, r.segs()[0])
    tamper(r, ("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)", (sid, key_for("NEVER-SEEN"))))
    r.start()
    assert_reports_rebuild(r, "differs")
    assert not r.index.contains(key_for("NEVER-SEEN")) and r.offer("NEVER-SEEN"), "a real trade is no longer rejected"


def test_3_swapped_identity_with_identical_row_count_is_detected(tmp_path):
    r = five(tmp_path)
    before = r.index.identity_count()
    tamper(r, ("UPDATE seen SET identity_key=? WHERE identity_key=?", (key_for("FAKE"), key_for("A"))))
    assert sql_one(r, "SELECT COUNT(*) FROM seen")[0] == before, "row count and per-segment counts are unchanged"
    r.start()
    assert_reports_rebuild(r, "differs")
    assert r.index.contains(key_for("A")) and not r.index.contains(key_for("FAKE"))


@pytest.mark.parametrize("column,value", [("identity_count", "identity_count + 1"), ("identity_count", "0"),
                                          ("identity_digest", "'" + "0" * 64 + "'")])
def test_4_wrong_recorded_count_or_digest_is_detected(tmp_path, column, value):
    r = five(tmp_path)
    sid = seg_id(r, r.segs()[1])
    tamper(r, (f"UPDATE reconciled_segments SET {column}={value} WHERE segment_id=?", (sid,)))
    r.start()
    assert_reports_rebuild(r, "differs from segment evidence")


def test_5_same_identity_owned_by_two_segments_is_detected(tmp_path):
    r = five(tmp_path)
    s1, s2 = (seg_id(r, s) for s in r.segs()[:2])
    tamper(r, ("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)", (s2, key_for("A"))))
    assert sql_one(r, "SELECT COUNT(DISTINCT segment_id) FROM seen WHERE identity_key=?", (key_for("A"),))[0] == 2
    r.start()
    assert_reports_rebuild(r)
    assert sql_one(r, "SELECT COUNT(*) FROM seen WHERE identity_key=?", (key_for("A"),))[0] == 1


def test_5b_forged_consistent_duplicate_across_segments_is_still_caught(tmp_path):
    """Evidence + SQLite forged together so that segment 2 'legitimately' claims A too: every per-segment digest
    agrees, so only the ownership (one identity, one segment) check can see it."""
    r = five(tmp_path)
    seg2 = r.segs()[1]
    forged = [key_for(t) for t in ("C", "D", "A")]
    count, digest = identity_set_digest(forged)
    ev_path = evidence_file(r, seg2)
    ev = json.loads(ev_path.read_text())
    ev["identity_count"], ev["identity_digest"] = count, digest
    sid = seg_id(r, seg2)
    tamper(r, ("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)", (sid, key_for("A"))),
           ("UPDATE reconciled_segments SET identity_count=?, identity_digest=? WHERE segment_id=?", (count, digest, sid)))
    ev_path.write_text(json.dumps(ev))
    r.start()
    assert_reports_rebuild(r, "owned by several segments")
    assert sql_one(r, "SELECT COUNT(*) FROM seen WHERE identity_key=?", (key_for("A"),))[0] == 1


def test_6b_swapped_ownership_with_identical_per_segment_counts_is_detected(tmp_path):
    r = five(tmp_path)
    s1, s2 = (seg_id(r, s) for s in r.segs()[:2])
    tamper(r, ("UPDATE seen SET segment_id=? WHERE identity_key=?", (s2, key_for("A"))),
           ("UPDATE seen SET segment_id=? WHERE identity_key=?", (s1, key_for("C"))))
    assert sql_one(r, "SELECT COUNT(*) FROM seen WHERE segment_id=?", (s1,))[0] == 2, "per-segment counts unchanged"
    r.start()
    assert_reports_rebuild(r, "differs")
    assert sql_one(r, "SELECT segment_id FROM seen WHERE identity_key=?", (key_for("A"),))[0] == s1


def test_6_identity_attributed_to_the_wrong_segment_is_detected(tmp_path):
    r = five(tmp_path)
    s1, s2 = (seg_id(r, s) for s in r.segs()[:2])
    tamper(r, ("UPDATE seen SET segment_id=? WHERE identity_key=?", (s2, key_for("A"))))
    assert sql_one(r, "SELECT segment_id FROM seen WHERE identity_key=?", (key_for("A"),))[0] == s2
    assert sql_one(r, "SELECT COUNT(*) FROM seen")[0] == 5, "nothing was added or removed"
    r.start()
    assert_reports_rebuild(r, "differs")
    assert sql_one(r, "SELECT segment_id FROM seen WHERE identity_key=?", (key_for("A"),))[0] == s1


@pytest.mark.parametrize("delta", [-1, +1])
def test_current_shape_with_a_different_schema_version_is_rebuilt(tmp_path, delta):
    """The version also versions SEMANTICS: an identical table shape under another version is not trusted."""
    r = five(tmp_path)
    tamper(r, (f"PRAGMA user_version={sd.INDEX_SCHEMA_VERSION + delta}",))
    r.start()
    assert_reports_rebuild(r, "user_version")
    assert r.index.user_version() == sd.INDEX_SCHEMA_VERSION


def test_orphan_membership_rows_and_dropped_index_are_detected(tmp_path):
    r = five(tmp_path)
    tamper(r, ("INSERT INTO seen (segment_id, identity_key) VALUES (999, ?)", (key_for("GHOST"),)))
    r.start()
    assert_reports_rebuild(r, "unknown segment_id")
    tamper(r, ("ALTER TABLE reconciled_segments ADD COLUMN junk TEXT",))
    r.start()
    assert_reports_rebuild(r, "shape")
    assert r.index.schema_problem() is None


# ===================================================================== self-consistent SQLite corruption (the point of C1)

def _forge_sqlite_consistently(r, drop_trade):
    """Delete an identity AND make the recorded count and digest agree with the remaining members."""
    seg = r.segs()[0]
    sid = seg_id(r, seg)
    remaining = [key_for(t) for t in pq.read_table(seg).column("trade_id").to_pylist() if t != drop_trade]
    count, digest = identity_set_digest(remaining)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for(drop_trade),)),
           ("UPDATE reconciled_segments SET identity_count=?, identity_digest=? WHERE segment_id=?", (count, digest, sid)))
    return seg


def test_self_consistent_corrupt_sqlite_is_caught_by_the_independent_evidence(tmp_path):
    r = five(tmp_path)
    _forge_sqlite_consistently(r, "A")
    r.start()                                   # NO deep verify: the evidence file is the independent reference
    assert_reports_rebuild(r, "differs from segment evidence")
    assert r.index.contains(key_for("A"))


def test_forged_evidence_plus_forged_sqlite_needs_deep_verify_and_deep_catches_it(tmp_path, monkeypatch):
    """Documents the trust model: evidence has the same trust level as the marker (both are checked against
    the bytes only by DEDUP_DEEP_VERIFY=1). Default startup cannot see a forgery of evidence AND index."""
    r = five(tmp_path)
    seg = _forge_sqlite_consistently(r, "A")
    ev_path = evidence_file(r, seg)
    ev = json.loads(ev_path.read_text())
    remaining = [key_for(t) for t in pq.read_table(seg).column("trade_id").to_pylist() if t != "A"]
    ev["identity_count"], ev["identity_digest"] = identity_set_digest(remaining)
    ev_path.write_text(json.dumps(ev))
    r.start()
    assert not r.co.last_report.rebuilt and not r.index.contains(key_for("A")), "default startup trusts evidence"
    r.crash()
    monkeypatch.setenv(DEEP_VERIFY_ENV, "1")
    r.start()
    assert_reports_rebuild(r, "differs from the segment bytes")
    assert r.index.contains(key_for("A")) and r.co.last_report.deep_verified == 3
    assert list(ev_path.parent.glob("*.ids.json.invalid.*")), "the forged evidence file is preserved, not deleted"


# ===================================================================== 7-8: DEDUP_DEEP_VERIFY=1

def test_7_missing_identity_detected_under_deep_verify(tmp_path, monkeypatch):
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("C"),)))
    monkeypatch.setenv(DEEP_VERIFY_ENV, "1")
    r.start()
    assert_reports_rebuild(r)
    assert r.co.last_report.deep_verified == 3 and r.index.contains(key_for("C"))


def test_8_fake_identity_detected_under_deep_verify(tmp_path, monkeypatch):
    r = five(tmp_path)
    sid = seg_id(r, r.segs()[2])
    tamper(r, ("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)", (sid, key_for("FAKE"))))
    monkeypatch.setenv(DEEP_VERIFY_ENV, "1")
    r.start()
    assert_reports_rebuild(r)
    assert not r.index.contains(key_for("FAKE"))


# ===================================================================== 9: rebuild restores exactly

def test_9_index_corruption_then_rebuild_restores_authoritative_identities_exactly(tmp_path):
    r = five(tmp_path)
    s = [seg_id(r, x) for x in r.segs()]
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("A"),)),
           ("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)", (s[0], key_for("FAKE1"))),
           ("UPDATE seen SET segment_id=? WHERE identity_key=?", (s[0], key_for("D"))),
           ("UPDATE reconciled_segments SET identity_count=99 WHERE segment_id=?", (s[2],)))
    r.start()
    assert_reports_rebuild(r)
    assert r.index.identity_count() == 5
    assert all(r.index.contains(key_for(t)) for t in "ABCDE") and not r.index.contains(key_for("FAKE1"))
    assert all(not r.offer(t) for t in "ABCDE")


# ===================================================================== 10: migration from the previous (F1, user_version=2) schema

def _write_f1_index(path, rows, *, version=2):
    c = sqlite3.connect(path)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE seen (identity_key TEXT PRIMARY KEY) WITHOUT ROWID")
    c.execute("CREATE TABLE reconciled_segments (segment_key TEXT PRIMARY KEY, identity_count INTEGER NOT NULL, "
              "evidence_sha256 TEXT, evidence_size INTEGER, confirmed_by TEXT) WITHOUT ROWID")
    for seg_key, (ids, sha, size) in rows.items():
        c.executemany("INSERT INTO seen VALUES (?)", [(i,) for i in ids])
        c.execute("INSERT INTO reconciled_segments VALUES (?,?,?,?,?)", (seg_key, len(ids), sha, size, "writer"))
    c.execute(f"PRAGMA user_version={version}")
    c.commit()
    c.close()


def _legacy_rows(r, poison):
    rows = {}
    for seg in r.segs():
        ids = [key_for(t) for t in pq.read_table(seg).column("trade_id").to_pylist()]
        m = read_marker(seg).publication
        rows[r.seg_key(seg)] = (ids, m["sha256"], m["size_bytes"])
    first = r.seg_key(r.segs()[0])
    rows[first] = (rows[first][0] + [poison], rows[first][1], rows[first][2])      # valid evidence, poisoned content
    return rows


@pytest.mark.parametrize("version", [0, 1, 2])
def test_10_previous_schema_is_never_trusted_even_with_valid_marker_evidence(tmp_path, version):
    r = five(tmp_path)
    rows = _legacy_rows(r, key_for("POISON"))
    r.crash()
    for suffix in ("", "-wal", "-shm"):
        Path(r.db + suffix).unlink(missing_ok=True)
    _write_f1_index(r.db, rows, version=version)
    legacy = SegmentDedupIndex(r.db)
    assert legacy.schema_problem() and legacy.contains(key_for("POISON")), "opening does not silently migrate"
    legacy.close()
    r.start()
    assert_reports_rebuild(r, "user_version")
    assert r.index.user_version() == sd.INDEX_SCHEMA_VERSION and r.index.schema_problem() is None
    assert not r.index.contains(key_for("POISON")) and r.offer("POISON"), "unverifiable F1 authority was dropped"
    r.restart()
    assert not r.co.last_report.rebuilt, "migration is one-time"


# ===================================================================== 11: crash inside the rebuild / migration transaction

class _FailOn:
    """Proxy for the SQLite connection that dies (BaseException) at a chosen statement."""

    def __init__(self, real, prefix, exc=Crash):
        self.real, self.prefix, self.exc, self.hit = real, prefix, exc, 0

    def execute(self, sql, *a):
        if sql.startswith(self.prefix):
            self.hit += 1
            raise self.exc("simulated process death inside the transaction")
        return self.real.execute(sql, *a)

    def executemany(self, *a):
        return self.real.executemany(*a)


@pytest.mark.parametrize("statement", ["DROP TABLE IF EXISTS seen", "DROP TABLE IF EXISTS reconciled_segments",
                                       "CREATE TABLE reconciled_segments", "CREATE TABLE seen", "PRAGMA user_version"])
def test_11a_crash_inside_rebuild_transaction_leaves_the_previous_state_untouched(tmp_path, statement):
    r = five(tmp_path)
    r.index.close()
    r.index = SegmentDedupIndex(r.db)
    real = r.index._conn
    before = real.execute("SELECT segment_id, identity_key FROM seen ORDER BY 1,2").fetchall()
    r.index._conn = _FailOn(real, statement)
    with pytest.raises(Crash):
        r.index.rebuild_reset()
    r.index._conn = real
    assert real.execute("SELECT segment_id, identity_key FROM seen ORDER BY 1,2").fetchall() == before
    assert r.index.schema_problem() is None, "rolled back as a whole: not half-dropped, not half-created"
    r.index.close()
    r.writer._release_lock()
    r.start()
    assert not r.co.last_report.rebuilt
    assert_membership_exact(r)


@pytest.mark.parametrize("statement", ["DROP TABLE IF EXISTS reconciled_segments", "CREATE TABLE seen"])
def test_11b_crash_inside_legacy_migration_leaves_legacy_untrusted_and_restart_completes(tmp_path, statement):
    r = five(tmp_path)
    rows = _legacy_rows(r, key_for("POISON"))
    r.crash()
    for suffix in ("", "-wal", "-shm"):
        Path(r.db + suffix).unlink(missing_ok=True)
    _write_f1_index(r.db, rows)
    idx = SegmentDedupIndex(r.db)
    real = idx._conn
    idx._conn = _FailOn(real, statement)
    with pytest.raises(Crash):
        idx.rebuild_reset()
    idx._conn = real
    assert idx.user_version() == 2 and idx.contains(key_for("POISON")), "legacy rows intact, still flagged"
    assert idx.schema_problem() is not None, "a half-migrated schema is never reported as current"
    idx.close()
    r.start()
    assert_reports_rebuild(r, "user_version")
    assert not r.index.contains(key_for("POISON"))


def test_11c_crash_between_reset_and_reindex_leaves_only_exact_partial_authority(tmp_path, fault_mp):
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("A"),)))
    real_commit, calls = SegmentDedupIndex.commit_segment, []

    def dying_commit(index, *a, **k):
        calls.append(a[0])
        if len(calls) == 2:
            raise Crash("died while re-indexing")
        return real_commit(index, *a, **k)
    fault_mp.setattr(SegmentDedupIndex, "commit_segment", dying_commit)
    with pytest.raises(Crash):
        r.start()
    fault_mp.undo()
    r.crash()
    conn = sqlite3.connect(r.db)
    try:
        committed = [k for (k,) in conn.execute("SELECT segment_key FROM reconciled_segments")]
        assert len(committed) == 1, "exactly the segment whose transaction completed"
        for key in committed:
            want = {key_for(t) for t in pq.read_table(r.stream_dir / key.split("/")[1]).column("trade_id").to_pylist()}
            got = {k for (k,) in conn.execute("SELECT identity_key FROM seen JOIN reconciled_segments "
                                              "USING(segment_id) WHERE segment_key=?", (key,))}
            assert got == want
    finally:
        conn.close()
    r.start()                                       # startup finishes the job before ingestion resumes
    assert r.co.last_report.indexed == 2 and not r.co.last_report.rebuilt
    assert_membership_exact(r)


# ===================================================================== 12-13: no needless work

def test_12_normal_valid_index_does_no_rebuild_no_evidence_rederivation_and_reads_no_segment_bytes(tmp_path, fault_mp):
    r = five(tmp_path)
    r.crash()
    fault_mp.setattr(SegmentDedupIndex, "rebuild_reset", lambda self: pytest.fail("rebuild on a valid index"))
    reads = []
    real = SegmentDedupCoordinator._verified_identities
    fault_mp.setattr(SegmentDedupCoordinator, "_verified_identities",
                     lambda self, *a, **k: (reads.append(a[0]), real(self, *a, **k))[1])
    r.start()
    rep = r.co.last_report
    assert (rep.rebuilt, rep.indexed, rep.evidence_regenerated, rep.deep_verified) == (False, 0, 0, 0)
    assert reads == [], "normal startup audits via markers + evidence files + the index; no parquet is read"
    assert not list(r.co.evidence_dir.glob("*.invalid.*")) and not list(r.stream_dir.glob("*.invalid.*"))


def test_13_repeated_startup_after_repair_is_idempotent(tmp_path):
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("B"),)))
    r.start()
    assert r.co.last_report.rebuilt
    snapshot = sql_one(r, "SELECT COUNT(*), group_concat(identity_key) FROM (SELECT identity_key FROM seen ORDER BY 1)")
    for _ in range(3):
        r.restart()
        rep = r.co.last_report
        assert (rep.rebuilt, rep.indexed, rep.evidence_regenerated) == (False, 0, 0)
        assert sql_one(r, "SELECT COUNT(*), group_concat(identity_key) FROM "
                          "(SELECT identity_key FROM seen ORDER BY 1)") == snapshot
    assert_membership_exact(r)


# ===================================================================== evidence files are derived and self-healing

def test_missing_or_garbage_evidence_is_regenerated_from_bytes_without_a_rebuild(tmp_path):
    r = five(tmp_path)
    r.crash()
    evidence_file(r, r.segs()[0]).unlink()
    evidence_file(r, r.segs()[1]).write_text("{not json")
    r.start()
    rep = r.co.last_report
    assert not rep.rebuilt and rep.evidence_regenerated == 2
    assert list(evidence_file(r, r.segs()[1]).parent.glob("*.invalid.0")), "garbage preserved, not deleted"
    r.restart()
    assert r.co.last_report.evidence_regenerated == 0


def test_evidence_bound_to_other_bytes_is_not_trusted(tmp_path):
    r = five(tmp_path)
    seg = r.segs()[0]
    ev_path = evidence_file(r, seg)
    ev = json.loads(ev_path.read_text())
    ev["segment_sha256"] = "0" * 64                       # everything else (count/digest) still plausible
    r.crash()
    ev_path.write_text(json.dumps(ev))
    r.start()
    assert not r.co.last_report.rebuilt and r.co.last_report.evidence_regenerated == 1
    assert json.loads(ev_path.read_text())["segment_sha256"] == read_marker(seg).publication["sha256"]


def test_lost_evidence_directory_plus_corrupt_index_is_still_detected(tmp_path):
    r = five(tmp_path)
    tamper(r, ("DELETE FROM seen WHERE identity_key=?", (key_for("A"),)))
    shutil.rmtree(r.co.evidence_dir)
    r.start()
    assert_reports_rebuild(r)
    assert r.co.last_report.evidence_regenerated == 3, "re-derived from the segment bytes, which are the authority"


# ===================================================================== 14: sequence reuse / stale evidence

def _replace_segment_with_other_bytes(r, seg, other_ids, tmp_path):
    """Same file name, different bytes, with a valid marker for the NEW bytes (what name/sequence reuse looks like)."""
    scratch = tmp_path / "scratch"
    w = ParquetWriter("scratch_trades", SCHEMA, base_dir=str(scratch), segment_rows=100, segment_seconds=3600)
    for t in other_ids:
        w.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": t})
    w.publish_open_segment()
    produced = sorted(w.stream_dir.glob("*.seg"))[0]
    w._release_lock()
    marker_path(seg).unlink()
    seg.unlink()
    shutil.copy(produced, seg)
    sha, size = pub.sha256_file(seg)
    pub.write_marker_atomic(seg, record_count=len(other_ids), first_ts=1, last_ts=1, sha256=sha, size_bytes=size,
                            confirmed_by=pub.CONFIRMED_BY_WRITER, legacy=False)


def test_14a_sequence_reuse_with_stale_index_and_evidence_cannot_suppress_the_new_segment(tmp_path):
    r = five(tmp_path)
    r.crash()
    victim = r.segs()[0]                                    # held A, B
    _replace_segment_with_other_bytes(r, victim, ["X", "Y"], tmp_path)
    r.start()
    assert_reports_rebuild(r, "differs from marker")
    assert r.index.contains(key_for("X")) and r.index.contains(key_for("Y")), "the new bytes are authority"
    assert r.offer("A") and r.offer("B"), "identities of bytes that no longer exist are not authority"
    ev = json.loads(evidence_file(r, victim).read_text())
    assert ev["segment_sha256"] == read_marker(victim).publication["sha256"], "stale evidence was replaced"
    assert list(evidence_file(r, victim).parent.glob("*.invalid.0")), "and preserved"


def test_14b_live_publish_over_stale_index_row_is_an_evidence_conflict_and_touches_no_file(tmp_path):
    r = five(tmp_path)
    victim = r.segs()[0]
    before = evidence_file(r, victim).read_bytes()
    _replace_segment_with_other_bytes(r, victim, ["X", "Y"], tmp_path)
    with pytest.raises(DedupStateError, match="EVIDENCE_CONFLICT"):
        r.co.on_segment_published(("h", 0), victim)
    assert evidence_file(r, victim).read_bytes() == before, "conflict is raised before any file is touched"
    assert not r.index.contains(key_for("X"))


def test_14c_stale_evidence_without_an_index_row_is_replaced_not_trusted(tmp_path):
    r = five(tmp_path)
    r.crash()
    victim = r.segs()[0]
    _replace_segment_with_other_bytes(r, victim, ["X", "Y"], tmp_path)
    conn = sqlite3.connect(r.db)
    sid = conn.execute("SELECT segment_id FROM reconciled_segments WHERE segment_key=?", (r.seg_key(victim),)).fetchone()[0]
    conn.execute("DELETE FROM seen WHERE segment_id=?", (sid,))
    conn.execute("DELETE FROM reconciled_segments WHERE segment_id=?", (sid,))
    conn.commit()
    conn.close()
    r.start()
    assert not r.co.last_report.rebuilt and r.co.last_report.indexed == 1
    assert r.index.contains(key_for("X")) and not r.index.contains(key_for("A"))
    assert_membership_exact(r)


# ===================================================================== 15: identity-key encoding drift

def test_15a_encoding_version_bump_forces_a_safe_rebuild(tmp_path, monkeypatch):
    r = five(tmp_path)
    r.crash()
    monkeypatch.setattr(sd, "IDENTITY_ENCODING_VERSION", sd.IDENTITY_ENCODING_VERSION + 1)
    r.start()
    assert_reports_rebuild(r, "identity-key encoding")
    assert r.co.last_report.indexed == 3
    r.restart()
    assert not r.co.last_report.rebuilt, "stable under the new encoding"


def test_15b_encoding_change_without_a_version_bump_is_caught_by_the_canary(tmp_path, monkeypatch):
    r = five(tmp_path)
    r.crash()
    before = sd.identity_encoding_fingerprint()
    monkeypatch.setattr(sd, "dedup_identity_key", lambda *parts: "|".join(parts))      # forgot to bump the version
    assert sd.identity_encoding_fingerprint() != before, "the canary notices an encoding change on its own"
    r.start()
    assert_reports_rebuild(r, "identity-key encoding")


def test_15c_evidence_written_under_another_encoding_is_not_trusted(tmp_path, monkeypatch):
    r = five(tmp_path)
    seg = r.segs()[0]
    ev_path = evidence_file(r, seg)
    ev = json.loads(ev_path.read_text())
    ev["identity_encoding"] = "v0:something-else"
    r.crash()
    ev_path.write_text(json.dumps(ev))
    r.start()
    assert not r.co.last_report.rebuilt and r.co.last_report.evidence_regenerated == 1


# ===================================================================== cross-segment duplicates that are REAL data

def test_genuine_cross_segment_duplicate_fails_closed_live_and_at_startup(tmp_path):
    r = Rig(tmp_path, segment_rows=2)
    assert r.offer("A") and r.offer("B")
    r.writer.publish_open_segment()
    r.force("A")                                            # the same trade again, bypassing dedup
    r.force("C")
    r.writer.publish_open_segment()                         # hook raises -> writer latches, nothing committed
    assert any(e["event_type"] == "DEDUP_STATE_FAILED" for e in r.events)
    second = r.segs()[1]
    assert not r.index.is_segment_reconciled(r.seg_key(second))
    assert sql_one(r, "SELECT COUNT(*) FROM seen WHERE identity_key=?", (key_for("A"),))[0] == 1
    r.crash()
    with pytest.raises(DedupStateError, match="CROSS_SEGMENT_DUPLICATE"):
        Rig(tmp_path, segment_rows=2)


def test_commit_segment_refuses_an_identity_owned_by_another_segment(tmp_path):
    idx = SegmentDedupIndex(str(tmp_path / "i.sqlite3"))
    assert idx.commit_segment("s/a.seg", ["k1", "k2"], "a" * 64, 1, "writer")
    with pytest.raises(DedupStateError, match="CROSS_SEGMENT_DUPLICATE"):
        idx.commit_segment("s/b.seg", ["k3", "k2"], "b" * 64, 1, "writer")
    assert not idx.is_segment_reconciled("s/b.seg") and not idx.contains("k3"), "rolled back as a whole"
    with pytest.raises(DedupStateError, match="IDENTITY_CONFLICT"):
        idx.commit_segment("s/a.seg", ["k1"], "a" * 64, 1, "writer")
    assert idx.commit_segment("s/a.seg", ["k2", "k1", "k1"], "a" * 64, 1, "writer") is False, "order/dup independent"


# ===================================================================== the digest itself

def test_identity_set_digest_is_order_and_duplicate_independent_and_content_sensitive():
    keys = ["b", "a", "c"]
    assert identity_set_digest(keys) == identity_set_digest(reversed(keys)) == identity_set_digest(keys + ["a"])
    assert identity_set_digest(keys)[0] == 3
    assert identity_set_digest(["a", "b", "x"])[1] != identity_set_digest(keys)[1], "same count, one identity swapped"
    assert identity_set_digest(["a", "b"])[1] != identity_set_digest(["ab"])[1], "framing is unambiguous"
    assert identity_set_digest([])[0] == 0


def test_streamed_membership_digest_equals_sorted_digest_for_non_ascii_keys(tmp_path):
    """Pins the ordering assumption: SQLite BINARY order == UTF-8 byte order == the digest's sort order."""
    idx = SegmentDedupIndex(str(tmp_path / "i.sqlite3"))
    keys = ["Z", "a", "\u00e9", "\u00ff", "\u4e2d", "\U0001f600", "\uffff", "a\x00b", "", "10:x", "9:x", "~", " "]
    idx.commit_segment("s/a.seg", keys, "a" * 64, 1, "writer")
    idx.commit_segment("s/b.seg", ["other"], "b" * 64, 1, "writer")
    sid = idx.reconciled_rows()["s/a.seg"].segment_id
    assert idx.membership_digests()[0][sid] == identity_set_digest(keys)
    assert idx.reconciled_rows()["s/a.seg"].identity_digest == identity_set_digest(keys)[1]


def test_new_index_has_the_current_shape_and_a_fresh_file_needs_no_rebuild(tmp_path):
    idx = SegmentDedupIndex(str(tmp_path / "i.sqlite3"))
    assert idx.user_version() == sd.INDEX_SCHEMA_VERSION and idx.schema_problem() is None
    idx.close()
    (tmp_path / "fresh").mkdir()
    r = Rig(tmp_path / "fresh")
    assert not r.co.last_report.rebuilt
