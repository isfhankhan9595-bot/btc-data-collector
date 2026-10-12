"""Phase 2A: bounded, read-only publication-marker validation (collector.collector.marker_validation).

A validated marker is a validated *claim*, not validated market data. These tests assert what must
never happen (a trust promotion, a false "absent", a read past the bound, a followed symlink, a
write) as much as what must.
"""
import ast
import contextlib
import dataclasses
import errno
import hashlib
import json
import os
import signal
import stat as _stat
from pathlib import Path

import pytest

from collector.collector import marker_validation as mv
from collector.collector import publication as pub
from collector.collector.marker_validation import (
    DEFAULT_MAX_MARKER_BYTES,
    DeclaredMarkerEvidence,
    MarkerValidation,
    MarkerValidationStatus as S,
    validate_markers,
)
from collector.collector.source_catalog import (
    MarkerPresence,
    SourceRootError,
    TrustState,
    discover_sources,
)

SEG = "2026-10-11-12-000003.seg"
LEGACY = "2026-10-11-12.parquet"
SHA = "ab" * 32

_REAL_OPEN, _REAL_CLOSE, _REAL_READ = os.open, os.close, os.read
_REAL_FSTAT, _REAL_STAT = os.fstat, os.stat
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
_O_TMPFILE = getattr(os, "O_TMPFILE", None)       # contains the O_DIRECTORY bit: compare it whole, never as a mask


# ---------------------------------------------------------------- fixtures / helpers

def _doc(seg: str = SEG, mutate=None, **pub_overrides) -> dict:
    doc = pub.build_marker(seg, record_count=5, first_ts=1791748703670, last_ts=1791748703999, sha256=SHA,
                           size_bytes=1234, confirmed_by="writer", legacy=False, boot_id="boot-1",
                           confirmed_at_utc="2026-10-11T12:00:00Z")
    doc["publication"].update(pub_overrides)
    if mutate is not None:
        mutate(doc)
    return doc


def _enc(doc) -> bytes:
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _stream(root: Path, stream: str = "trades") -> Path:
    d = root / "raw" / stream
    d.mkdir(parents=True, exist_ok=True)
    return d


def _put(root: Path, marker, *, seg: str = SEG, stream: str = "trades", seg_bytes: bytes = b"segment-bytes") -> Path:
    """A segment plus (optionally) its marker. ``marker`` is bytes, a JSON-able object, or None."""
    d = _stream(root, stream)
    (d / seg).write_bytes(seg_bytes)
    if marker is not None:
        (d / (seg + ".meta.json")).write_bytes(marker if isinstance(marker, bytes) else _enc(marker))
    return d


def _run(root: Path, **kw):
    result = discover_sources(root)
    vals = validate_markers(root, result, **kw)
    return result, {v.relative_path: v for v in vals}


def _one(root: Path, marker, **kw) -> MarkerValidation:
    _put(root, marker)
    _, by = _run(root, **kw)
    return by[f"raw/trades/{SEG}"]


def _snapshot(root: Path):
    snap = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in sorted(dirnames) + sorted(filenames):
            p = Path(dirpath) / name
            st = p.lstat()
            digest = hashlib.sha256(p.read_bytes()).hexdigest() if _stat.S_ISREG(st.st_mode) else None
            snap[str(p.relative_to(root))] = (st.st_mode, st.st_size, st.st_mtime_ns, digest,
                                              os.readlink(p) if _stat.S_ISLNK(st.st_mode) else None)
        st = Path(dirpath).lstat()
        snap[str(Path(dirpath).relative_to(root)) + "/"] = (st.st_mode, st.st_mtime_ns)
    return snap


@contextlib.contextmanager
def _deadline(seconds: int = 20):
    """A swapped-in FIFO must not be able to hang the run: fail the test instead."""
    def _expired(signum, frame):
        raise TimeoutError("marker validation hung")
    old = signal.signal(signal.SIGALRM, _expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


class _Ledger:
    """Wraps os.open / os.close / os.read / os.fstat / os.stat for the duration of a ``with`` block.

    Records every open (name + flags), every read (requested vs returned), and which descriptors were
    never closed. Optional hooks inject failures or mutate the filesystem mid-read."""

    def __init__(self, monkeypatch, *, on_open=None, on_read=None, on_fstat=None):
        self.mp, self.on_open, self.on_read, self.on_fstat = monkeypatch, on_open, on_read, on_fstat
        self.opens, self.reads, self.live = [], [], {}

    def __enter__(self):
        ledger = self

        def op(path, flags, *a, **k):
            ledger.opens.append((os.fspath(path), flags, k.get("dir_fd")))
            if ledger.on_open is not None:
                ledger.on_open(os.fspath(path), flags)
            fd = _REAL_OPEN(path, flags, *a, **k)
            ledger.live[fd] = os.fspath(path)
            return fd

        def cl(fd):
            ledger.live.pop(fd, None)
            return _REAL_CLOSE(fd)

        def rd(fd, n):
            if ledger.on_read is not None:
                ledger.on_read("before", fd, n)
            data = _REAL_READ(fd, n)
            ledger.reads.append((fd, n, len(data)))
            if ledger.on_read is not None:
                ledger.on_read("after", fd, n)
            return data

        def fs(fd):
            if ledger.on_fstat is not None:
                ledger.on_fstat(fd)
            return _REAL_FSTAT(fd)

        self.mp.setattr(os, "open", op)
        self.mp.setattr(os, "close", cl)
        self.mp.setattr(os, "read", rd)
        self.mp.setattr(os, "fstat", fs)
        return self

    def __exit__(self, *exc):
        self.mp.setattr(os, "open", _REAL_OPEN)
        self.mp.setattr(os, "close", _REAL_CLOSE)
        self.mp.setattr(os, "read", _REAL_READ)
        self.mp.setattr(os, "fstat", _REAL_FSTAT)
        return False

    def marker_opens(self):
        return [o for o in self.opens if o[0].endswith(".meta.json")]


def _no_secrets(vals, root: Path):
    text = repr(vals)
    assert str(root) not in text, "absolute path leaked into a validation result"
    assert "Errno" not in text and "boom" not in text


# ---------------------------------------------------------------- 1. a correct schema-2 marker

def test_valid_schema2_marker_is_a_claim_with_declared_evidence_preserved_natively(tmp_path):
    v = _one(tmp_path, _doc())
    assert v.status is S.VALID_V2
    assert v.reasons == ("MARKER_SCHEMA_2_VALID", "SEGMENT_BYTES_NOT_VERIFIED")
    assert v.marker_presence is MarkerPresence.REGULAR_FILE and v.detail == ""
    ev = v.evidence
    assert ev == DeclaredMarkerEvidence(
        segment=SEG, schema=2, state="PUBLICATION_CONFIRMED", confirmed_by="writer", legacy=False,
        size_bytes=1234, sha256=SHA, record_count=5, first_record_ts=1791748703670,
        last_record_ts=1791748703999, confirmed_at_utc="2026-10-11T12:00:00Z", boot_id="boot-1",
        writer_schema="parquet_writer/2")
    for name in ("schema", "size_bytes", "record_count", "first_record_ts", "last_record_ts"):
        assert type(getattr(ev, name)) is int, name          # native int, never bool/float/str
    assert type(ev.legacy) is bool
    assert v.bytes_read == v.observed_size == len(_enc(_doc()))


def test_missing_optional_evidence_stays_missing_and_nothing_is_synthesised(tmp_path):
    def strip(doc):
        doc["first_record_ts"] = None
        del doc["last_record_ts"]
        for k in ("boot_id", "confirmed_at_utc", "writer_schema"):
            del doc["publication"][k]
    ev = _one(tmp_path, _doc(mutate=strip)).evidence
    assert (ev.first_record_ts, ev.last_record_ts, ev.boot_id, ev.confirmed_at_utc, ev.writer_schema) == (
        None, None, None, None, None)


def test_a_valid_marker_never_changes_the_source_trust_state_or_presence(tmp_path):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    before = dataclasses.astuple(result.records[0])
    (v,) = validate_markers(tmp_path, result)
    assert v.status is S.VALID_V2
    assert dataclasses.astuple(result.records[0]) == before
    rec = result.records[0]
    assert rec.trust_state is TrustState.UNKNOWN and rec.marker_presence is MarkerPresence.REGULAR_FILE
    assert "MARKER_PRESENT_CONTENT_NOT_READ" in rec.reasons          # discovery's own verdict is untouched
    # nothing in a validation result can be mistaken for a trust state or a verification verdict
    fields = {f.name for f in dataclasses.fields(MarkerValidation)}
    assert not fields & {"trust_state", "confirmed", "verified", "corrupt", "matches", "hash_matches"}
    assert {s.name for s in S} == {"NOT_CHECKED", "VALID_V2", "LEGACY_HINT_V1", "INVALID_CONTENT",
                                   "TOO_LARGE", "UNKNOWN_IO", "NOT_APPLICABLE"}
    assert not {s.value for s in S} & {t.value for t in (TrustState.CONFIRMED, TrustState.CONFIRMED_RETROACTIVE,
                                                         TrustState.CORRUPT)}


def test_real_writer_markers_validate_and_fit_far_inside_the_limit(tmp_path):
    from datetime import datetime, timezone
    import pyarrow as pa
    from collector.collector.parquet_writer import ParquetWriter
    schema = pa.schema([("timestamp", pa.timestamp("ms", tz="UTC")), ("value", pa.int64())])
    writer = ParquetWriter("example", schema, base_dir=str(tmp_path), segment_rows=1)
    writer.write({"timestamp": datetime.now(timezone.utc), "value": 7})
    writer.close()
    before = _snapshot(tmp_path)
    result = discover_sources(tmp_path)
    (v,) = validate_markers(tmp_path, result)
    assert _snapshot(tmp_path) == before
    assert v.status is S.VALID_V2 and v.evidence.confirmed_by == "writer" and v.evidence.legacy is False
    assert v.observed_size < DEFAULT_MAX_MARKER_BYTES // 50          # a real marker is hundreds of bytes
    assert DEFAULT_MAX_MARKER_BYTES == 64 * 1024


# ---------------------------------------------------------------- 2-7. content that must be refused

def test_marker_naming_a_different_segment_is_invalid_for_this_source(tmp_path):
    v = _one(tmp_path, _doc(seg="2026-10-11-12-000004.seg"))
    assert v.status is S.INVALID_CONTENT and v.evidence is None
    assert "segment name does not match" in v.detail


@pytest.mark.parametrize("label,mutate,fragment", [
    ("schema 1", lambda d: d["publication"].update(schema=1), "schema != 2"),
    ("schema 3", lambda d: d["publication"].update(schema=3), "schema != 2"),
    ("schema as str", lambda d: d["publication"].update(schema="2"), "schema != 2"),
    ("schema as float", lambda d: d["publication"].update(schema=2.0), "schema != 2"),
    ("schema missing", lambda d: d["publication"].pop("schema"), "schema != 2"),
    ("state wrong", lambda d: d["publication"].update(state="RENAMED"), "state != PUBLICATION_CONFIRMED"),
    ("state missing", lambda d: d["publication"].pop("state"), "state != PUBLICATION_CONFIRMED"),
    ("segment missing", lambda d: d["publication"].pop("segment"), "segment name does not match"),
    ("confirmed_by unknown", lambda d: d["publication"].update(confirmed_by="root"), "confirmed_by not allowed"),
    ("confirmed_by missing", lambda d: d["publication"].pop("confirmed_by"), "confirmed_by not allowed"),
    ("legacy as str", lambda d: d["publication"].update(legacy="false"), "legacy not a bool"),
    ("legacy as int", lambda d: d["publication"].update(legacy=0), "legacy not a bool"),
    ("publication is a list", lambda d: d.update(publication=[]), "publication is not an object"),
    ("publication is null", lambda d: d.update(publication=None), "publication is not an object"),
])
def test_schema_and_state_violations_are_invalid_content(tmp_path, label, mutate, fragment):
    v = _one(tmp_path, _doc(mutate=mutate))
    assert v.status is S.INVALID_CONTENT, label
    assert fragment in v.detail and v.evidence is None
    assert v.marker_presence is MarkerPresence.REGULAR_FILE            # path state is never overwritten


@pytest.mark.parametrize("where,key", [
    ("publication", "schema"), ("publication", "size_bytes"), ("top", "record_count"),
    ("top", "first_record_ts"), ("top", "last_record_ts")])
@pytest.mark.parametrize("bad", [True, False])
def test_booleans_are_rejected_wherever_an_integer_is_required(tmp_path, where, key, bad):
    def mutate(doc):
        (doc["publication"] if where == "publication" else doc)[key] = bad
    v = _one(tmp_path, _doc(mutate=mutate))
    assert v.status is S.INVALID_CONTENT and v.evidence is None
    assert key in v.detail or key == "schema"


@pytest.mark.parametrize("label,mutate", [
    ("size negative", lambda d: d["publication"].update(size_bytes=-1)),
    ("size float", lambda d: d["publication"].update(size_bytes=12.0)),
    ("size str", lambda d: d["publication"].update(size_bytes="12")),
    ("size null", lambda d: d["publication"].update(size_bytes=None)),
    ("size missing", lambda d: d["publication"].pop("size_bytes")),
    ("count negative", lambda d: d.update(record_count=-1)),
    ("count float", lambda d: d.update(record_count=1.5)),
    ("count str", lambda d: d.update(record_count="5")),
    ("count null", lambda d: d.update(record_count=None)),
    ("count missing", lambda d: d.pop("record_count")),
    ("first_ts str", lambda d: d.update(first_record_ts="1")),
    ("last_ts float", lambda d: d.update(last_record_ts=1.0)),
])
def test_invalid_size_and_count_fields_are_invalid_content(tmp_path, label, mutate):
    v = _one(tmp_path, _doc(mutate=mutate))
    assert v.status is S.INVALID_CONTENT and v.evidence is None, label


@pytest.mark.parametrize("sha", [
    SHA.upper(), "ab" * 31, "ab" * 33, "zz" * 32, "", None, 12345, ["ab"] * 32, SHA + "\n", " " + SHA, SHA + " ",
])
def test_malformed_sha256_is_invalid_content(tmp_path, sha):
    v = _one(tmp_path, _doc(sha256=sha))
    assert v.status is S.INVALID_CONTENT and "sha256 invalid" in v.detail and v.evidence is None


@pytest.mark.parametrize("label,raw", [
    ("invalid utf-8", b"\xff\xfe{\"publication\":{}}"),
    ("truncated json", b'{"publication":{"schema":2'),
    ("not json at all", b"hello"),
    ("empty file", b""),
    ("whitespace only", b" \n\t "),
    ("json list", b"[]"),
    ("json string", b'"x"'),
    ("json number", b"7"),
    ("json null", b"null"),
    ("json true", b"true"),
    ("nesting bomb", b"[" * 60000),
])
def test_undecodable_malformed_or_non_object_content_is_invalid_not_a_crash(tmp_path, label, raw):
    v = _one(tmp_path, raw)
    assert v.status is S.INVALID_CONTENT, label
    assert v.evidence is None and v.reasons == ("MARKER_CONTENT_INVALID",)
    assert v.bytes_read == len(raw) and v.observed_size == len(raw)


def test_every_invalid_content_result_leaves_the_segment_unjudged(tmp_path):
    """Invalid marker contents are evidence about the marker, not proof the segment is corrupt."""
    _put(tmp_path, b"garbage")
    result, by = _run(tmp_path)
    assert by[f"raw/trades/{SEG}"].status is S.INVALID_CONTENT
    assert result.records[0].trust_state is TrustState.UNKNOWN          # discovery's verdict, untouched
    assert result.records[0].trust_state not in (TrustState.CORRUPT, TrustState.CONFIRMED)


# ---------------------------------------------------------------- 8. legacy v1 hint

def test_v1_historical_hint_is_a_hint_not_a_publication_confirmation(tmp_path):
    v = _one(tmp_path, {"record_count": 9, "first_record_ts": 1, "last_record_ts": 2})
    assert v.status is S.LEGACY_HINT_V1
    assert v.reasons == ("MARKER_LEGACY_V1_HINT", "NOT_PUBLICATION_CONFIRMATION")
    assert v.evidence is None, "an unvalidated v1 hint must not be presented as declared publication evidence"
    assert v.status is not S.VALID_V2


# ---------------------------------------------------------------- 9. the size bound

def test_marker_over_the_limit_is_rejected_from_fstat_without_reading_it(tmp_path, monkeypatch):
    _put(tmp_path, b" " * 4097)
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result, max_marker_bytes=4096)
    assert v.status is S.TOO_LARGE and v.reasons == ("MARKER_EXCEEDS_SIZE_LIMIT",)
    assert v.observed_size == 4097 and v.bytes_read == 0 and v.evidence is None
    assert ledger.reads == [], "an oversized marker must not be read at all"
    assert ledger.live == {}


def test_a_huge_sparse_marker_is_rejected_without_allocating_or_reading_it(tmp_path, monkeypatch):
    d = _put(tmp_path, None)
    with open(d / (SEG + ".meta.json"), "wb") as handle:
        handle.truncate(8 * 1024 ** 3)                                  # 8 GiB, sparse: no disk used
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.TOO_LARGE and v.observed_size == 8 * 1024 ** 3 and v.bytes_read == 0
    assert ledger.reads == []


def test_the_limit_is_exact_at_the_boundary_and_reads_never_exceed_limit_plus_one(tmp_path, monkeypatch):
    base = _enc(_doc())
    limit = len(base) + 100
    padded = base + b" " * 100                                          # valid JSON, exactly ``limit`` bytes
    _put(tmp_path, padded)
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        (at_limit,) = validate_markers(tmp_path, result, max_marker_bytes=limit)
    assert at_limit.status is S.VALID_V2 and at_limit.bytes_read == limit
    assert all(requested <= limit + 1 for _fd, requested, _got in ledger.reads)
    assert sum(got for _fd, _req, got in ledger.reads) <= limit + 1

    (tmp_path / "raw" / "trades" / (SEG + ".meta.json")).write_bytes(padded + b" ")
    result = discover_sources(tmp_path)
    (over,) = validate_markers(tmp_path, result, max_marker_bytes=limit)
    assert over.status is S.TOO_LARGE and over.bytes_read == 0


def test_a_marker_that_grows_past_the_limit_while_being_read_is_uncertain_and_bounded(tmp_path, monkeypatch):
    limit = 2048
    _put(tmp_path, b" " * 100 + _enc(_doc()))
    result = discover_sources(tmp_path)
    marker = tmp_path / "raw" / "trades" / (SEG + ".meta.json")

    def grow(phase, fd, n):
        if phase == "after" and not getattr(grow, "done", False):
            grow.done = True
            with open(marker, "ab") as h:
                h.write(b" " * (limit * 4))

    with _Ledger(monkeypatch, on_read=grow) as ledger:
        (v,) = validate_markers(tmp_path, result, max_marker_bytes=limit)
    assert v.status is S.UNKNOWN_IO and v.reasons[0] == "MARKER_CHANGED_DURING_READ"
    assert sum(got for _fd, _req, got in ledger.reads) <= limit + 1
    assert v.bytes_read <= limit + 1


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "64", None, mv.MAX_ALLOWED_MARKER_BYTES + 1])
def test_the_configurable_limit_itself_is_bounded_and_validated(tmp_path, bad):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    with pytest.raises(ValueError):
        validate_markers(tmp_path, result, max_marker_bytes=bad)


def test_an_unusable_data_root_raises_instead_of_guessing(tmp_path):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    with pytest.raises(SourceRootError):
        validate_markers(tmp_path / "does-not-exist", result)


# ---------------------------------------------------------------- 10. I/O failure is uncertainty

def _boom(code: int, root: Path):
    return OSError(code, f"boom {root}/raw/trades/{SEG}.meta.json")


@pytest.mark.parametrize("stage", ["open", "fstat", "read"])
@pytest.mark.parametrize("code", [errno.EACCES, errno.EIO, errno.EINTR, errno.EMFILE])
def test_io_failures_are_unknown_never_absent_invalid_or_corrupt(tmp_path, monkeypatch, stage, code):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)

    def fail_open(path, flags):
        if path.endswith(".meta.json"):
            raise _boom(code, tmp_path)

    def fail_read(phase, fd, n):
        if phase == "before":
            raise _boom(code, tmp_path)

    calls = {"n": 0}

    def fail_fstat(fd):
        calls["n"] += 1
        raise _boom(code, tmp_path)

    hooks = {"open": dict(on_open=fail_open), "read": dict(on_read=fail_read), "fstat": dict(on_fstat=fail_fstat)}
    with _Ledger(monkeypatch, **hooks[stage]) as ledger:
        vals = validate_markers(tmp_path, result)
    (v,) = vals
    assert v.status is S.UNKNOWN_IO
    assert v.status not in (S.NOT_CHECKED, S.INVALID_CONTENT, S.TOO_LARGE, S.VALID_V2)
    assert f"ERRNO_{errno.errorcode[code]}" in v.reasons
    assert v.evidence is None
    assert v.marker_presence is MarkerPresence.REGULAR_FILE            # never rewritten to ABSENT
    _no_secrets(vals, tmp_path)
    assert ledger.live == {}, f"descriptors leaked: {ledger.live}"


def test_a_marker_that_vanishes_between_discovery_and_open_is_unknown_not_absent(tmp_path):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    (d / (SEG + ".meta.json")).unlink()
    (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("MARKER_MISSING_AT_OPEN",)
    assert v.marker_presence is MarkerPresence.REGULAR_FILE


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_a_real_permission_failure_is_uncertainty(tmp_path):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    marker = d / (SEG + ".meta.json")
    marker.chmod(0)
    try:
        vals = validate_markers(tmp_path, result)
    finally:
        marker.chmod(0o644)
    assert vals[0].status is S.UNKNOWN_IO and "ERRNO_EACCES" in vals[0].reasons
    _no_secrets(vals, tmp_path)


def test_descriptors_are_closed_on_success_and_on_every_early_exit(tmp_path, monkeypatch):
    for i, content in enumerate((_enc(_doc("2026-10-11-12-000000.seg")), b"junk", b" " * 9000)):
        _put(tmp_path, content, seg=f"2026-10-11-12-00000{i}.seg")
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        vals = validate_markers(tmp_path, result, max_marker_bytes=4096)
    assert [v.status for v in vals] == [S.VALID_V2, S.INVALID_CONTENT, S.TOO_LARGE]
    assert ledger.live == {}


# ---------------------------------------------------------------- 11. symlink swaps

def test_marker_swapped_for_a_symlink_after_discovery_is_refused_and_the_target_never_read(tmp_path, monkeypatch):
    d = _put(tmp_path, _doc())
    decoy = tmp_path / "decoy.json"
    decoy.write_bytes(_enc(_doc()))                                      # a perfectly valid marker, elsewhere
    result = discover_sources(tmp_path)
    marker = d / (SEG + ".meta.json")
    marker.unlink()
    marker.symlink_to(decoy)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("MARKER_SYMLINK_AT_OPEN",)
    assert v.evidence is None and v.bytes_read == 0
    assert ledger.reads == [], "the symlink target must never be read"
    assert marker.is_symlink() and decoy.read_bytes() == _enc(_doc())    # nothing was touched


def test_stream_directory_swapped_for_a_symlink_after_discovery_is_refused(tmp_path, monkeypatch):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    real = d.rename(tmp_path / "raw" / "trades_real")
    d.symlink_to(real)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)
    assert ledger.marker_opens() == [] and ledger.reads == []


def test_raw_directory_swapped_for_a_symlink_after_discovery_is_refused(tmp_path, monkeypatch):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    (tmp_path / "raw").rename(tmp_path / "raw_real")
    (tmp_path / "raw").symlink_to(tmp_path / "raw_real")
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)
    assert ledger.reads == []


def test_stream_directory_that_disappears_or_becomes_a_file_is_unknown(tmp_path):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    (d / SEG).unlink()
    (d / (SEG + ".meta.json")).unlink()
    d.rmdir()
    (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("PATH_COMPONENT_MISSING_AT_OPEN",)
    d.write_bytes(b"now a file")
    (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)


def test_one_unsafe_stream_does_not_poison_a_healthy_one(tmp_path):
    _put(tmp_path, _doc(), stream="trades")
    other = _put(tmp_path, _doc(), stream="bybit_trades")
    result = discover_sources(tmp_path)
    real = other.rename(tmp_path / "raw" / "elsewhere")
    other.symlink_to(real)
    by = {v.relative_path: v for v in validate_markers(tmp_path, result)}
    assert by[f"raw/trades/{SEG}"].status is S.VALID_V2
    assert by[f"raw/bybit_trades/{SEG}"].status is S.UNKNOWN_IO


# ---------------------------------------------------------------- 12. modification during the read

def _mutating_read(marker: Path, action):
    state = {"done": False}

    def hook(phase, fd, n):
        if phase == "after" and not state["done"]:
            state["done"] = True
            action(marker)
    return hook


def _rewrite_same_size(marker: Path):
    data = marker.read_bytes()
    marker.write_bytes(data.replace(b"boot-1", b"boot-2"))               # same length, different bytes
    os.utime(marker, ns=(1_000_000_000, 2_000_000_000))                  # deterministic mtime change


def _replace_with_copy(marker: Path):                                    # identical bytes, NEW inode
    new = marker.with_name("replacement.tmp")
    new.write_bytes(marker.read_bytes())
    os.replace(new, marker)


def _unlink(marker: Path):
    marker.unlink()


def _swap_symlink(marker: Path):
    target = marker.with_name("elsewhere.json")
    target.write_bytes(marker.read_bytes())
    marker.unlink()
    marker.symlink_to(target)


def _append(marker: Path):
    with open(marker, "ab") as h:
        h.write(b" ")


def _truncate(marker: Path):
    with open(marker, "r+b") as h:
        h.truncate(10)


@pytest.mark.parametrize("label,action,reason", [
    ("rewritten in place", _rewrite_same_size, "MARKER_CHANGED_DURING_READ"),
    ("appended to", _append, "MARKER_CHANGED_DURING_READ"),
    ("truncated", _truncate, "MARKER_CHANGED_DURING_READ"),
    ("replaced by an identical copy (new inode)", _replace_with_copy, "MARKER_REPLACED_DURING_READ"),
    ("unlinked", _unlink, "MARKER_REPLACED_DURING_READ"),
    ("swapped for a symlink", _swap_symlink, "MARKER_REPLACED_DURING_READ"),
])
def test_a_marker_changed_during_inspection_is_uncertain_even_if_the_bytes_read_were_valid(
        tmp_path, monkeypatch, label, action, reason):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    marker = d / (SEG + ".meta.json")
    with _Ledger(monkeypatch, on_read=_mutating_read(marker, action)) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == (reason,), label
    assert v.evidence is None, "content from an unstable read must not be reported"
    assert ledger.live == {}


def test_a_short_read_of_an_unchanged_file_is_uncertain_never_parsed_as_a_truncated_marker(tmp_path, monkeypatch):
    """EOF before ``st_size`` bytes with identical before/after ``fstat`` (a lying filesystem, a racing
    layer below the VFS) must not be parsed: a truncated prefix would be reported as INVALID_CONTENT,
    a statement about the marker that was never established."""
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    calls = {"n": 0}

    def short_read(fd, n):
        data = _REAL_READ(fd, n)
        calls["n"] += 1
        return data[:-3] if calls["n"] == 1 and data else data

    with monkeypatch.context() as mp:
        mp.setattr(os, "read", short_read)
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("MARKER_CHANGED_DURING_READ",)
    assert v.evidence is None and v.bytes_read == v.observed_size - 3


# ---------------------------------------------------------------- 13. non-regular objects

@pytest.mark.parametrize("kind", ["directory", "fifo", "symlink"])
def test_non_regular_marker_paths_are_not_checked_and_never_opened(tmp_path, monkeypatch, kind):
    d = _put(tmp_path, None)
    path = d / (SEG + ".meta.json")
    if kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        outside = tmp_path / "outside.json"
        outside.write_bytes(_enc(_doc()))
        path.symlink_to(outside)
    result = discover_sources(tmp_path)
    with _deadline(), _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert result.records[0].marker_presence is MarkerPresence.NON_REGULAR
    assert v.status is S.NOT_CHECKED and v.reasons == ("MARKER_NOT_REGULAR_AT_DISCOVERY",)
    assert v.marker_presence is MarkerPresence.NON_REGULAR
    assert ledger.opens == [] and ledger.reads == [], "a non-regular marker path must never be opened"


def test_a_fifo_swapped_in_after_discovery_cannot_block_and_is_never_read(tmp_path, monkeypatch):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    marker = d / (SEG + ".meta.json")
    marker.unlink()
    os.mkfifo(marker)
    with _deadline(), _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("MARKER_NOT_REGULAR_AT_OPEN",)
    assert ledger.reads == [] and ledger.live == {}
    flags = [f for name, f, _ in ledger.opens if name.endswith(".meta.json")]
    assert flags and all(f & os.O_NONBLOCK and f & os.O_NOFOLLOW for f in flags)


def test_a_directory_swapped_in_after_discovery_is_uncertain(tmp_path):
    d = _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    marker = d / (SEG + ".meta.json")
    marker.unlink()
    marker.mkdir()
    (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("MARKER_NOT_REGULAR_AT_OPEN",)


# ---------------------------------------------------------------- 14. absence stays absence

def test_genuine_absence_is_preserved_and_never_confused_with_a_content_failure(tmp_path):
    _put(tmp_path, None, seg="2026-10-11-12-000000.seg")                          # no marker at all
    _put(tmp_path, b"garbage", seg="2026-10-11-12-000001.seg")                    # present, invalid
    _put(tmp_path, {"record_count": 1}, seg="2026-10-11-12-000002.seg")           # v1 hint
    result, by = _run(tmp_path)
    absent = by["raw/trades/2026-10-11-12-000000.seg"]
    assert (absent.status, absent.marker_presence, absent.reasons) == (
        S.NOT_CHECKED, MarkerPresence.ABSENT, ("MARKER_ABSENT_AT_DISCOVERY",))
    assert by["raw/trades/2026-10-11-12-000001.seg"].status is S.INVALID_CONTENT
    assert by["raw/trades/2026-10-11-12-000002.seg"].status is S.LEGACY_HINT_V1
    states = {r.relative_path.rsplit("/", 1)[1]: r.trust_state for r in result.records}
    assert states["2026-10-11-12-000000.seg"] is TrustState.UNCONFIRMED       # discovery's verdict is unchanged


# ---------------------------------------------------------------- 15. legacy hourly Parquet

def test_legacy_hourly_parquet_is_marker_not_applicable_and_nothing_is_opened(tmp_path, monkeypatch):
    d = _stream(tmp_path)
    (d / LEGACY).write_bytes(b"PAR1")
    (d / (LEGACY + ".meta.json")).write_bytes(_enc(_doc(seg=LEGACY)))            # a stray file must not matter
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert result.records[0].marker_presence is MarkerPresence.NOT_APPLICABLE
    assert v.status is S.NOT_APPLICABLE and v.reasons == ("MARKER_CONVENTION_NOT_APPLICABLE",)
    assert ledger.opens == []


# ---------------------------------------------------------------- 16. a claim is not a verification

def test_a_valid_marker_whose_declared_size_and_hash_disagree_with_the_segment_is_still_just_a_claim(
        tmp_path, monkeypatch):
    seg_bytes = b"these are the real segment bytes"
    _put(tmp_path, _doc(size_bytes=999_999, sha256="0" * 64), seg_bytes=seg_bytes)
    result = discover_sources(tmp_path)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.VALID_V2 and "SEGMENT_BYTES_NOT_VERIFIED" in v.reasons
    assert v.evidence.size_bytes == 999_999 and v.evidence.sha256 == "0" * 64       # preserved verbatim
    assert result.records[0].size_bytes == len(seg_bytes) != v.evidence.size_bytes
    assert v.evidence.sha256 != hashlib.sha256(seg_bytes).hexdigest()
    assert not any(name.endswith(".seg") for name, _f, _d in ledger.opens), "the segment must never be opened"
    assert result.records[0].trust_state is TrustState.UNKNOWN                      # no promotion, no CORRUPT


# ---------------------------------------------------------------- 17. existing precedence is respected

def test_collision_and_invalid_name_precedence_is_preserved_and_their_markers_are_never_opened(
        tmp_path, monkeypatch):
    d = _stream(tmp_path)
    (d / LEGACY).write_bytes(b"PAR1")                                            # collides with the segment below
    _put(tmp_path, _doc())                                                       # 2026-10-11-12-000003.seg
    bad = "2026-13-45-12-000001.seg"                                             # not a real date
    _put(tmp_path, _doc(seg=bad), seg=bad)
    link_seg = "2026-10-12-01-000000.seg"
    (tmp_path / "elsewhere.seg").write_bytes(b"x")
    (d / link_seg).symlink_to(tmp_path / "elsewhere.seg")
    (d / (link_seg + ".meta.json")).write_bytes(_enc(_doc(seg=link_seg)))
    result = discover_sources(tmp_path)
    states = {r.relative_path.rsplit("/", 1)[1]: r.trust_state for r in result.records}
    assert states[SEG] is TrustState.AMBIGUOUS and states[LEGACY] is TrustState.AMBIGUOUS
    assert states[bad] is TrustState.INVALID and states[link_seg] is TrustState.INVALID
    with _Ledger(monkeypatch) as ledger:
        by = {v.relative_path.rsplit("/", 1)[1]: v for v in validate_markers(tmp_path, result)}
    assert by[SEG].status is S.NOT_CHECKED and by[SEG].reasons == ("SOURCE_STATE_AMBIGUOUS",)
    assert by[LEGACY].status is S.NOT_APPLICABLE                                  # a format fact, even in a collision
    assert by[bad].status is S.NOT_CHECKED and by[bad].reasons == ("SOURCE_STATE_INVALID",)
    assert by[link_seg].status is S.NOT_CHECKED and by[link_seg].reasons == ("SOURCE_STATE_INVALID",)
    assert ledger.opens == [] and ledger.reads == []


# ---------------------------------------------------------------- 18. nothing is mutated

def test_validation_mutates_nothing_and_never_opens_for_writing(tmp_path, monkeypatch):
    _put(tmp_path, _doc(), seg="2026-10-11-12-000000.seg")
    _put(tmp_path, b"garbage", seg="2026-10-11-12-000001.seg")
    _put(tmp_path, b" " * 5000, seg="2026-10-11-12-000002.seg")
    _put(tmp_path, None, seg="2026-10-11-12-000004.seg")
    result = discover_sources(tmp_path)
    before = _snapshot(tmp_path)

    def refuse(label):
        def _raise(*a, **k):
            raise AssertionError(f"forbidden filesystem mutation: {label}")
        return _raise

    def read_only(path, flags):
        assert not flags & _WRITE_FLAGS, f"opened for writing/creation: {path}"
        assert _O_TMPFILE is None or flags & _O_TMPFILE != _O_TMPFILE, f"O_TMPFILE: {path}"

    with monkeypatch.context() as mp:
        for fn in ("rename", "replace", "remove", "unlink", "rmdir", "mkdir", "makedirs", "symlink", "link",
                   "chmod", "utime", "truncate", "write", "fsync", "ftruncate", "fchmod", "mkfifo"):
            if hasattr(os, fn):
                mp.setattr(os, fn, refuse(f"os.{fn}"))
        for fn in ("write_bytes", "write_text", "touch", "unlink", "rename", "replace", "mkdir", "rmdir"):
            mp.setattr(Path, fn, refuse(f"Path.{fn}"))
        with _Ledger(mp, on_open=read_only) as ledger:
            vals = validate_markers(tmp_path, result, max_marker_bytes=4096)
    assert _snapshot(tmp_path) == before
    assert len(vals) == 4 and ledger.live == {}


def test_validation_is_deterministic_and_follows_discovery_order(tmp_path):
    for i in range(5):
        _put(tmp_path, _doc(seg=f"2026-10-11-12-00000{i}.seg") if i % 2 else b"junk", seg=f"2026-10-11-12-00000{i}.seg")
    result = discover_sources(tmp_path)
    first = validate_markers(tmp_path, result)
    assert first == validate_markers(tmp_path, result)
    assert [v.relative_path for v in first] == [r.relative_path for r in result.records]
    with pytest.raises(dataclasses.FrozenInstanceError):
        first[0].status = S.VALID_V2


def test_without_safe_descriptor_relative_access_every_marker_is_unknown_not_read(tmp_path, monkeypatch):
    _put(tmp_path, _doc())
    result = discover_sources(tmp_path)
    monkeypatch.setattr(mv, "_SAFE_FLAGS", None)
    with _Ledger(monkeypatch) as ledger:
        (v,) = validate_markers(tmp_path, result)
    assert v.status is S.UNKNOWN_IO and v.reasons == ("SAFE_OPEN_UNAVAILABLE",)
    assert ledger.opens == []


# ---------------------------------------------------------------- the module stays inside its lane

#: Every ``os.<name>`` the module may touch. An allowlist, not a denylist: anything that could write,
#: create, rename, remove, lock or change metadata is simply not on it.
_ALLOWED_OS_ATTRS = {
    "open", "close", "read", "fstat", "stat", "fspath", "PathLike", "stat_result", "supports_dir_fd",
    "supports_follow_symlinks", "O_RDONLY", "O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK", "O_NOCTTY",
}


def test_module_source_has_no_recovery_mutation_hashing_or_parquet_dependencies():
    tree = ast.parse(Path(mv.__file__).read_text())
    forbidden_names = {
        "confirm_unmarked", "preserve_invalid", "orphan_marker", "quarantine_segment", "write_marker_atomic",
        "startup_reconcile", "iter_segments", "read_marker", "classify_segment", "sha256_file", "sha256_bytes",
        "read_segment_table", "fsync_file", "fsync_dir", "rmtree", "open", "hashlib", "pq", "pa",
    }
    forbidden_attrs = forbidden_names - {"open"} | {"read_bytes", "read_text", "write_bytes", "write_text",
                                                   "write", "unlink", "rename", "replace", "mkdir", "touch"}
    forbidden_modules = {"pandas", "pyarrow", "sqlite3", "shutil", "tempfile", "subprocess", "hashlib", "pathlib"}
    imported: set = set()
    os_attrs: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported.add(module.split(".")[0] if node.level == 0 else module)
            assert not module.endswith(("segment_dedup", "parquet_writer", "storage_layout")), module
            for alias in node.names:
                assert alias.name not in forbidden_names, f"forbidden import: {alias.name}"
        elif isinstance(node, ast.Name):
            assert node.id not in forbidden_names, f"forbidden name used (builtin open included): {node.id}"
        elif isinstance(node, ast.Attribute):
            assert node.attr not in forbidden_attrs, f"forbidden attribute used: {node.attr}"
            if isinstance(node.value, ast.Name) and node.value.id == "os":
                os_attrs.add(node.attr)
            else:
                assert node.attr != "open", "only os.open (descriptor-relative, no-follow) may open anything"
    assert not imported & forbidden_modules, imported & forbidden_modules
    assert os_attrs <= _ALLOWED_OS_ATTRS, os_attrs - _ALLOWED_OS_ATTRS


def test_discovery_module_still_never_opens_anything_and_does_not_know_about_validation():
    from collector.collector import source_catalog as sc
    text = Path(sc.__file__).read_text()
    assert "marker_validation" not in text and "parse_marker_bytes" not in text


# ---------------------------------------------------------------- 19. publication.py compatibility

_CORPUS = [
    ("valid", _enc(_doc())),
    ("valid, other segment", _enc(_doc(seg="other.seg"))),
    ("v1 hint", _enc({"record_count": 3, "first_record_ts": 1, "last_record_ts": 2})),
    ("empty object", b"{}"),
    ("empty", b""),
    ("invalid utf-8", b"\xff\xfe"),
    ("not json", b"nope"),
    ("list", b"[]"),
    ("publication not object", b'{"publication":3}'),
    ("schema 3 + bad sha", _enc(_doc(schema=3, sha256="xyz"))),
    ("bool size", _enc(_doc(size_bytes=True))),
    ("sha trailing newline", _enc(_doc(sha256=SHA + "\n"))),
]


@pytest.mark.parametrize("label,raw", _CORPUS)
def test_read_marker_and_the_shared_parser_agree_exactly(tmp_path, label, raw):
    seg = tmp_path / SEG
    seg.write_bytes(b"x")
    pub.marker_path(seg).write_bytes(raw)
    via_file = pub.read_marker(seg)
    via_bytes = pub.parse_marker_bytes(raw, SEG)
    assert via_file == via_bytes, label


_GOLDEN = {                                           # pinned from the pre-refactor implementation
    "valid": ("valid", ""),
    "valid, other segment": ("invalid", "segment name does not match"),
    "v1 hint": ("absent", "pre-F1 (v1) meta hint"),
    "empty object": ("absent", "pre-F1 (v1) meta hint"),
    "empty": ("invalid", "not valid JSON: JSONDecodeError"),
    "invalid utf-8": ("invalid", "not valid JSON: UnicodeDecodeError"),
    "not json": ("invalid", "not valid JSON: JSONDecodeError"),
    "list": ("invalid", "top level is not a JSON object"),
    "publication not object": ("invalid", "publication is not an object"),
    "schema 3 + bad sha": ("invalid", "schema != 2; sha256 invalid"),
    "bool size": ("invalid", "size_bytes invalid"),
    "sha trailing newline": ("invalid", "sha256 invalid"),
}


@pytest.mark.parametrize("label,raw", _CORPUS)
def test_read_marker_statuses_and_reasons_are_unchanged_for_recovery_callers(tmp_path, label, raw):
    seg = tmp_path / SEG
    seg.write_bytes(b"x")
    pub.marker_path(seg).write_bytes(raw)
    result = pub.read_marker(seg)
    assert (result.status.value, result.reason) == _GOLDEN[label]
    assert result.v1 is (label in {"v1 hint", "empty object"})


def test_read_marker_keeps_absence_and_reports_io_errors_as_unknown_io_not_invalid(tmp_path):
    seg = tmp_path / SEG
    seg.write_bytes(b"x")
    assert (pub.read_marker(seg).status, pub.read_marker(seg).reason) == (pub.MarkerStatus.ABSENT, "no marker file")
    pub.marker_path(seg).mkdir()                                     # an OSError that is not FileNotFoundError
    result = pub.read_marker(seg)
    # An unreadable marker is unknown evidence: it used to be reported as INVALID (malformed content),
    # which made startup recovery rename a marker it had never read. The reason text is unchanged.
    assert result.status is pub.MarkerStatus.UNKNOWN_IO and result.reason.startswith("unreadable: IsADirectoryError")
    assert result.status is not pub.MarkerStatus.INVALID and not result.valid and not result.v1
    assert pub.MARKER_SCHEMA == 2 and pub.ALLOWED_CONFIRMED_BY == {"writer", "startup_refsync"}


def test_the_writers_own_output_is_still_valid_after_the_shared_parser_refactor(tmp_path):
    seg = tmp_path / SEG
    seg.write_bytes(b"x" * 10)
    sha, size = pub.sha256_file(seg)
    pub.write_marker_atomic(seg, record_count=2, first_ts=1, last_ts=2, sha256=sha, size_bytes=size,
                            confirmed_by="writer", legacy=False)
    marker = pub.read_marker(seg)
    assert marker.valid and pub.classify_segment(seg, marker) is pub.PublicationState.PUBLICATION_CONFIRMED
    root = _as_root(tmp_path, seg)
    (v,) = validate_markers(root, discover_sources(root))
    assert v.status is S.VALID_V2 and v.evidence.sha256 == sha and v.evidence.size_bytes == size


def _as_root(tmp_path: Path, seg: Path) -> Path:
    """Lay the segment out as ``<root>/raw/trades/<seg>`` so the catalog can see it."""
    root = tmp_path / "catalog_root"
    d = _stream(root)
    (d / seg.name).write_bytes(seg.read_bytes())
    (d / (seg.name + ".meta.json")).write_bytes(pub.marker_path(seg).read_bytes())
    return root
