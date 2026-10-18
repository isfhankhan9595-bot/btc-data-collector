"""Adversarial tests for read-only Parquet metadata verification.

Positive fixtures are real Parquet written by the pinned PyArrow; negative fixtures are mutated or
hand-built bytes. Every test runs the real pipeline (discovery -> marker validation -> segment byte
verification -> Parquet metadata verification).
"""
import errno
import functools
import hashlib
import io
import json
import os
import re
import signal
import stat
import struct
import subprocess
import sys
import textwrap
import time
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector import parquet_metadata_validation as pmv
from collector.collector import publication
from collector.collector.marker_validation import MarkerValidationStatus, validate_markers
from collector.collector.parquet_metadata_validation import (
    DEFAULT_MAX_FOOTER_BYTES,
    MAX_ALLOWED_FOOTER_BYTES,
    ParquetMetadataStatus as P,
    verify_parquet_metadata,
)
from collector.collector.segment_validation import (
    SegmentVerificationStatus,
    verify_segment_bytes,
)
from collector.collector.source_catalog import TrustState, discover_sources

SEG = "2026-10-18-02-000003.seg"
LEGACY = "2026-10-18-02.parquet"
_REAL_READ = os.read
_REAL_PREAD = os.pread
SCHEMA = pa.schema([("ts", pa.int64()), ("v", pa.string())])


def _parquet(groups=((("aaaa", 1), ("bbbb", 2)), (("cccc", 3),)), compression="NONE") -> bytes:
    """Real Parquet. ``groups`` is a list of row groups, each a list of (v, ts) rows."""
    buf = io.BytesIO()
    writer = pq.ParquetWriter(buf, SCHEMA, compression=compression)
    for rows in groups:
        writer.write_table(pa.table(
            {"ts": [r[1] for r in rows], "v": [r[0] for r in rows]}, schema=SCHEMA))
    writer.close()
    return buf.getvalue()


def _footer_len(data: bytes) -> int:
    return struct.unpack("<I", data[-8:-4])[0]


def _marker_doc(name, payload, *, record_count=3, size_bytes=None, sha256=None, legacy=False):
    return publication.build_marker(
        name,
        record_count=record_count,
        first_ts=1792288800000,
        last_ts=1792288800100,
        sha256=sha256 if sha256 is not None else hashlib.sha256(payload).hexdigest(),
        size_bytes=size_bytes if size_bytes is not None else len(payload),
        confirmed_by="writer",
        legacy=legacy,
        boot_id="test-boot",
        confirmed_at_utc="2026-10-18T02:00:00Z",
    )


def _put(root: Path, *, name=SEG, payload=None, marker=True, stream="trades", **marker_kw):
    payload = _parquet() if payload is None else payload
    folder = root / "raw" / stream
    folder.mkdir(parents=True, exist_ok=True)
    segment = folder / name
    segment.write_bytes(payload)
    if marker:
        doc = _marker_doc(name, payload, **marker_kw)
        (folder / (name + ".meta.json")).write_bytes(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    return segment


def _stages(root: Path):
    discovery = discover_sources(root)
    markers = validate_markers(root, discovery)
    byte_results = verify_segment_bytes(root, discovery, markers)
    return discovery, markers, byte_results


def _run(root: Path, **kw):
    discovery, markers, byte_results = _stages(root)
    return discovery, markers, byte_results, verify_parquet_metadata(
        root, discovery, markers, byte_results, **kw)


_WORKER = pmv._worker                      # protocol constants shared with the production worker
WORKER_FILE = _WORKER.__file__
_FIXED_CODE = re.compile(r"[A-Z][A-Z0-9_]*")


def _verify_one(tmp_path, *, payload=None, record_count=3, **kw):
    """One marked segment through the real pipeline: ``(payload, result)``."""
    payload = _parquet() if payload is None else payload
    _put(tmp_path, payload=payload, record_count=record_count)
    _d, _m, _b, (result,) = _run(tmp_path, **kw)
    return payload, result


def _assert_no_parse_facts(result):
    """Nothing that depends on a successful footer parse may be claimed."""
    assert result.num_rows is None and result.num_row_groups is None and result.num_columns is None
    assert result.declared_record_count_matches is None
    assert result.marker_claims_corroborated_by_bytes is None


def _assert_byte_facts_kept(result, payload):
    """The completed, stable hash pass established these whatever the parser did afterwards."""
    assert result.observed_size == len(payload)
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.bytes_hashed == len(payload)
    assert result.byte_evidence_matches_prior is True
    assert result.declared_size_matches is True and result.declared_sha256_matches is True
    assert result.footer_length == _footer_len(payload)


def _assert_fixed_diagnostics(result, tmp_path, *secrets):
    """Reasons are fixed codes: no raw exception text, process output or absolute path."""
    assert result.reasons and all(_FIXED_CODE.fullmatch(code) for code in result.reasons), result.reasons
    text = repr(result)
    for secret in (str(tmp_path), "Traceback", "std::", "Couldn't", *secrets):
        assert secret not in text


_SCRIPTED_PRELUDE = '''
import os, signal, struct, sys, time
try:
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))      # a deliberate abort must not write a core file
except Exception:
    pass
HDR = struct.Struct("<4sQQQ")
REPLY = struct.Struct("<4sBBBBQQQ32s")

def read_exact(n):
    out = b""
    while len(out) < n:
        chunk = os.read(0, min(1 << 20, n - len(out)))
        if not chunk:
            raise SystemExit(9)
        out += chunk
    return out

def read_request():
    magic, footer_len, size, budget = HDR.unpack(read_exact(HDR.size))
    return footer_len, size, budget, read_exact(footer_len + 8)

def send(code, detail=0, problems=0, counts=(0, 0, 0), name=b"", magic=b"PQR1", reserved=0):
    os.write(1, REPLY.pack(magic, code, detail, problems, reserved, *counts, name))
'''


@pytest.fixture
def scripted_worker(tmp_path_factory, monkeypatch):
    """Install a stand-in worker process. It is NOT the production worker: it exists to make the
    parent's handling of crashes, hangs and malformed replies deterministic, with a real subprocess,
    real pipes and the production parent code."""
    def install(code: str) -> Path:
        script = tmp_path_factory.mktemp("scripted") / "worker.py"
        script.write_text(_SCRIPTED_PRELUDE + textwrap.dedent(code))
        monkeypatch.setattr(pmv, "_worker_argv", lambda: [sys.executable, "-I", str(script)])
        return script
    return install


_INJECTED_HEAD = '''
import importlib.util, sys
spec = importlib.util.spec_from_file_location("production_worker", %r)
mod = importlib.util.module_from_spec(spec)
sys.modules["production_worker"] = mod
spec.loader.exec_module(mod)
pa = mod.pa
'''
_INJECTED_TAIL = '''
mod.pq.ParquetFile = fake
sys.exit(mod.main())
'''


@pytest.fixture
def injected_parse(tmp_path_factory, monkeypatch):
    """Run the PRODUCTION worker loop, reply encoding and exception classification in a real
    subprocess, with only the PyArrow ``ParquetFile`` call replaced by ``fake``. The parse itself is
    therefore simulated (a deterministic mock); the process boundary, protocol and every decision
    around it are the real code."""
    def install(fake_source: str) -> Path:
        script = tmp_path_factory.mktemp("injected") / "worker.py"
        script.write_text(_INJECTED_HEAD % WORKER_FILE + textwrap.dedent(fake_source) + _INJECTED_TAIL)
        monkeypatch.setattr(pmv, "_worker_argv", lambda: [sys.executable, "-I", str(script)])
        return script
    return install


def _raises(expression: str) -> str:
    return f"def fake(*a, **k):\n    raise {expression}\n"


@functools.lru_cache(maxsize=None)
def _many_row_groups(count: int) -> bytes:
    """A real Parquet file with ``count`` one-row row groups: a large footer, tiny data."""
    return _parquet(groups=tuple(((f"v{i:06d}", i),) for i in range(count)))


def _snapshot(root: Path):
    out = {}
    for directory, subdirs, names in os.walk(root, followlinks=False):
        for name in sorted(subdirs + names):
            path = Path(directory) / name
            st = path.lstat()
            content = (hashlib.sha256(path.read_bytes()).hexdigest()
                       if stat.S_ISREG(st.st_mode) else None)
            link = os.readlink(path) if stat.S_ISLNK(st.st_mode) else None
            out[str(path.relative_to(root))] = (st.st_mode, st.st_size, st.st_mtime_ns, content, link)
    return out


# ----------------------------------------------------------------------------------------------
# positive behaviour and marker comparisons
# ----------------------------------------------------------------------------------------------

def test_valid_parquet_reports_footer_facts_and_matching_marker_without_trust_or_mutation(tmp_path):
    payload = _parquet()
    _put(tmp_path, payload=payload, record_count=3)
    before = _snapshot(tmp_path)
    discovery, markers, byte_results, (result,) = _run(tmp_path)

    assert markers[0].status is MarkerValidationStatus.VALID_V2
    assert byte_results[0].status is SegmentVerificationStatus.HASHED_STABLE
    assert result.status is P.VALID_PARQUET
    assert (result.num_rows, result.num_row_groups, result.num_columns) == (3, 2, 2)
    assert result.footer_length == _footer_len(payload)
    assert result.observed_size == len(payload)
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.bytes_hashed == len(payload)
    assert result.byte_evidence_matches_prior is True
    assert result.declared_size_matches is True
    assert result.declared_sha256_matches is True
    assert result.declared_record_count_matches is True
    assert result.marker_claims_corroborated_by_bytes is True
    assert result.reasons == ("PARQUET_METADATA_VALID",)
    assert not hasattr(result, "trust_state")
    assert discovery.records[0].trust_state is TrustState.UNKNOWN
    assert _snapshot(tmp_path) == before, "verification must not modify data, markers or directories"


def test_marker_row_count_mismatch_is_explicit_and_observed_count_is_reported_independently(tmp_path):
    _put(tmp_path, record_count=99)
    _d, _m, _b, (result,) = _run(tmp_path)

    assert result.status is P.VALID_PARQUET
    assert result.num_rows == 3, "observed count must not be replaced by the marker's claim"
    assert result.declared_record_count_matches is False
    assert "DECLARED_RECORD_COUNT_MISMATCH" in result.reasons
    assert result.marker_claims_corroborated_by_bytes is True


@pytest.mark.parametrize(
    "kw,reason,field",
    [
        ({"size_bytes": 12345}, "DECLARED_SIZE_MISMATCH", "declared_size_matches"),
        ({"sha256": "f" * 64}, "DECLARED_SHA256_MISMATCH", "declared_sha256_matches"),
    ],
)
def test_size_or_hash_mismatch_never_disappears_into_success_and_blocks_the_count_comparison(
        tmp_path, kw, reason, field):
    # The marker's record_count equals the real row count: a "match" here would be a false
    # confirmation, because the marker's bytes claim does not describe this file.
    _put(tmp_path, record_count=3, **kw)
    _d, _m, _b, (result,) = _run(tmp_path)

    assert result.status is P.VALID_PARQUET
    assert getattr(result, field) is False
    assert reason in result.reasons
    assert result.marker_claims_corroborated_by_bytes is False
    assert result.declared_record_count_matches is None
    assert "RECORD_COUNT_COMPARISON_SKIPPED_BYTES_UNCORROBORATED" in result.reasons
    assert result.num_rows == 3


def test_unmarked_segment_is_inspected_without_inferred_marker_evidence(tmp_path):
    _put(tmp_path, marker=False)
    discovery, markers, _b, (result,) = _run(tmp_path)

    assert markers[0].status is MarkerValidationStatus.NOT_CHECKED
    assert result.status is P.VALID_PARQUET
    assert result.num_rows == 3
    assert result.declared_size_matches is None
    assert result.declared_sha256_matches is None
    assert result.declared_record_count_matches is None
    assert result.marker_claims_corroborated_by_bytes is None
    assert discovery.records[0].trust_state is TrustState.UNCONFIRMED


def test_legacy_hourly_parquet_is_inspected_with_its_weaker_evidence_contract(tmp_path):
    _put(tmp_path, name=LEGACY, marker=False)
    discovery, markers, _b, (result,) = _run(tmp_path)

    assert markers[0].status is MarkerValidationStatus.NOT_APPLICABLE
    assert result.status is P.VALID_PARQUET
    assert result.num_rows == 3
    assert result.declared_record_count_matches is None
    assert result.marker_claims_corroborated_by_bytes is None
    assert discovery.records[0].trust_state is TrustState.LEGACY


def test_marker_declared_legacy_provenance_is_visible_in_reasons(tmp_path):
    _put(tmp_path, legacy=True)
    _d, _m, _b, (result,) = _run(tmp_path)
    assert result.status is P.VALID_PARQUET
    assert "DECLARED_MARKER_LEGACY" in result.reasons


def test_metadata_inspection_never_decodes_rows_and_never_reopens_the_path(tmp_path, monkeypatch):
    import pandas

    payload = _parquet()
    _put(tmp_path, payload=payload)
    discovery, markers, byte_results = _stages(tmp_path)

    def forbidden(*_a, **_k):
        raise AssertionError("row data / path-based reads are forbidden in metadata verification")

    for target, name in [
        (pq.ParquetFile, "read"), (pq.ParquetFile, "read_row_group"),
        (pq.ParquetFile, "read_row_groups"), (pq.ParquetFile, "iter_batches"),
        (pq, "read_table"), (pq, "read_metadata"), (pandas, "read_parquet"),
    ]:
        monkeypatch.setattr(target, name, forbidden)
    received = []
    real_parse = pmv._ParserWorker.parse

    def spy(self, window, size, footer_len):
        received.append((bytes(window), size, footer_len))
        return real_parse(self, window, size, footer_len)

    monkeypatch.setattr(pmv._ParserWorker, "parse", spy)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)

    flen = _footer_len(payload)
    assert result.status is P.VALID_PARQUET
    assert received == [(payload[-(flen + 8):], len(payload), flen)], (
        "the parser side receives exactly the captured, hashed footer window plus two integers -- "
        "never a path, a descriptor or a re-opened file")


def test_worker_parses_an_in_memory_buffer_and_nothing_else(monkeypatch):
    payload = _parquet()
    flen = _footer_len(payload)
    sources = []
    real_file = pq.ParquetFile

    def spy(source, **kwargs):
        sources.append(source)
        return real_file(source, **kwargs)

    monkeypatch.setattr(_WORKER.pq, "ParquetFile", spy)
    outcome = _WORKER.parse_footer(payload[-(flen + 8):], len(payload), flen)
    assert outcome.code == _WORKER.CODE_VALID and outcome.num_rows == 3
    assert len(sources) == 1 and isinstance(sources[0], pa.BufferReader)


# ----------------------------------------------------------------------------------------------
# cross-stage binding
# ----------------------------------------------------------------------------------------------

def test_same_size_replacement_between_stages_is_unknown_and_yields_no_metadata_facts(tmp_path):
    original = _parquet(groups=((("aaaa", 1), ("bbbb", 2)),))
    replacement = _parquet(groups=((("zzzz", 9), ("yyyy", 8)),))
    assert len(original) == len(replacement) and original != replacement
    seg = _put(tmp_path, payload=original, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    st = seg.stat()
    seg.write_bytes(replacement)                  # in place: same inode, same size
    os.utime(seg, ns=(st.st_atime_ns, st.st_mtime_ns))   # and the mtime put back

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "BYTES_CHANGED_SINCE_BYTE_VERIFICATION" in result.reasons
    assert "BYTE_VERIFICATION_SHA256_DIFFERS" in result.reasons
    assert result.byte_evidence_matches_prior is False
    assert result.num_rows is None and result.num_row_groups is None and result.num_columns is None
    assert result.observed_sha256 == hashlib.sha256(replacement).hexdigest()


def test_same_size_replacement_by_rename_with_a_different_inode_is_also_detected(tmp_path):
    original = _parquet(groups=((("aaaa", 1),),))
    replacement = _parquet(groups=((("bbbb", 1),),))
    assert len(original) == len(replacement)
    seg = _put(tmp_path, payload=original, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    other = seg.with_name("replacement.tmp")
    other.write_bytes(replacement)
    os.replace(other, seg)

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.byte_evidence_matches_prior is False
    assert result.num_rows is None


def test_size_change_between_stages_is_detected_without_reading_the_file(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    seg.write_bytes(_parquet(groups=((("aaaa", 1),),)) + b"\x00")

    reads = []
    monkeypatch.setattr(pmv.os, "read", lambda *a: reads.append(a) or _REAL_READ(*a))
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "BYTE_VERIFICATION_SIZE_DIFFERS" in result.reasons
    assert reads == []


def test_a_file_the_byte_stage_could_not_hash_stably_is_never_opened(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    unstable = replace(byte_results[0], status=SegmentVerificationStatus.UNKNOWN_IO,
                       reasons=("SEGMENT_READ_FAILED", "ERRNO_EIO"), observed_size=None,
                       observed_sha256=None, bytes_hashed=0, declared_size_matches=None,
                       declared_sha256_matches=None)
    opened = []
    real_open = os.open
    monkeypatch.setattr(pmv.os, "open",
                        lambda p, *a, **k: opened.append(os.fspath(p)) or real_open(p, *a, **k))
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, (unstable,))
    assert result.status is P.NOT_CHECKED
    assert result.reasons == ("BYTE_VERIFICATION_NOT_HASHED_STABLE", "PRIOR_STATUS_UNKNOWN_IO")
    assert seg.name not in opened


@pytest.mark.parametrize("mutate,match", [
    (lambda b: replace(b, observed_sha256="not-a-digest"), "observed_sha256"),
    (lambda b: replace(b, bytes_hashed=b.observed_size - 1), "bytes_hashed"),
    (lambda b: replace(b, reasons=("something",)), "reasons"),
    (lambda b: replace(b, declared_size_matches=True), "declared_matches"),
    (lambda b: replace(b, relative_path="raw/trades/other.seg"), "match discovery order"),
])
def test_fabricated_or_misaligned_byte_verification_is_rejected_before_any_filesystem_access(
        tmp_path, monkeypatch, mutate, match):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    monkeypatch.setattr(pmv.os, "open", lambda *a, **k: pytest.fail("opened before validation"))
    with pytest.raises(ValueError, match=match):
        verify_parquet_metadata(tmp_path, discovery, markers, (mutate(byte_results[0]),))


def test_input_sequences_must_each_have_one_entry_per_discovery_record(tmp_path):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    with pytest.raises(ValueError, match="one result per discovery record"):
        verify_parquet_metadata(tmp_path, discovery, markers, ())


# ----------------------------------------------------------------------------------------------
# filesystem safety
# ----------------------------------------------------------------------------------------------

def test_file_missing_after_byte_verification_is_unknown_not_absent_or_corrupt(tmp_path):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    seg.unlink()
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_MISSING_AT_OPEN",)
    assert result.num_rows is None


def test_symlink_substituted_after_byte_verification_is_not_followed(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(_parquet())          # a perfectly valid Parquet file: must still not be read
    seg.unlink()
    seg.symlink_to(outside)
    reads = []
    monkeypatch.setattr(pmv.os, "read", lambda *a: reads.append(a) or _REAL_READ(*a))

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_SYMLINK_AT_OPEN",)
    assert result.num_rows is None and result.observed_sha256 is None
    assert reads == []


def test_symlinked_stream_directory_is_not_followed(tmp_path):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    real = tmp_path / "raw" / "trades"
    moved = tmp_path / "elsewhere"
    real.rename(moved)
    (tmp_path / "raw" / "trades").symlink_to(moved)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)


def test_directory_substituted_for_the_file_is_not_regular_and_not_corrupt(tmp_path):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    seg.unlink()
    seg.mkdir()
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_NOT_REGULAR_AT_OPEN" in result.reasons


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs FIFOs")
def test_fifo_substituted_for_the_file_does_not_block(tmp_path):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    seg.unlink()
    os.mkfifo(seg)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_NOT_REGULAR_AT_OPEN" in result.reasons


def test_unsupported_safe_open_fails_closed_and_opens_nothing(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    monkeypatch.setattr(pmv, "_SAFE_FLAGS", None)
    monkeypatch.setattr(pmv.os, "open", lambda *a, **k: pytest.fail("no fallback open allowed"))
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SAFE_OPEN_UNAVAILABLE",)


def test_read_error_is_unknown_io_with_fixed_diagnostics_never_corruption(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    def fail_read(_fd, _count):
        raise OSError(errno.EIO, "injected path-bearing error " + str(tmp_path))

    monkeypatch.setattr(pmv.os, "read", fail_read)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_READ_FAILED", "ERRNO_EIO")
    assert result.observed_sha256 is None and result.num_rows is None
    assert "injected" not in repr(result) and str(tmp_path) not in repr(result)


def test_trailer_read_error_is_unknown_io_never_corruption(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    def fail_pread(*_a):
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(pmv.os, "pread", fail_pread)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_READ_FAILED", "ERRNO_EIO")


def test_short_trailer_read_is_unknown_io(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    monkeypatch.setattr(pmv.os, "pread", lambda fd, n, off: _REAL_PREAD(fd, n - 1, off))
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_SHORT_READ",)


def test_short_read_mid_file_is_unknown_io_not_a_truncated_parquet_verdict(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    calls = []

    def empty_read(_fd, _n):
        calls.append(1)
        if len(calls) > 20:
            pytest.fail("read loop did not terminate on an empty read (would spin forever)")
        return b""

    monkeypatch.setattr(pmv.os, "read", empty_read)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_SHORT_READ",)
    assert result.observed_sha256 is None


def test_file_growing_during_the_pass_is_unknown(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    changed = False

    def read_then_append(fd, count):
        nonlocal changed
        chunk = _REAL_READ(fd, count)
        if chunk and not changed:
            changed = True
            with seg.open("ab") as handle:
                handle.write(b"x")
        return chunk

    monkeypatch.setattr(pmv.os, "read", read_then_append)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_GREW_DURING_READ" in result.reasons
    assert result.num_rows is None


def test_identity_change_during_the_pass_is_unknown(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    touched = False

    def read_then_touch(fd, count):
        nonlocal touched
        chunk = _REAL_READ(fd, count)
        if chunk and not touched:
            touched = True
            st = seg.stat()
            os.utime(seg, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
        return chunk

    monkeypatch.setattr(pmv.os, "read", read_then_touch)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_CHANGED_DURING_READ" in result.reasons


def test_path_replaced_during_the_pass_is_unknown(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    replaced = False

    def read_then_replace(fd, count):
        nonlocal replaced
        chunk = _REAL_READ(fd, count)
        if chunk and not replaced:
            replaced = True
            other = seg.with_name("swap.tmp")
            other.write_bytes(seg.read_bytes())      # identical bytes, different inode
            os.replace(other, seg)
        return chunk

    monkeypatch.setattr(pmv.os, "read", read_then_replace)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_REPLACED_DURING_READ" in result.reasons


# ----------------------------------------------------------------------------------------------
# content classification
# ----------------------------------------------------------------------------------------------

def _classify(tmp_path, payload):
    _put(tmp_path, payload=payload, marker=False)
    _d, _m, byte_results, (result,) = _run(tmp_path)
    assert byte_results[0].status is SegmentVerificationStatus.HASHED_STABLE
    return result


@pytest.mark.parametrize("make,reason", [
    (lambda v: v[:11], "PARQUET_FILE_TOO_SHORT"),
    (lambda v: b"", "PARQUET_FILE_TOO_SHORT"),
    (lambda v: b"XXXX" + v[4:], "PARQUET_HEAD_MAGIC_INVALID"),
    (lambda v: v[:-4] + b"XXXX", "PARQUET_TAIL_MAGIC_INVALID"),
    (lambda v: v[: len(v) // 2], "PARQUET_TAIL_MAGIC_INVALID"),                 # truncated file
    (lambda v: v[:-8] + struct.pack("<I", 0) + b"PAR1", "PARQUET_FOOTER_LENGTH_ZERO"),
    (lambda v: v[:-8] + struct.pack("<I", len(v)) + b"PAR1", "PARQUET_FOOTER_LENGTH_EXCEEDS_FILE"),
    (lambda v: v[:-8] + struct.pack("<I", 0xFFFFFFFF) + b"PAR1", "PARQUET_FOOTER_LENGTH_EXCEEDS_FILE"),
])
def test_structural_corruption_is_invalid_parquet_from_content_evidence(tmp_path, make, reason):
    result = _classify(tmp_path, make(_parquet()))
    assert result.status is P.INVALID_PARQUET
    assert reason in result.reasons
    assert result.num_rows is None and result.num_row_groups is None
    assert result.observed_sha256 is not None, "the bytes WERE read stably; only their structure is bad"


def test_garbage_footer_between_valid_magics_is_a_parse_failure(tmp_path):
    payload = b"PAR1" + b"\x00" * 64 + struct.pack("<I", 50) + b"PAR1"
    result = _classify(tmp_path, payload)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons[0] == "PARQUET_FOOTER_PARSE_FAILED"
    assert result.footer_length == 50
    assert "thrift" not in repr(result).lower(), "no raw parser text in diagnostics"


def test_data_bytes_removed_from_under_an_intact_footer_are_out_of_bounds(tmp_path):
    good = _parquet()
    flen = _footer_len(good)
    head, footer = good[:-(flen + 8)], good[-(flen + 8):]
    result = _classify(tmp_path, head[:4] + head[24:] + footer)      # 20 data bytes missing
    assert result.status is P.INVALID_PARQUET
    assert "PARQUET_COLUMN_CHUNK_OUT_OF_BOUNDS" in result.reasons


def test_invalid_structure_still_reports_marker_byte_comparisons(tmp_path):
    payload = _parquet()[:-4] + b"XXXX"
    _put(tmp_path, payload=payload, record_count=3, sha256="f" * 64)
    _d, _m, _b, (result,) = _run(tmp_path)
    assert result.status is P.INVALID_PARQUET
    assert result.declared_sha256_matches is False
    assert result.declared_size_matches is True
    assert "DECLARED_SHA256_MISMATCH" in result.reasons
    assert result.declared_record_count_matches is None


def test_parser_reported_unsupported_feature_is_not_called_corruption(tmp_path, injected_parse):
    """Unsupported-feature control: ArrowNotImplementedError stays UNSUPPORTED_METADATA."""
    injected_parse(_raises(f'pa.ArrowNotImplementedError("injected feature text " + {str(tmp_path)!r})'))
    payload, result = _verify_one(tmp_path)
    assert result.status is P.UNSUPPORTED_METADATA
    assert result.reasons == ("PARQUET_METADATA_UNSUPPORTED",)
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    assert "injected" not in repr(result) and str(tmp_path) not in repr(result)


def test_parser_oserror_on_in_memory_bytes_is_content_evidence(tmp_path, injected_parse):
    """Malformed-content control: PyArrow reports thrift failures as OSError; for an in-memory buffer
    that is not an I/O fault, and (unlike the std::bad_alloc signature) it is evidence of corruption."""
    injected_parse(_raises(
        f'OSError("Couldn\'t deserialize thrift: TProtocolException: Invalid data " + {str(tmp_path)!r})'))
    payload, result = _verify_one(tmp_path)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_FOOTER_PARSE_FAILED",)
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    assert str(tmp_path) not in repr(result)


def test_real_thrift_failure_text_is_malformed_content_not_a_resource_failure(tmp_path):
    """The real PyArrow 24.0.0 message for ordinary garbage must keep meaning INVALID_PARQUET."""
    payload = b"PAR1" + b"\x00" * 64 + struct.pack("<I", 50) + b"PAR1"
    _put(tmp_path, payload=payload, record_count=3)
    _d, _m, _b, (result,) = _run(tmp_path)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_FOOTER_PARSE_FAILED",)
    assert result.status not in (P.UNKNOWN_RESOURCE, P.UNKNOWN_PARSER_FAILURE)


def test_trailer_that_differs_between_preread_and_captured_bytes_fails_closed(tmp_path, monkeypatch):
    """The capture window is sized from a positioned pre-read of the trailer. If the bytes captured
    during the hashing pass disagree with that pre-read, neither observation may be trusted."""
    payload = _parquet()
    _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    flen = _footer_len(payload)
    lying = struct.pack("<I", flen - 1) + b"PAR1"          # still plausible, so a smaller window is used
    monkeypatch.setattr(pmv.os, "pread", lambda fd, n, off: lying)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("TRAILER_CHANGED_DURING_READ",)
    assert result.num_rows is None


def test_footer_whose_row_group_counts_do_not_sum_to_its_total_is_invalid(tmp_path, injected_parse):
    """A stub stands in for a footer that parses but contradicts itself (PyArrow offers no way to
    write one); the check must reject it rather than report the inconsistent totals."""
    injected_parse('''
        class _Group:
            num_rows, num_columns = 3, 0

        class _Meta:
            num_rows, num_row_groups, num_columns = 5, 1, 1

            def row_group(self, _i):
                return _Group()

        class _Parsed:
            metadata = _Meta()

        def fake(*a, **k):
            return _Parsed()
    ''')
    payload, result = _verify_one(tmp_path)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH",)
    _assert_no_parse_facts(result)
    assert result.num_rows is None


@pytest.mark.parametrize("expression", [
    "RuntimeError('boom')", "ValueError('boom')", "KeyError('boom')", "AssertionError('boom')",
])
def test_unexpected_programmer_errors_are_not_swallowed_into_invalid_parquet(
        tmp_path, injected_parse, expression):
    """Unexpected-error control (the replacement for the old MemoryError-propagation test): an error
    that is neither malformed-content evidence, an unsupported feature nor a recognised resource
    failure must stay loud. It must NOT become INVALID_PARQUET (or any other verdict about the file)."""
    injected_parse(_raises(expression.replace("'boom'", repr("boom " + str(tmp_path)))))
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    with pytest.raises(pmv.ParquetParserUnexpectedError) as caught:
        verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    message = str(caught.value)
    assert expression.split("(")[0] in message, "the exception *type* is reported"
    assert "boom" not in message and str(tmp_path) not in message, "never the exception text or a path"


def test_a_recognised_memory_error_is_a_resource_outcome_not_a_crash_and_not_corruption(
        tmp_path, injected_parse):
    """The old test expected an injected Python MemoryError to propagate out of the verifier. That is
    deliberately replaced: MemoryError from the parser is recognised resource exhaustion."""
    injected_parse(_raises("MemoryError()"))
    payload, result = _verify_one(tmp_path)
    assert result.status is P.UNKNOWN_RESOURCE
    assert result.status is not P.INVALID_PARQUET


# ----------------------------------------------------------------------------------------------
# footer resource bound
# ----------------------------------------------------------------------------------------------

def _track_buffer_sizes(monkeypatch):
    """Record the length of every ``bytearray`` the verifier grows with ``+=`` (its ``head`` and
    ``window`` capture buffers). The module-global name shadows the builtin only inside ``pmv``."""
    sizes = []

    class _Spy(bytearray):
        def __iadd__(self, other):
            result = super().__iadd__(other)
            sizes.append(len(self))
            return result

    monkeypatch.setattr(pmv, "bytearray", _Spy, raising=False)
    return sizes


def _forbid_parser(monkeypatch):
    """Nothing may reach the parser side: no footer handed over, and not even a worker process
    started (so no footer byte could have been passed to PyArrow in any process)."""
    def forbidden(*_a, **_k):
        pytest.fail("this footer must never reach the parser: no worker may be used or started")

    monkeypatch.setattr(pmv._ParserWorker, "parse", forbidden)
    monkeypatch.setattr(pmv.subprocess, "Popen", forbidden)


def _many_row_group_pair():
    """Two real, distinct Parquet files of exactly the same size with footers > 1000 bytes."""
    original = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    replacement = _parquet(groups=tuple(((f"w{i:03d}", i),) for i in range(50)))
    assert len(original) == len(replacement) and original != replacement
    assert _footer_len(original) > 1000 and _footer_len(replacement) > 1000
    return original, replacement


def test_oversized_footer_is_rejected_before_the_footer_is_buffered_or_parsed(tmp_path, monkeypatch):
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    flen = _footer_len(payload)
    assert flen > 1000
    sizes = _track_buffer_sizes(monkeypatch)
    _forbid_parser(monkeypatch)

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=flen - 1)
    assert result.status is P.FOOTER_TOO_LARGE
    assert result.reasons == ("FOOTER_EXCEEDS_LIMIT",)
    assert result.footer_length == flen
    assert result.num_rows is None and result.num_row_groups is None and result.num_columns is None
    # The oversized footer is never buffered: only the 4-byte head and 8-byte trailer are captured.
    assert sizes and max(sizes) <= pmv._TRAILER_BYTES < flen


def test_oversized_footer_is_reported_only_after_the_full_digest_matches_the_prior_evidence(tmp_path):
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    _put(tmp_path, payload=payload)                      # real VALID_V2 marker
    discovery, markers, byte_results = _stages(tmp_path)
    (prior,) = byte_results
    assert prior.status is SegmentVerificationStatus.HASHED_STABLE

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=_footer_len(payload) - 1)
    assert result.status is P.FOOTER_TOO_LARGE
    assert result.byte_evidence_matches_prior is True
    assert result.observed_size == len(payload)
    assert result.bytes_hashed == len(payload)           # the complete sequential pass finished
    assert result.observed_sha256 == prior.observed_sha256 == hashlib.sha256(payload).hexdigest()
    # Byte-level marker comparisons are facts about the hashed bytes; footer-derived ones are not made.
    assert result.declared_size_matches is True and result.declared_sha256_matches is True
    assert result.declared_record_count_matches is None
    assert result.num_rows is None


def test_invalid_leading_magic_is_classified_invalid_parquet_before_oversized_footer(
        tmp_path, monkeypatch):
    """Regression: with an unchanged file (digest equals the prior byte-verification evidence), an
    invalid leading magic used to be masked as FOOTER_TOO_LARGE because the oversized-footer return
    preceded the structural checks. Precedence: digest match -> provable structural errors ->
    footer size limit."""
    good = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    flen = _footer_len(good)
    assert flen > 1000
    bad = b"XXXX" + good[4:]                      # leading magic destroyed; trailer still plausible
    assert bad[:4] != pmv.PARQUET_MAGIC and bad[-4:] == pmv.PARQUET_MAGIC
    assert _footer_len(bad) == flen and flen <= len(bad) - pmv._MIN_PARQUET_BYTES
    _put(tmp_path, payload=bad, marker=False)

    # The real byte-verification stage hashes exactly these malformed bytes, so the prior digest
    # legitimately matches the file under test.
    discovery, markers, byte_results = _stages(tmp_path)
    (prior,) = byte_results
    assert prior.status is SegmentVerificationStatus.HASHED_STABLE
    assert prior.observed_size == len(bad)
    assert prior.observed_sha256 == hashlib.sha256(bad).hexdigest()

    sizes = _track_buffer_sizes(monkeypatch)
    _forbid_parser(monkeypatch)
    for name in ("read_metadata", "read_table", "read_schema"):
        monkeypatch.setattr(
            pq, name,
            lambda *a, **k: pytest.fail("an oversized footer must never reach PyArrow"))

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=flen - 1)

    assert result.status is P.INVALID_PARQUET
    assert result.status is not P.FOOTER_TOO_LARGE
    assert result.reasons == ("PARQUET_HEAD_MAGIC_INVALID",)
    assert "FOOTER_EXCEEDS_LIMIT" not in result.reasons
    # The classification is only reached after the full digest matched the prior evidence.
    assert result.byte_evidence_matches_prior is True
    assert result.observed_size == len(bad)
    assert result.bytes_hashed == len(bad)
    assert result.observed_sha256 == prior.observed_sha256
    # No footer-derived facts are fabricated.
    assert result.footer_length == flen
    assert result.num_rows is None and result.num_row_groups is None and result.num_columns is None
    # The oversized footer is never buffered: only the 4-byte head and 8-byte trailer are captured.
    assert sizes and max(sizes) <= pmv._TRAILER_BYTES < flen


def test_buffer_spy_control_a_footer_within_the_limit_is_captured_in_full(tmp_path, monkeypatch):
    """Proves the buffer spy used by the oversized tests can actually see a footer capture."""
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    sizes = _track_buffer_sizes(monkeypatch)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.VALID_PARQUET
    assert max(sizes) == pmv._TRAILER_BYTES + _footer_len(payload)


@pytest.mark.parametrize("how", ["in_place", "rename"])
def test_oversized_footer_cannot_bypass_prior_digest_verification_via_same_size_replacement(
        tmp_path, monkeypatch, how):
    """Regression: a footer over the limit used to return FOOTER_TOO_LARGE before the file was hashed,
    so a same-size replacement made after byte verification was reported as an oversized-footer
    version of the verified file."""
    original, replacement = _many_row_group_pair()
    seg = _put(tmp_path, payload=original, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    (prior,) = byte_results
    assert prior.status is SegmentVerificationStatus.HASHED_STABLE
    if how == "in_place":
        st = seg.stat()
        seg.write_bytes(replacement)
        os.utime(seg, ns=(st.st_atime_ns, st.st_mtime_ns))
    else:
        other = seg.with_name("replacement.tmp")
        other.write_bytes(replacement)
        os.replace(other, seg)
    assert seg.stat().st_size == prior.observed_size
    limit = _footer_len(replacement) - 1                 # below the replacement's declared footer
    sizes = _track_buffer_sizes(monkeypatch)
    _forbid_parser(monkeypatch)

    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=limit)
    assert result.status is P.UNKNOWN_IO
    assert result.status is not P.FOOTER_TOO_LARGE
    assert "FOOTER_EXCEEDS_LIMIT" not in result.reasons
    assert "BYTES_CHANGED_SINCE_BYTE_VERIFICATION" in result.reasons
    assert "BYTE_VERIFICATION_SHA256_DIFFERS" in result.reasons
    assert result.byte_evidence_matches_prior is False
    assert result.observed_sha256 == hashlib.sha256(replacement).hexdigest() != prior.observed_sha256
    assert result.bytes_hashed == len(replacement)
    assert result.footer_length is None
    assert result.num_rows is None and result.num_row_groups is None and result.num_columns is None
    assert max(sizes) <= pmv._TRAILER_BYTES


def test_oversized_footer_file_that_grows_during_the_pass_is_unknown_not_oversized(tmp_path, monkeypatch):
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    seg = _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    changed = False

    def read_then_append(fd, count):
        nonlocal changed
        chunk = _REAL_READ(fd, count)
        if chunk and not changed:
            changed = True
            with seg.open("ab") as handle:
                handle.write(b"x")
        return chunk

    monkeypatch.setattr(pmv.os, "read", read_then_append)
    _forbid_parser(monkeypatch)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=_footer_len(payload) - 1)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_GREW_DURING_READ" in result.reasons
    assert result.num_rows is None


def test_oversized_footer_file_replaced_during_the_pass_is_unknown_not_oversized(tmp_path, monkeypatch):
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    seg = _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    replaced = False

    def read_then_replace(fd, count):
        nonlocal replaced
        chunk = _REAL_READ(fd, count)
        if chunk and not replaced:
            replaced = True
            other = seg.with_name("swap.tmp")
            other.write_bytes(seg.read_bytes())          # identical bytes, different inode
            os.replace(other, seg)
        return chunk

    monkeypatch.setattr(pmv.os, "read", read_then_replace)
    _forbid_parser(monkeypatch)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=_footer_len(payload) - 1)
    assert result.status is P.UNKNOWN_IO
    assert "SEGMENT_REPLACED_DURING_READ" in result.reasons


def test_oversized_footer_with_a_trailer_that_changes_between_preread_and_pass_fails_closed(
        tmp_path, monkeypatch):
    payload = _parquet(groups=tuple(((f"v{i:03d}", i),) for i in range(50)))
    _put(tmp_path, payload=payload, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    flen = _footer_len(payload)
    # The pre-read reports a different (still oversized, still plausible) trailer than the pass reads.
    lying = struct.pack("<I", flen + 1) + b"PAR1"
    monkeypatch.setattr(pmv.os, "pread", lambda fd, n, off: lying)
    _forbid_parser(monkeypatch)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results,
                                        max_footer_bytes=flen - 1)
    assert result.status is P.UNKNOWN_IO
    assert result.reasons == ("TRAILER_CHANGED_DURING_READ",)
    assert result.byte_evidence_matches_prior is True


def test_footer_exactly_at_the_limit_is_accepted(tmp_path):
    payload = _parquet()
    _put(tmp_path, payload=payload, marker=False)
    _d, _m, _b, (result,) = _run(tmp_path, max_footer_bytes=_footer_len(payload))
    assert result.status is P.VALID_PARQUET


def test_declared_footer_length_beyond_the_file_is_invalid_not_merely_large(tmp_path):
    payload = _parquet()
    bad = payload[:-8] + struct.pack("<I", DEFAULT_MAX_FOOTER_BYTES * 4) + b"PAR1"
    result = _classify(tmp_path, bad)
    assert result.status is P.INVALID_PARQUET
    assert "PARQUET_FOOTER_LENGTH_EXCEEDS_FILE" in result.reasons


@pytest.mark.parametrize("bad", [0, -1, True, MAX_ALLOWED_FOOTER_BYTES + 1, "1024", 1.5])
def test_footer_limit_must_be_a_bounded_integer(tmp_path, bad):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)
    with pytest.raises(ValueError, match="max_footer_bytes"):
        verify_parquet_metadata(tmp_path, discovery, markers, byte_results, max_footer_bytes=bad)


def test_default_limit_covers_the_writers_worst_case_segment_geometry():
    """Evidence for DEFAULT_MAX_FOOTER_BYTES: ParquetWriter rotates at segment_rows=5000 and each
    flush is a row group of >= 1 row, so 5000 row groups is the ceiling; the widest repository schema
    has 26 columns. The real footer for that geometry must fit with margin."""
    from collector.collector import config as cfg

    widest = max((getattr(cfg, n) for n in dir(cfg)
                  if n.endswith("_SCHEMA") and isinstance(getattr(cfg, n), pa.Schema)),
                 key=len)
    assert len(widest) >= 26
    columns = {}
    for field in widest:
        t = field.type
        if pa.types.is_floating(t):
            columns[field.name] = [1.5] * 5000
        elif pa.types.is_integer(t):
            columns[field.name] = list(range(5000)) if t.bit_width >= 32 else [1] * 5000
        elif pa.types.is_boolean(t):
            columns[field.name] = [True] * 5000
        elif pa.types.is_string(t):
            columns[field.name] = ["x"] * 5000
        else:
            columns[field.name] = [None] * 5000
    buf = io.BytesIO()
    writer = pq.ParquetWriter(buf, widest, compression="snappy")
    writer.write_table(pa.Table.from_pydict(columns, schema=widest), row_group_size=1)
    writer.close()
    data = buf.getvalue()
    assert pq.ParquetFile(io.BytesIO(data)).metadata.num_row_groups == 5000
    assert _footer_len(data) < DEFAULT_MAX_FOOTER_BYTES // 2, (
        "default limit must keep >= 2x headroom over the writer's theoretical worst case")


# ----------------------------------------------------------------------------------------------
# forbidden behaviour
# ----------------------------------------------------------------------------------------------

def test_non_candidates_are_not_checked_and_not_opened(tmp_path, monkeypatch):
    seg = _put(tmp_path, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    byte_results = verify_segment_bytes(tmp_path, discovery, markers)
    invalid = replace(discovery.records[0], trust_state=TrustState.INVALID)
    discovery = replace(discovery, records=(invalid,))
    markers = validate_markers(tmp_path, discovery)
    byte_results = verify_segment_bytes(tmp_path, discovery, markers)
    opened = []
    real_open = os.open
    monkeypatch.setattr(pmv.os, "open",
                        lambda p, *a, **k: opened.append(os.fspath(p)) or real_open(p, *a, **k))
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.NOT_CHECKED
    assert result.reasons == ("SOURCE_STATE_INVALID",)
    assert seg.name not in opened


def test_no_scenario_mutates_files_markers_directories_or_trust(tmp_path):
    _put(tmp_path, name=SEG, record_count=99)
    _put(tmp_path, name="2026-10-18-02-000004.seg", payload=_parquet()[:-4] + b"XXXX", marker=False)
    _put(tmp_path, name="2026-10-18-04.parquet", marker=False)   # own hour: same-hour legacy+seg is AMBIGUOUS
    _put(tmp_path, name="2026-10-18-03-000001.seg", payload=b"not parquet at all", marker=False)
    before = _snapshot(tmp_path)
    discovery, _m, _b, results = _run(tmp_path)
    assert {r.status for r in results} >= {P.VALID_PARQUET, P.INVALID_PARQUET}
    assert _snapshot(tmp_path) == before
    assert all(rec.trust_state in (TrustState.UNKNOWN, TrustState.UNCONFIRMED, TrustState.LEGACY)
               for rec in discovery.records)
    assert all(not hasattr(r, "trust_state") for r in results)


# ----------------------------------------------------------------------------------------------
# parser resource failures: classification (the worker's pure functions)
# ----------------------------------------------------------------------------------------------

_BAD_ALLOC = "Couldn't deserialize thrift: std::bad_alloc\n"      # PyArrow 24.0.0, reproduced under RLIMIT_AS


def test_pyarrow_memory_error_is_also_an_arrow_exception_which_is_why_order_matters():
    """Documents the defect: the old handler caught ``(OSError, pa.ArrowException)`` and therefore
    filed ``ArrowMemoryError`` under INVALID_PARQUET."""
    assert issubclass(pa.ArrowMemoryError, pa.ArrowException)
    assert issubclass(pa.ArrowMemoryError, MemoryError)
    assert not issubclass(pa.ArrowMemoryError, OSError)


@pytest.mark.parametrize("exc,code,detail", [
    (pa.ArrowMemoryError("x"), _WORKER.CODE_RESOURCE, _WORKER.RESOURCE_ARROW_MEMORY_ERROR),
    (MemoryError(), _WORKER.CODE_RESOURCE, _WORKER.RESOURCE_MEMORY_ERROR),
    (OSError(_BAD_ALLOC), _WORKER.CODE_RESOURCE, _WORKER.RESOURCE_STD_BAD_ALLOC),
    (OSError(_BAD_ALLOC.rstrip()), _WORKER.CODE_RESOURCE, _WORKER.RESOURCE_STD_BAD_ALLOC),
    # malformed-content evidence stays malformed-content evidence
    (OSError("Couldn't deserialize thrift: TProtocolException: Invalid data\n"),
     _WORKER.CODE_INVALID_PARSE, 0),
    (OSError(errno.EIO, "x"), _WORKER.CODE_INVALID_PARSE, 0),
    (pa.ArrowInvalid("x"), _WORKER.CODE_INVALID_PARSE, 0),
    (pa.ArrowException("x"), _WORKER.CODE_INVALID_PARSE, 0),
    # the signature is matched exactly: text merely embedded in a longer message is not a resource claim
    (OSError("Couldn't deserialize thrift: std::bad_alloc in column name"), _WORKER.CODE_INVALID_PARSE, 0),
    (OSError("Column 'std::bad_alloc' is invalid"), _WORKER.CODE_INVALID_PARSE, 0),
    # unsupported feature
    (pa.ArrowNotImplementedError("x"), _WORKER.CODE_UNSUPPORTED, 0),
])
def test_worker_classifies_parser_exceptions(exc, code, detail):
    outcome = _WORKER.classify_exception(exc)
    assert (outcome.code, outcome.detail) == (code, detail)


@pytest.mark.parametrize("exc", [RuntimeError("x"), ValueError("x"), KeyError("x"), AssertionError("x"),
                                 TypeError("x"), AttributeError("x")])
def test_worker_classifies_anything_else_as_unexpected_with_only_the_type_name(exc):
    outcome = _WORKER.classify_exception(exc)
    assert outcome.code == _WORKER.CODE_UNEXPECTED
    assert outcome.name == type(exc).__name__


def test_worker_parse_footer_maps_each_injected_failure_to_the_documented_code(monkeypatch):
    payload = _parquet()
    flen = _footer_len(payload)
    window = payload[-(flen + 8):]

    def raising(exc):
        def fake(*_a, **_k):
            raise exc
        return fake

    expected = {
        "arrow": (pa.ArrowMemoryError("x"), _WORKER.CODE_RESOURCE),
        "py": (MemoryError(), _WORKER.CODE_RESOURCE),
        "alloc": (OSError(_BAD_ALLOC), _WORKER.CODE_RESOURCE),
        "bad": (OSError("Couldn't deserialize thrift: TProtocolException: Invalid data\n"),
                _WORKER.CODE_INVALID_PARSE),
        "feature": (pa.ArrowNotImplementedError("x"), _WORKER.CODE_UNSUPPORTED),
        "bug": (RuntimeError("x"), _WORKER.CODE_UNEXPECTED),
    }
    for key, (exc, code) in expected.items():
        monkeypatch.setattr(_WORKER.pq, "ParquetFile", raising(exc))
        outcome = _WORKER.parse_footer(window, len(payload), flen)
        assert outcome.code == code, key
        assert (outcome.num_rows, outcome.num_row_groups, outcome.num_columns) == (0, 0, 0)


# ----------------------------------------------------------------------------------------------
# parser resource failures: end to end through the real process boundary
# (the PyArrow call is simulated; the worker loop, protocol, classification and parent are real)
# ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("expression,kind", [
    ('pa.ArrowMemoryError("injected")', "PARSER_RESOURCE_ARROW_MEMORY_ERROR"),
    ("MemoryError()", "PARSER_RESOURCE_MEMORY_ERROR"),
    (f"OSError({_BAD_ALLOC!r})", "PARSER_RESOURCE_STD_BAD_ALLOC"),
])
def test_recognised_resource_failures_are_unknown_resource_never_invalid_parquet(
        tmp_path, injected_parse, expression, kind):
    injected_parse(_raises(expression))
    payload, result = _verify_one(tmp_path)

    assert result.status is P.UNKNOWN_RESOURCE
    assert result.status is not P.INVALID_PARQUET, "resource failure is not evidence of corruption"
    assert result.reasons == ("PARQUET_PARSER_RESOURCE_EXHAUSTED", kind)
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    _assert_fixed_diagnostics(result, tmp_path)


def test_a_footer_that_merely_mentions_bad_alloc_in_its_error_is_still_invalid(tmp_path, injected_parse):
    injected_parse(_raises('OSError("Couldn\'t deserialize thrift: std::bad_alloc in a column name")'))
    _payload, result = _verify_one(tmp_path)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_FOOTER_PARSE_FAILED",)


def test_resource_failure_does_not_disturb_marker_byte_comparisons_or_invent_count_matches(
        tmp_path, injected_parse):
    injected_parse(_raises("MemoryError()"))
    payload = _parquet()
    _put(tmp_path, payload=payload, record_count=99, sha256="f" * 64)
    _d, _m, _b, (result,) = _run(tmp_path)
    assert result.status is P.UNKNOWN_RESOURCE
    assert result.declared_sha256_matches is False and result.declared_size_matches is True
    assert "DECLARED_SHA256_MISMATCH" in result.reasons
    _assert_no_parse_facts(result)


# ----------------------------------------------------------------------------------------------
# parser process containment: crashes, hangs and malformed replies (stand-in workers, real processes)
# ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("code,detail", [
    ("read_request(); os.abort()", "PARSER_WORKER_SIGNAL_SIGABRT"),            # the reported native abort
    ("read_request(); os.kill(os.getpid(), signal.SIGKILL)", "PARSER_WORKER_SIGNAL_SIGKILL"),
    ("read_request(); os.kill(os.getpid(), signal.SIGSEGV)", "PARSER_WORKER_SIGNAL_SIGSEGV"),
    ("read_request(); os._exit(7)", "PARSER_WORKER_EXIT_CODE_7"),
    ("read_request(); sys.exit(0)", "PARSER_WORKER_NO_REPLY"),
    ("sys.exit(0)", "PARSER_WORKER_NO_REPLY"),                                   # dies before reading
    ("read_request(); os.write(1, b'PQR1'); os._exit(5)", "PARSER_WORKER_EXIT_CODE_5"),   # truncated reply
])
def test_abnormal_worker_termination_cannot_crash_the_verifier_or_produce_parse_facts(
        tmp_path, scripted_worker, code, detail):
    scripted_worker(code)
    payload, result = _verify_one(tmp_path)

    assert result.status is P.UNKNOWN_PARSER_FAILURE
    assert result.status is not P.INVALID_PARQUET and result.status is not P.VALID_PARQUET
    assert result.reasons == ("PARQUET_PARSER_WORKER_FAILED", detail)
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    _assert_fixed_diagnostics(result, tmp_path)


def test_a_hung_worker_times_out_is_killed_and_yields_no_parse_facts(tmp_path, scripted_worker, monkeypatch):
    pidfile = tmp_path.parent / (tmp_path.name + "-worker.pid")
    scripted_worker(f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\nread_request()\ntime.sleep(3600)\n")
    monkeypatch.setattr(pmv, "_parse_timeout", lambda _footer_len: 0.5)
    started = time.monotonic()
    payload, result = _verify_one(tmp_path)

    assert time.monotonic() - started < 15, "the verifier must not wait on a hung parser"
    assert result.status is P.UNKNOWN_PARSER_FAILURE
    assert result.reasons == ("PARQUET_PARSER_WORKER_FAILED", "PARSER_WORKER_TIMED_OUT")
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    with pytest.raises(ProcessLookupError):                  # killed and reaped, not leaked
        os.kill(int(pidfile.read_text()), 0)


def test_a_worker_that_never_reads_its_input_cannot_block_the_parent(tmp_path, scripted_worker, monkeypatch):
    """The footer is larger than a pipe buffer, so a blocking write would hang forever."""
    scripted_worker("time.sleep(3600)\n")
    monkeypatch.setattr(pmv, "_parse_timeout", lambda _footer_len: 0.5)
    payload = _parquet(groups=tuple(((f"v{i:04d}", i),) for i in range(1500)))
    assert _footer_len(payload) > 200_000
    started = time.monotonic()
    _payload, result = _verify_one(tmp_path, payload=payload, record_count=1500)
    assert time.monotonic() - started < 15
    assert result.status is P.UNKNOWN_PARSER_FAILURE
    assert result.reasons[-1] == "PARSER_WORKER_TIMED_OUT"
    _assert_no_parse_facts(result)


@pytest.mark.parametrize("code", [
    "read_request(); send(1, counts=(3, 2, 2), magic=b'XXXX')",                       # bad magic
    "read_request(); send(99)",                                                          # unknown code
    "read_request(); send(1, counts=(3, 2, 2), reserved=1)",
    "read_request(); send(1, detail=1, counts=(3, 2, 2))",                              # stray field on VALID
    "read_request(); send(1, problems=1, counts=(3, 2, 2))",
    "read_request(); send(2, counts=(1, 1, 1))",                                         # counts on a non-VALID reply
    "read_request(); send(2, name=b'Oops')",                                             # name on a non-UNEXPECTED reply
    "read_request(); send(3, problems=0)",                                               # structure error without a problem
    "read_request(); send(3, problems=8)",                                               # unknown problem bit
    "read_request(); send(5, detail=0)",                                                 # resource without a kind
    "read_request(); send(5, detail=42)",                                                # unknown resource kind
    "read_request(); os.write(1, b'SECRET-/abs/path ' * 4); time.sleep(3600)",          # garbage, then hang
    "read_request(); os.write(1, b'Z' * (1 << 20)); time.sleep(3600)",                  # output flood
])
def test_an_untrustworthy_worker_reply_is_a_parser_failure_never_a_verdict(
        tmp_path, scripted_worker, code):
    scripted_worker(code)
    payload, result = _verify_one(tmp_path)
    assert result.status is P.UNKNOWN_PARSER_FAILURE
    assert result.reasons == ("PARQUET_PARSER_WORKER_FAILED", "PARSER_WORKER_PROTOCOL_ERROR")
    _assert_no_parse_facts(result)
    _assert_byte_facts_kept(result, payload)
    _assert_fixed_diagnostics(result, tmp_path, "SECRET", "abs/path")


@pytest.mark.parametrize("reply,status,reasons,facts", [
    ("send(1, counts=(3, 2, 2))", P.VALID_PARQUET, ("PARQUET_METADATA_VALID",), (3, 2, 2)),
    ("send(2)", P.INVALID_PARQUET, ("PARQUET_FOOTER_PARSE_FAILED",), None),
    ("send(3, problems=1)", P.INVALID_PARQUET, ("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH",), None),
    ("send(3, problems=2)", P.INVALID_PARQUET, ("PARQUET_COLUMN_CHUNK_OUT_OF_BOUNDS",), None),
    ("send(3, problems=3)", P.INVALID_PARQUET,
     ("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH", "PARQUET_COLUMN_CHUNK_OUT_OF_BOUNDS"), None),
    ("send(4)", P.UNSUPPORTED_METADATA, ("PARQUET_METADATA_UNSUPPORTED",), None),
    ("send(5, detail=1)", P.UNKNOWN_RESOURCE,
     ("PARQUET_PARSER_RESOURCE_EXHAUSTED", "PARSER_RESOURCE_ARROW_MEMORY_ERROR"), None),
    ("send(5, detail=2)", P.UNKNOWN_RESOURCE,
     ("PARQUET_PARSER_RESOURCE_EXHAUSTED", "PARSER_RESOURCE_MEMORY_ERROR"), None),
    ("send(5, detail=3)", P.UNKNOWN_RESOURCE,
     ("PARQUET_PARSER_RESOURCE_EXHAUSTED", "PARSER_RESOURCE_STD_BAD_ALLOC"), None),
])
def test_each_well_formed_worker_reply_maps_to_exactly_one_documented_outcome(
        tmp_path, scripted_worker, reply, status, reasons, facts):
    scripted_worker("read_request()\n" + reply + "\n")
    _payload, result = _verify_one(tmp_path)
    assert result.status is status
    assert result.reasons[: len(reasons)] == reasons
    got = (result.num_rows, result.num_row_groups, result.num_columns)
    assert got == (facts if facts is not None else (None, None, None))
    if status is not P.VALID_PARQUET:
        _assert_no_parse_facts(result)


def test_a_worker_reported_programming_error_is_raised_with_only_its_sanitised_type_name(
        tmp_path, scripted_worker):
    scripted_worker("read_request(); send(6, name=b'../etc; Runtime-Error')")
    _put(tmp_path)
    discovery, markers, byte_results = _stages(tmp_path)
    with pytest.raises(pmv.ParquetParserUnexpectedError) as caught:
        verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert str(caught.value) == "parquet footer worker raised unexpected etcRuntimeError"


def test_worker_start_failure_fails_closed_and_never_falls_back_to_in_process_parsing(
        tmp_path, monkeypatch):
    def forbidden(*_a, **_k):
        pytest.fail("no in-process parsing fallback is allowed")

    monkeypatch.setattr(pq, "ParquetFile", forbidden)
    monkeypatch.setattr(_WORKER, "parse_footer", forbidden)
    for argv, detail in [([str(tmp_path / "no-such-python"), "-I", "x"], ("PARSER_WORKER_START_FAILED", "ERRNO_ENOENT")),
                         (None, ("PARSER_WORKER_START_FAILED",))]:
        monkeypatch.setattr(pmv, "_worker_argv", lambda argv=argv: argv)
        folder = tmp_path / ("run" + str(len(detail)))
        payload, result = _verify_one(folder)
        assert result.status is P.UNKNOWN_PARSER_FAILURE
        assert result.reasons == ("PARQUET_PARSER_WORKER_FAILED", *detail)
        _assert_no_parse_facts(result)
        _assert_byte_facts_kept(result, payload)


def test_the_verifier_itself_never_parses_a_footer_in_process(tmp_path, monkeypatch):
    """The parent process cannot parse: PyArrow's parser is forbidden here, yet files still verify,
    because the parse happens in the worker."""
    def forbidden(*_a, **_k):
        pytest.fail("the verifier process must never call the native footer parser")

    monkeypatch.setattr(pq, "ParquetFile", forbidden)
    monkeypatch.setattr(_WORKER, "parse_footer", forbidden)
    payload, result = _verify_one(tmp_path)
    assert result.status is P.VALID_PARQUET
    assert (result.num_rows, result.num_row_groups, result.num_columns) == (3, 2, 2)


def test_the_worker_is_started_without_footer_bytes_or_paths_on_its_command_line(tmp_path, monkeypatch):
    launched = []
    real_popen = subprocess.Popen

    def spy(argv, *args, **kwargs):
        launched.append((list(argv), kwargs))
        return real_popen(argv, *args, **kwargs)

    monkeypatch.setattr(pmv.subprocess, "Popen", spy)
    _verify_one(tmp_path)
    assert len(launched) == 1
    argv, kwargs = launched[0]
    assert argv == [sys.executable, "-I", WORKER_FILE]
    assert str(tmp_path) not in " ".join(argv)
    assert kwargs["stdin"] is subprocess.PIPE and kwargs["stdout"] is subprocess.PIPE
    assert kwargs["stderr"] is subprocess.DEVNULL, "process output is never captured"
    assert kwargs["close_fds"] is True


_SPAWN_LOG = '''
open(LOG, "a").write(str(os.getpid()) + "\\n")
while True:
    read_request()
    %s
'''


@pytest.mark.parametrize("reply,reuses", [
    ("send(1, counts=(3, 2, 2))", True),        # clean verdicts keep the worker
    ("send(2)", True),
    ("send(4)", True),
    ("send(5, detail=2)", False),               # a resource failure retires it
    ("os.abort()", False),                      # a crash retires it
])
def test_the_worker_is_reused_after_clean_verdicts_and_replaced_after_any_failure(
        tmp_path, scripted_worker, reply, reuses):
    log = tmp_path.parent / (tmp_path.name + "-spawns.log")
    scripted_worker("LOG = %r\n" % str(log) + _SPAWN_LOG % reply)
    for index in (1, 2, 3):
        _put(tmp_path, name=f"2026-10-18-02-00000{index}.seg")
    _d, _m, _b, results = _run(tmp_path)
    assert len(results) == 3
    spawns = len(log.read_text().split())
    assert spawns == (1 if reuses else 3)


def test_a_crash_on_one_file_does_not_affect_the_files_after_it(tmp_path, scripted_worker):
    """One verify call, three files; the worker dies on the second. The third is verified normally by a
    fresh worker and the crash left no state behind."""
    log = tmp_path.parent / (tmp_path.name + "-spawns.log")
    scripted_worker('''
        LOG = %r
        open(LOG, "a").write(str(os.getpid()) + "\\n")
        count = len(open(LOG).read().split())
        read_request()
        if count == 1:
            os.abort()
        send(1, counts=(3, 2, 2))
        read_request()
        send(1, counts=(3, 2, 2))
    ''' % str(log))
    for index in (1, 2, 3):
        _put(tmp_path, name=f"2026-10-18-02-00000{index}.seg", record_count=3)
    _d, _m, _b, results = _run(tmp_path)
    assert [r.status for r in results] == [P.UNKNOWN_PARSER_FAILURE, P.VALID_PARQUET, P.VALID_PARQUET]
    _assert_no_parse_facts(results[0])
    assert results[0].reasons[1] == "PARSER_WORKER_SIGNAL_SIGABRT"
    assert len(set(log.read_text().split())) == 2, "exactly one replacement worker was started"


# ----------------------------------------------------------------------------------------------
# the production worker itself: protocol bounds and address-space limit (real processes)
# ----------------------------------------------------------------------------------------------

def _spawn_production_worker():
    return subprocess.Popen([sys.executable, "-I", WORKER_FILE], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)


def test_protocol_constants_and_bounds_are_consistent_between_parent_and_worker():
    assert _WORKER.MAX_FOOTER_BYTES == MAX_ALLOWED_FOOTER_BYTES
    assert _WORKER.REPLY.size == 64, "replies are fixed-size: no free text can ride along"
    assert _WORKER.validate_header(_WORKER.REQUEST_MAGIC, MAX_ALLOWED_FOOTER_BYTES,
                                   MAX_ALLOWED_FOOTER_BYTES + 12,
                                   pmv._address_space_budget(MAX_ALLOWED_FOOTER_BYTES))


@pytest.mark.parametrize("header", [
    (b"XXXX", 100, 1000, 0),                                              # bad magic
    (b"PQW1", 0, 1000, 0),                                                # empty footer
    (b"PQW1", MAX_ALLOWED_FOOTER_BYTES + 1, 1 << 40, 0),                  # beyond the hard footer ceiling
    (b"PQW1", 100, 100, 0),                                               # footer cannot fit the file
    (b"PQW1", 100, 1000, 1 << 41),                                        # implausible budget
])
def test_production_worker_rejects_out_of_bounds_requests_without_reading_a_body(header):
    proc = _spawn_production_worker()
    try:
        proc.stdin.write(struct.pack("<4sQQQ", *header))
        proc.stdin.flush()
        assert proc.wait(timeout=60) == _WORKER.EXIT_PROTOCOL_ERROR
        assert proc.stdout.read() == b""
    finally:
        proc.kill()
        proc.wait()
        proc.stdin.close()
        proc.stdout.close()


def test_production_worker_treats_a_truncated_request_as_a_protocol_error_and_eof_as_clean_exit():
    proc = _spawn_production_worker()
    proc.stdin.write(struct.pack("<4sQQQ", b"PQW1", 100, 1000, 1 << 20) + b"short")
    proc.stdin.close()
    assert proc.wait(timeout=60) == _WORKER.EXIT_PROTOCOL_ERROR
    proc.stdout.close()

    proc = _spawn_production_worker()
    proc.stdin.close()
    assert proc.wait(timeout=60) == 0
    proc.stdout.close()


def test_production_worker_round_trip_returns_exactly_one_fixed_size_reply():
    payload = _parquet()
    flen = _footer_len(payload)
    proc = _spawn_production_worker()
    try:
        proc.stdin.write(struct.pack("<4sQQQ", b"PQW1", flen, len(payload), 256 << 20)
                         + payload[-(flen + 8):])
        proc.stdin.flush()
        reply = _WORKER.REPLY.unpack(proc.stdout.read(_WORKER.REPLY.size))
        proc.stdin.close()
        assert proc.stdout.read() == b"", "nothing but the one reply was written"
        assert proc.wait(timeout=60) == 0
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()
    magic, code, detail, problems, _reserved, rows, groups, columns, name = reply
    assert (magic, code, detail, problems, name.rstrip(b"\0")) == (b"PQR1", _WORKER.CODE_VALID, 0, 0, b"")
    assert (rows, groups, columns) == (3, 2, 2)


_LIMITER_PROBE = '''
import importlib.util, sys
spec = importlib.util.spec_from_file_location("production_worker", %r)
mod = importlib.util.module_from_spec(spec); sys.modules["production_worker"] = mod
spec.loader.exec_module(mod)
limiter = mod._AddressSpaceLimiter()
assert limiter.apply(32 << 20) is True
try:
    bytearray(512 << 20)
    print("NOT LIMITED")
except MemoryError:
    print("limited")
limiter.restore()
block = bytearray(64 << 20)
print("restored")
'''


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="address-space limit is applied on Linux")
def test_address_space_limit_is_applied_and_restored_in_a_disposable_process():
    done = subprocess.run([sys.executable, "-I", "-c", _LIMITER_PROBE % WORKER_FILE],
                          capture_output=True, text=True, timeout=120)
    assert done.stdout.split() == ["limited", "restored"], (done.stdout, done.stderr[-300:])


# ----------------------------------------------------------------------------------------------
# resource bounds
# ----------------------------------------------------------------------------------------------

def test_budget_and_timeout_are_finite_and_bounded_at_the_maximum_configured_footer():
    biggest = MAX_ALLOWED_FOOTER_BYTES
    assert pmv._address_space_budget(biggest) == 16 * biggest + 64 * 1024 * 1024
    assert pmv._address_space_budget(biggest) < 5 * 1024 ** 3
    assert pmv._address_space_budget(1) >= 64 * 1024 * 1024
    assert 0 < pmv._parse_timeout(1) < pmv._parse_timeout(biggest) <= 120


def test_a_maximum_configured_footer_limit_does_not_enlarge_what_is_captured(tmp_path, monkeypatch):
    payload = _parquet()
    sizes = _track_buffer_sizes(monkeypatch)
    _put(tmp_path, payload=payload, marker=False)
    _d, _m, _b, (result,) = _run(tmp_path, max_footer_bytes=MAX_ALLOWED_FOOTER_BYTES)
    assert result.status is P.VALID_PARQUET
    assert max(sizes) == pmv._TRAILER_BYTES + _footer_len(payload), "capture is sized by the declared footer"


# ----------------------------------------------------------------------------------------------
# the reported scenario: a valid file with a very large footer under a constrained address space
# ----------------------------------------------------------------------------------------------

@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="address-space limit is applied on Linux")
def test_real_worker_with_a_40k_row_group_footer_under_constrained_memory(tmp_path, monkeypatch):
    """REAL native parser, REAL address-space limit, real subprocess; nothing simulated.

    Measured on PyArrow 24.0.0 / Linux, parsing this footer needs ~76 MiB of address space beyond
    the worker's baseline. With less, the native parser either raises ``OSError(...std::bad_alloc)`` or
    aborts the process (SIGABRT); which one depends on where the allocation fails and is NOT asserted.
    What must hold for every budget is the invariant below, and that is what this test checks.
    """
    payload = _many_row_groups(40_000)
    flen = _footer_len(payload)
    assert 7 * 1024 * 1024 < flen < 9 * 1024 * 1024, "the ~7.8 MiB footer of the reported scenario"
    _put(tmp_path, payload=payload, record_count=40_000)
    discovery, markers, byte_results = _stages(tmp_path)
    production_budget = pmv._address_space_budget

    seen = {}
    for budget_mib in (0, 1, 4, 16, 32, 64):
        monkeypatch.setattr(pmv, "_address_space_budget", lambda _l, mib=budget_mib: mib * 1024 * 1024)
        (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
        seen[budget_mib] = (result.status.value, result.reasons[-1])
        assert result.status is not P.INVALID_PARQUET, f"resource failure mislabelled corrupt: {seen}"
        assert result.status in (P.UNKNOWN_RESOURCE, P.UNKNOWN_PARSER_FAILURE, P.VALID_PARQUET), seen
        if result.status is P.VALID_PARQUET:
            assert (result.num_rows, result.num_row_groups, result.num_columns) == (40_000, 40_000, 2)
        else:
            _assert_no_parse_facts(result)
            _assert_byte_facts_kept(result, payload)
            _assert_fixed_diagnostics(result, tmp_path)
    assert any(status != "VALID_PARQUET" for status, _ in seen.values()), (
        "the address-space limit never took effect, so containment was not exercised: " + repr(seen))

    # With the production budget the same real file is VALID: the limit is generous enough for it.
    monkeypatch.setattr(pmv, "_address_space_budget", production_budget)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.VALID_PARQUET
    assert (result.num_rows, result.num_row_groups) == (40_000, 40_000)
    assert result.declared_record_count_matches is True


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="address-space limit is applied on Linux")
def test_a_real_resource_failure_on_one_file_leaves_the_neighbouring_files_verified(tmp_path, monkeypatch):
    big = _many_row_groups(20_000)
    _put(tmp_path, name="2026-10-18-02-000001.seg", record_count=3)
    _put(tmp_path, name="2026-10-18-02-000002.seg", payload=big, record_count=20_000)
    _put(tmp_path, name="2026-10-18-02-000003.seg", record_count=3)
    production_budget = pmv._address_space_budget
    monkeypatch.setattr(pmv, "_address_space_budget",
                        lambda footer_len: 1 << 20 if footer_len > 1_000_000 else production_budget(footer_len))
    _d, _m, _b, results = _run(tmp_path)
    assert results[0].status is P.VALID_PARQUET
    assert results[1].status in (P.UNKNOWN_RESOURCE, P.UNKNOWN_PARSER_FAILURE)
    assert results[1].status is not P.INVALID_PARQUET
    _assert_no_parse_facts(results[1])
    assert results[2].status is P.VALID_PARQUET and results[2].num_rows == 3


# ----------------------------------------------------------------------------------------------
# worker retirement, limit restoration and protocol hygiene (gaps found by mutation testing)
# ----------------------------------------------------------------------------------------------

_FIRST_SPAWN_MISBEHAVES = '''
LOG = %r
open(LOG, "a").write(str(os.getpid()) + "\\n")
first = len(open(LOG).read().split()) == 1
while True:
    read_request()
    if first:
        %s
    send(1, counts=(3, 2, 2))
'''


@pytest.mark.parametrize("misbehaviour,detail", [
    ("time.sleep(3600)", "PARSER_WORKER_TIMED_OUT"),
    ("os.write(1, b'garbage-not-a-reply' * 4); time.sleep(3600)", "PARSER_WORKER_PROTOCOL_ERROR"),
])
def test_a_hung_or_lying_worker_is_retired_before_the_next_file_is_verified(
        tmp_path, scripted_worker, monkeypatch, misbehaviour, detail):
    """If the bad worker were kept, the second file would be sent to it and fail too."""
    log = tmp_path.parent / (tmp_path.name + "-spawns.log")
    scripted_worker(_FIRST_SPAWN_MISBEHAVES % (str(log), misbehaviour))
    monkeypatch.setattr(pmv, "_parse_timeout", lambda _footer_len: 1.0)
    for index in (1, 2):
        _put(tmp_path, name=f"2026-10-18-02-00000{index}.seg", record_count=3)
    _d, _m, _b, results = _run(tmp_path)
    assert [r.status for r in results] == [P.UNKNOWN_PARSER_FAILURE, P.VALID_PARQUET]
    assert results[0].reasons[-1] == detail
    assert len(set(log.read_text().split())) == 2


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="address-space limit is applied on Linux")
def test_the_address_space_limit_of_one_request_does_not_leak_into_the_next(tmp_path, monkeypatch):
    """The first (small) footer is parsed under a deliberately tiny budget; the second needs far more.
    If the worker kept the first request's limit it would fail while reading the second request."""
    _put(tmp_path, name="2026-10-18-02-000001.seg", record_count=3)
    _put(tmp_path, name="2026-10-18-02-000002.seg", payload=_many_row_groups(20_000), record_count=20_000)
    production_budget = pmv._address_space_budget
    monkeypatch.setattr(pmv, "_address_space_budget",
                        lambda footer_len: 1 << 20 if footer_len < 10_000 else production_budget(footer_len))
    _d, _m, _b, results = _run(tmp_path)
    assert [r.status for r in results] == [P.VALID_PARQUET, P.VALID_PARQUET]
    assert results[1].num_row_groups == 20_000


def test_stray_writes_to_the_workers_stdout_cannot_corrupt_the_protocol(tmp_path, injected_parse):
    """Native code that printed to fd 1 would otherwise be read by the parent as part of a reply."""
    injected_parse('''
        import os
        _real = mod.pq.ParquetFile

        def fake(*a, **k):
            os.write(1, b"NOISE FROM NATIVE CODE " * 8)
            return _real(*a, **k)
    ''')
    _payload, result = _verify_one(tmp_path)
    assert result.status is P.VALID_PARQUET
    assert (result.num_rows, result.num_row_groups) == (3, 2)
