"""Adversarial tests for read-only Parquet metadata verification.

Positive fixtures are real Parquet written by the pinned PyArrow; negative fixtures are mutated or
hand-built bytes. Every test runs the real pipeline (discovery -> marker validation -> segment byte
verification -> Parquet metadata verification).
"""
import errno
import hashlib
import io
import json
import os
import stat
import struct
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

    _put(tmp_path)
    discovery, markers, byte_results = _stages(tmp_path)

    def forbidden(*_a, **_k):
        raise AssertionError("row data / path-based reads are forbidden in metadata verification")

    for target, name in [
        (pq.ParquetFile, "read"), (pq.ParquetFile, "read_row_group"),
        (pq.ParquetFile, "read_row_groups"), (pq.ParquetFile, "iter_batches"),
        (pq, "read_table"), (pq, "read_metadata"), (pandas, "read_parquet"),
    ]:
        monkeypatch.setattr(target, name, forbidden)
    sources = []
    real_file = pq.ParquetFile

    def spy(source, **kwargs):
        sources.append(source)
        return real_file(source, **kwargs)

    monkeypatch.setattr(pmv.pq, "ParquetFile", spy)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)

    assert result.status is P.VALID_PARQUET
    assert len(sources) == 1 and isinstance(sources[0], pa.BufferReader), (
        "the parser must receive the captured, hashed bytes, never a path or a re-opened file")


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


def test_parser_reported_unsupported_feature_is_not_called_corruption(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    def unsupported(*_a, **_k):
        raise pa.ArrowNotImplementedError("injected feature text " + str(tmp_path))

    monkeypatch.setattr(pmv.pq, "ParquetFile", unsupported)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.UNSUPPORTED_METADATA
    assert result.reasons == ("PARQUET_METADATA_UNSUPPORTED",)
    assert "injected" not in repr(result) and str(tmp_path) not in repr(result)


def test_parser_oserror_on_in_memory_bytes_is_content_evidence(tmp_path, monkeypatch):
    # PyArrow reports thrift failures as OSError; for an in-memory buffer that is not an I/O fault.
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    def raises(*_a, **_k):
        raise OSError("Couldn't deserialize thrift " + str(tmp_path))

    monkeypatch.setattr(pmv.pq, "ParquetFile", raises)
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_FOOTER_PARSE_FAILED",)
    assert str(tmp_path) not in repr(result)


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


def test_footer_whose_row_group_counts_do_not_sum_to_its_total_is_invalid(tmp_path, monkeypatch):
    """A stub stands in for a footer that parses but contradicts itself (PyArrow offers no way to
    write one); the check must reject it rather than report the inconsistent totals."""
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    class _Group:
        num_rows, num_columns = 3, 0

    class _Meta:
        num_rows, num_row_groups, num_columns = 5, 1, 1

        def row_group(self, _i):
            return _Group()

    class _Parsed:
        metadata = _Meta()

    monkeypatch.setattr(pmv.pq, "ParquetFile", lambda *a, **k: _Parsed())
    (result,) = verify_parquet_metadata(tmp_path, discovery, markers, byte_results)
    assert result.status is P.INVALID_PARQUET
    assert result.reasons == ("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH",)
    assert result.num_rows is None


def test_unexpected_exception_types_are_not_swallowed(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery, markers, byte_results = _stages(tmp_path)

    def boom(*_a, **_k):
        raise MemoryError

    monkeypatch.setattr(pmv.pq, "ParquetFile", boom)
    with pytest.raises(MemoryError):
        verify_parquet_metadata(tmp_path, discovery, markers, byte_results)


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
    monkeypatch.setattr(pmv.pq, "ParquetFile",
                        lambda *a, **k: pytest.fail("an oversized footer must never reach the parser"))


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
