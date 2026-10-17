
"""Adversarial tests for read-only segment byte verification."""
import errno
import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from collector.collector import publication
from collector.collector import segment_validation as sv
from collector.collector.marker_validation import (
    DeclaredMarkerEvidence,
    MarkerValidationStatus,
    validate_markers,
)
from collector.collector.segment_validation import (
    SegmentVerificationStatus as S,
    verify_segment_bytes,
)
from collector.collector.source_catalog import TrustState, discover_sources

SEG = "2026-10-18-02-000003.seg"
LEGACY = "2026-10-18-02.parquet"
_REAL_READ = os.read


def _marker_doc(name: str, payload: bytes, *, size_bytes=None, sha256=None):
    return publication.build_marker(
        name,
        record_count=2,
        first_ts=1792288800000,
        last_ts=1792288800100,
        sha256=sha256 if sha256 is not None else hashlib.sha256(payload).hexdigest(),
        size_bytes=size_bytes if size_bytes is not None else len(payload),
        confirmed_by="writer",
        legacy=False,
        boot_id="test-boot",
        confirmed_at_utc="2026-10-18T02:00:00Z",
    )


def _put(root: Path, *, name=SEG, payload=b"stable segment payload", marker=True,
         declared_size=None, declared_sha=None, stream="trades"):
    folder = root / "raw" / stream
    folder.mkdir(parents=True, exist_ok=True)
    segment = folder / name
    segment.write_bytes(payload)
    if marker:
        doc = _marker_doc(
            name, payload, size_bytes=declared_size, sha256=declared_sha
        )
        (folder / (name + ".meta.json")).write_bytes(
            json.dumps(doc, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
    return segment


def _run(root: Path):
    discovery = discover_sources(root)
    markers = validate_markers(root, discovery)
    results = verify_segment_bytes(root, discovery, markers)
    return discovery, markers, results


def _snapshot(root: Path):
    out = {}
    for directory, subdirs, names in os.walk(root, followlinks=False):
        for name in sorted(subdirs + names):
            path = Path(directory) / name
            st = path.lstat()
            content = (
                hashlib.sha256(path.read_bytes()).hexdigest()
                if stat.S_ISREG(st.st_mode) else None
            )
            link = os.readlink(path) if stat.S_ISLNK(st.st_mode) else None
            out[str(path.relative_to(root))] = (st.st_mode, st.st_size, st.st_mtime_ns, content, link)
    return out


def test_correct_marker_is_compared_to_real_segment_bytes_without_promoting_trust(tmp_path):
    payload = b"BTC raw bytes: preserve exactly\x00\x01"
    _put(tmp_path, payload=payload)
    before = _snapshot(tmp_path)
    discovery, markers, (result,) = _run(tmp_path)

    assert markers[0].status is MarkerValidationStatus.VALID_V2
    assert result.status is S.HASHED_STABLE
    assert result.observed_size == len(payload)
    assert result.bytes_hashed == len(payload)
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.discovery_size_matches is True
    assert result.declared_size_matches is True
    assert result.declared_sha256_matches is True
    assert result.reasons == ("SEGMENT_BYTES_STABLE_AND_HASHED",)
    assert discovery.records[0].trust_state is TrustState.UNKNOWN
    assert _snapshot(tmp_path) == before, "verification must not modify data or metadata"


@pytest.mark.parametrize(
    "declared_size,declared_sha,expected_reason,field",
    [
        (999, None, "DECLARED_SIZE_MISMATCH", "declared_size_matches"),
        (None, "f" * 64, "DECLARED_SHA256_MISMATCH", "declared_sha256_matches"),
    ],
)
def test_declared_marker_mismatch_is_reported_but_not_hidden_in_overall_status(
    tmp_path, declared_size, declared_sha, expected_reason, field
):
    payload = b"actual source bytes"
    _put(tmp_path, payload=payload, declared_size=declared_size, declared_sha=declared_sha)
    discovery, _markers, (result,) = _run(tmp_path)

    assert result.status is S.HASHED_STABLE
    assert getattr(result, field) is False
    assert expected_reason in result.reasons
    assert discovery.records[0].trust_state is TrustState.UNKNOWN


def test_unmarked_segment_is_hashed_but_never_given_inferred_marker_comparisons(tmp_path):
    payload = b"published segment with no marker"
    _put(tmp_path, payload=payload, marker=False)
    discovery, markers, (result,) = _run(tmp_path)

    assert discovery.records[0].trust_state is TrustState.UNCONFIRMED
    assert markers[0].status is MarkerValidationStatus.NOT_CHECKED
    assert result.status is S.HASHED_STABLE
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.declared_size_matches is None
    assert result.declared_sha256_matches is None
    assert discovery.records[0].trust_state is TrustState.UNCONFIRMED


def test_legacy_hourly_parquet_is_hashed_without_applying_segment_marker_semantics(tmp_path):
    payload = b"legacy content, not a schema-2 segment"
    _put(tmp_path, name=LEGACY, payload=payload, marker=False)
    discovery, markers, (result,) = _run(tmp_path)

    assert discovery.records[0].trust_state is TrustState.LEGACY
    assert markers[0].status is MarkerValidationStatus.NOT_APPLICABLE
    assert result.status is S.HASHED_STABLE
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.declared_size_matches is None
    assert result.declared_sha256_matches is None
    assert discovery.records[0].trust_state is TrustState.LEGACY


def test_size_changed_after_discovery_is_explicit_even_if_the_current_read_is_stable(tmp_path):
    payload = b"before discovery"
    segment = _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    with segment.open("ab") as handle:
        handle.write(b" appended after catalog")
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)

    assert result.status is S.HASHED_STABLE
    assert result.discovery_size_matches is False
    assert "DISCOVERY_SIZE_CHANGED" in result.reasons
    assert result.observed_sha256 == hashlib.sha256(segment.read_bytes()).hexdigest()
    assert result.declared_size_matches is None


def test_a_segment_that_grows_while_being_hashed_is_unknown_not_partially_verified(tmp_path, monkeypatch):
    segment = _put(tmp_path, payload=b"small initial payload", marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    changed = False

    def read_then_append(fd, count):
        nonlocal changed
        chunk = _REAL_READ(fd, count)
        if chunk and not changed:
            changed = True
            with segment.open("ab") as handle:
                handle.write(b" concurrent append")
        return chunk

    monkeypatch.setattr(sv.os, "read", read_then_append)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)

    assert result.status is S.UNKNOWN_IO
    assert result.observed_sha256 is None
    assert "SEGMENT_GREW_DURING_READ" in result.reasons
    assert result.bytes_hashed == len(b"small initial payload")


def test_segment_symlink_substitution_is_not_followed(tmp_path):
    segment = _put(tmp_path, payload=b"initial", marker=False)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"must not be followed")
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    segment.unlink()
    segment.symlink_to(outside)

    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    assert result.status is S.UNKNOWN_IO
    assert result.observed_sha256 is None
    assert result.reasons == ("SEGMENT_SYMLINK_AT_OPEN",)


def test_read_failure_returns_fixed_diagnostics_and_never_a_partial_digest(tmp_path, monkeypatch):
    _put(tmp_path, payload=b"some bytes", marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)

    def fail_read(_fd, _count):
        raise OSError(errno.EIO, "injected path-bearing error")

    monkeypatch.setattr(sv.os, "read", fail_read)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)

    assert result.status is S.UNKNOWN_IO
    assert result.observed_sha256 is None
    assert result.reasons == ("SEGMENT_READ_FAILED", "ERRNO_EIO")
    assert "injected" not in repr(result)
    assert str(tmp_path) not in repr(result)


def test_marker_results_must_match_the_discovery_order_and_paths(tmp_path):
    _put(tmp_path, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    wrong_path = type(markers[0])(
        relative_path="raw/trades/other.seg",
        marker_presence=markers[0].marker_presence,
        status=markers[0].status,
        reasons=markers[0].reasons,
    )
    with pytest.raises(ValueError, match="match discovery order"):
        verify_segment_bytes(tmp_path, discovery, (wrong_path,))


def test_marker_validation_cannot_supply_schema2_evidence_for_a_legacy_file(tmp_path):
    _put(tmp_path, name=LEGACY, marker=False)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    markers[0] = type(markers[0])(
        relative_path=markers[0].relative_path,
        marker_presence=markers[0].marker_presence,
        status=MarkerValidationStatus.VALID_V2,
        reasons=("test",),
        evidence=DeclaredMarkerEvidence(
            segment=LEGACY, schema=2, state="PUBLICATION_CONFIRMED", confirmed_by="writer",
            legacy=False, size_bytes=1, sha256="0" * 64, record_count=0,
            first_record_ts=None, last_record_ts=None,
        ),
    )
    with pytest.raises(ValueError, match="does not identify"):
        verify_segment_bytes(tmp_path, discovery, markers)



def test_v2_evidence_cannot_be_fabricated_for_an_absent_marker_path(tmp_path):
    _put(tmp_path, marker=False)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    markers[0] = type(markers[0])(
        relative_path=markers[0].relative_path,
        marker_presence=markers[0].marker_presence,
        status=MarkerValidationStatus.VALID_V2,
        reasons=("MARKER_SCHEMA_2_VALID", "SEGMENT_BYTES_NOT_VERIFIED"),
        evidence=DeclaredMarkerEvidence(
            segment=SEG, schema=2, state="PUBLICATION_CONFIRMED", confirmed_by="writer",
            legacy=False, size_bytes=1, sha256="0" * 64, record_count=0,
            first_record_ts=None, last_record_ts=None,
        ),
    )
    with pytest.raises(ValueError, match="inconsistent marker presence"):
        verify_segment_bytes(tmp_path, discovery, markers)


def test_unsupported_safe_open_fails_closed_for_valid_candidates(tmp_path, monkeypatch):
    _put(tmp_path, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    monkeypatch.setattr(sv, "_SAFE_FLAGS", None)

    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    assert result.status is S.UNKNOWN_IO
    assert result.reasons == ("SAFE_OPEN_UNAVAILABLE",)


def test_invalid_symlink_source_is_not_opened(tmp_path, monkeypatch):
    segment = _put(tmp_path, payload=b"initial", marker=False)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"must not be opened through the raw source name")
    segment.unlink()
    segment.symlink_to(outside)

    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    real_open = os.open
    opened_segment_names = []

    def track_open(path, flags, *args, **kwargs):
        if os.fspath(path) == segment.name:
            opened_segment_names.append(os.fspath(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(sv.os, "open", track_open)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)

    assert discovery.records[0].trust_state is TrustState.INVALID
    assert result.status is S.NOT_CHECKED
    assert result.reasons == ("SOURCE_STATE_INVALID",)
    assert opened_segment_names == []
