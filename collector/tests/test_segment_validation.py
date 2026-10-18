
"""Adversarial tests for read-only segment byte verification."""
import errno
import hashlib
import json
import os
import re
import stat
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

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
from collector.collector.source_catalog import MarkerPresence, TrustState, discover_sources

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


def _legacy_evidence():
    return DeclaredMarkerEvidence(
        segment=LEGACY, schema=2, state="PUBLICATION_CONFIRMED", confirmed_by="writer",
        legacy=False, size_bytes=1, sha256="0" * 64, record_count=0,
        first_record_ts=None, last_record_ts=None,
    )


def test_marker_validation_cannot_supply_schema2_evidence_for_a_legacy_file(tmp_path):
    """Every other marker-consistency check passes here, so the rejection can only come
    from the path-identity check (legacy hourly Parquet never carries segment markers).

    The record's marker presence is set to REGULAR_FILE alongside the marker result's, so the
    discovery-agreement check cannot pre-empt the path-identity check this test isolates; the
    legacy record's real NOT_APPLICABLE presence is covered by the presence-contradiction test."""
    _put(tmp_path, name=LEGACY, marker=False)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    assert discovery.records[0].kind is not sv.SegmentKind.SEGMENT
    discovery = replace(
        discovery,
        records=(replace(discovery.records[0], marker_presence=MarkerPresence.REGULAR_FILE),),
    )
    markers[0] = replace(
        markers[0],
        marker_presence=MarkerPresence.REGULAR_FILE,
        status=MarkerValidationStatus.VALID_V2,
        reasons=("MARKER_SCHEMA_2_VALID", "SEGMENT_BYTES_NOT_VERIFIED"),
        evidence=_legacy_evidence(),
    )
    with pytest.raises(ValueError, match="does not identify"):
        verify_segment_bytes(tmp_path, discovery, markers)


def test_forged_v2_evidence_for_a_legacy_file_is_rejected_even_without_consistent_presence(tmp_path):
    """The original forged shape (no marker presence, placeholder reason) stays rejected."""
    _put(tmp_path, name=LEGACY, marker=False)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    markers[0] = replace(
        markers[0],
        status=MarkerValidationStatus.VALID_V2,
        reasons=("test",),
        evidence=_legacy_evidence(),
    )
    with pytest.raises(ValueError, match="inconsistent marker presence"):
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


# ---------------------------------------------------------------------------
# Declared-evidence contract: inconsistent or fabricated marker results are
# rejected with ValueError before any file is opened (no partial batch).
# ---------------------------------------------------------------------------

SEG2 = "2026-10-18-02-000004.seg"


def _seg_evidence():
    return DeclaredMarkerEvidence(
        segment=SEG, schema=2, state="PUBLICATION_CONFIRMED", confirmed_by="writer",
        legacy=False, size_bytes=1, sha256="0" * 64, record_count=0,
        first_record_ts=None, last_record_ts=None,
    )


def _forge(root, *, marker_overrides=None, **evidence_overrides):
    """A real VALID_V2 result for the first record, with chosen fields replaced."""
    discovery = discover_sources(root)
    markers = list(validate_markers(root, discovery))
    assert markers[0].status is MarkerValidationStatus.VALID_V2
    evidence = replace(markers[0].evidence, **evidence_overrides)
    markers[0] = replace(markers[0], evidence=evidence, **(marker_overrides or {}))
    return discovery, markers


def _fail_if_filesystem_touched(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("evidence must be rejected before any file is opened or read")

    monkeypatch.setattr(sv.os, "open", refuse)
    monkeypatch.setattr(sv.os, "read", refuse)


_BAD_DECLARED_FIELDS = [
    ("schema", 3), ("schema", 2.0), ("schema", True), ("schema", "2"), ("schema", None),
    ("state", "PUBLICATION_PENDING"), ("state", None),
    ("confirmed_by", "attacker"), ("confirmed_by", ["writer"]), ("confirmed_by", None),
    ("legacy", "false"), ("legacy", 0), ("legacy", None),
    ("size_bytes", -1), ("size_bytes", "22"), ("size_bytes", True),
    ("size_bytes", 22.0), ("size_bytes", None),
    ("sha256", None), ("sha256", 12345), ("sha256", "F" * 64), ("sha256", "f" * 63),
    ("sha256", "f" * 64 + "\n"), ("sha256", "z" * 64), ("sha256", "SECRET-NOT-A-DIGEST"),
]


@pytest.mark.parametrize("field,value", _BAD_DECLARED_FIELDS)
def test_malformed_declared_evidence_is_rejected_before_any_file_access(
    tmp_path, monkeypatch, field, value
):
    _put(tmp_path)
    discovery, markers = _forge(tmp_path, **{field: value})
    _fail_if_filesystem_touched(monkeypatch)

    with pytest.raises(ValueError, match=f"declares invalid .*{field}") as excinfo:
        verify_segment_bytes(tmp_path, discovery, markers)

    message = str(excinfo.value)
    assert str(tmp_path) not in message
    assert "SECRET-NOT-A-DIGEST" not in message
    assert "attacker" not in message


def test_startup_refsync_marker_with_legacy_true_is_valid_evidence(tmp_path):
    """legacy=True from startup refsync is a real, legitimate declaration: only its type
    is validated, so genuine markers are never rejected by this module."""
    payload = b"segment confirmed by startup refsync"
    folder = tmp_path / "raw" / "trades"
    folder.mkdir(parents=True)
    (folder / SEG).write_bytes(payload)
    doc = publication.build_marker(
        SEG, record_count=2, first_ts=None, last_ts=None,
        sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload),
        confirmed_by="startup_refsync", legacy=True, boot_id="test-boot",
        confirmed_at_utc="2026-10-18T02:00:00Z",
    )
    (folder / (SEG + ".meta.json")).write_bytes(
        json.dumps(doc, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    )

    _discovery, markers, (result,) = _run(tmp_path)

    assert markers[0].status is MarkerValidationStatus.VALID_V2
    assert markers[0].evidence.confirmed_by == "startup_refsync"
    assert markers[0].evidence.legacy is True
    assert result.status is S.HASHED_STABLE
    assert result.declared_size_matches is True
    assert result.declared_sha256_matches is True


def test_evidence_on_a_result_that_is_not_valid_v2_is_rejected(tmp_path):
    _put(tmp_path, marker=False)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    assert markers[0].status is not MarkerValidationStatus.VALID_V2
    markers[0] = replace(markers[0], evidence=_seg_evidence())
    with pytest.raises(ValueError, match="non-VALID_V2"):
        verify_segment_bytes(tmp_path, discovery, markers)


@pytest.mark.parametrize(
    "reasons",
    ["MARKER_SCHEMA_2_VALID", ["MARKER_SCHEMA_2_VALID"], ("SEGMENT_BYTES_NOT_VERIFIED",), ()],
)
def test_valid_v2_needs_the_schema_validation_reason_as_a_tuple_member(tmp_path, reasons):
    """A bare string would satisfy a substring test, and a list is not what the validator emits."""
    _put(tmp_path)
    discovery, markers = _forge(tmp_path, marker_overrides={"reasons": reasons})
    with pytest.raises(ValueError, match="lacks schema-validation evidence"):
        verify_segment_bytes(tmp_path, discovery, markers)


def test_evidence_of_an_unexpected_type_is_rejected(tmp_path):
    _put(tmp_path)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    lookalike = SimpleNamespace(**vars(markers[0].evidence))
    markers[0] = replace(markers[0], evidence=lookalike)
    with pytest.raises(ValueError, match="unexpected type"):
        verify_segment_bytes(tmp_path, discovery, markers)


def test_evidence_naming_a_different_segment_is_rejected(tmp_path):
    """Every field is otherwise valid, so only the path-identity check can reject this."""
    _put(tmp_path)
    discovery, markers = _forge(tmp_path, segment=SEG2)
    with pytest.raises(ValueError, match="does not identify"):
        verify_segment_bytes(tmp_path, discovery, markers)


def test_one_bad_evidence_item_rejects_the_call_before_any_file_is_read(tmp_path, monkeypatch):
    _put(tmp_path)
    _put(tmp_path, name=SEG2)
    discovery = discover_sources(tmp_path)
    markers = list(validate_markers(tmp_path, discovery))
    assert len(markers) == 2
    assert all(m.status is MarkerValidationStatus.VALID_V2 for m in markers)
    good = tuple(markers)
    markers[1] = replace(markers[1], evidence=replace(markers[1].evidence, sha256="not-a-digest"))

    _fail_if_filesystem_touched(monkeypatch)
    with pytest.raises(ValueError, match="declares invalid .*sha256"):
        verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    # Nothing was hashed and no state was left behind: the same call with sound input works.
    results = verify_segment_bytes(tmp_path, discovery, good)
    assert [r.status for r in results] == [S.HASHED_STABLE, S.HASHED_STABLE]
    assert all(r.declared_sha256_matches is True for r in results)


# ---------------------------------------------------------------------------
# F2: a VALID_V2 marker result must agree with the discovery record it claims to
# describe. These checks establish internal consistency only; a caller-built
# result still cannot be authenticated.
# ---------------------------------------------------------------------------

_FILESYSTEM_CALLS = ("open", "read", "stat", "fstat", "lstat", "scandir", "listdir", "readlink")


@contextmanager
def _recorded_filesystem_calls(monkeypatch):
    """Record, without blocking, every filesystem call made through the os module.

    Recording (rather than refusing) keeps pytest's own failure reporting working while the
    wrappers are installed, and a failing assertion can still show what was touched.
    """
    calls = []
    with monkeypatch.context() as patch:
        for name in _FILESYSTEM_CALLS:
            real = getattr(os, name)

            def wrapper(*args, _name=name, _real=real, **kwargs):
                calls.append(_name)
                return _real(*args, **kwargs)

            patch.setattr(os, name, wrapper)
        yield calls


def _assert_rejected_before_filesystem_access(monkeypatch, root, discovery, markers, match):
    results = None
    error = None
    with _recorded_filesystem_calls(monkeypatch) as calls:
        try:
            results = verify_segment_bytes(root, discovery, markers)
        except ValueError as exc:
            error = exc
    assert error is not None, (
        "inconsistent marker evidence was accepted: the verifier returned "
        f"{len(results)} result(s) after {len(calls)} filesystem call(s)"
    )
    assert re.search(match, str(error)), f"rejected for a different reason: {error}"
    assert results is None, "a rejected call must not return a partial result tuple"
    assert calls == [], f"rejection must precede all filesystem access, saw {calls}"
    assert str(root) not in str(error)
    return error


def test_filesystem_call_recorder_sees_a_real_verification(tmp_path, monkeypatch):
    """Positive control: the recorder is not vacuous, so an empty record really means no access."""
    _put(tmp_path)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)

    with _recorded_filesystem_calls(monkeypatch) as calls:
        (result,) = verify_segment_bytes(tmp_path, discovery, markers)

    assert result.status is S.HASHED_STABLE
    assert "open" in calls and "read" in calls and "fstat" in calls


@pytest.mark.parametrize(
    "record_presence",
    [
        MarkerPresence.ABSENT,
        MarkerPresence.NON_REGULAR,
        MarkerPresence.UNKNOWN,
        MarkerPresence.NOT_APPLICABLE,
    ],
    ids=lambda presence: presence.value,
)
def test_valid_v2_marker_cannot_contradict_the_discovery_records_marker_presence(
    tmp_path, monkeypatch, record_presence
):
    """Both marker results are genuine VALID_V2 / REGULAR_FILE results for genuinely present
    markers, and the first record is untouched. Only the second discovery record is altered to
    say its marker was ABSENT / NON_REGULAR / UNKNOWN. Schema, state, declared fields, evidence
    type, reasons, relative path and path identity are all valid, so the only thing left to
    reject is the disagreement between the marker result and the discovery record."""
    _put(tmp_path)
    _put(tmp_path, name=SEG2)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    assert [r.relative_path for r in discovery.records] == [f"raw/trades/{SEG}", f"raw/trades/{SEG2}"]
    assert [m.status for m in markers] == [MarkerValidationStatus.VALID_V2] * 2
    assert {m.marker_presence for m in markers} == {MarkerPresence.REGULAR_FILE}
    assert {r.marker_presence for r in discovery.records} == {MarkerPresence.REGULAR_FILE}

    records = list(discovery.records)
    records[1] = replace(records[1], marker_presence=record_presence)
    contradicted = replace(discovery, records=tuple(records))
    assert contradicted.records[1].marker_presence is record_presence
    assert markers[1].marker_presence is MarkerPresence.REGULAR_FILE

    _assert_rejected_before_filesystem_access(
        monkeypatch, tmp_path, contradicted, markers, "contradicts .*marker presence"
    )

    # Control: the very same marker results are accepted against the unaltered discovery, so
    # the rejection above is attributable to the presence disagreement alone.
    results = verify_segment_bytes(tmp_path, discovery, markers)
    assert [r.status for r in results] == [S.HASHED_STABLE, S.HASHED_STABLE]
    assert all(r.declared_sha256_matches is True for r in results)


@pytest.mark.parametrize("other_marker", ["absent", "symlink", "directory"])
def test_real_valid_v2_marker_result_cannot_be_paired_with_discovery_of_another_marker_state(
    tmp_path, monkeypatch, other_marker
):
    """Everything here comes from the real discovery and marker-validation routines: a genuine
    VALID_V2 result from a tree whose marker is a regular file, paired with the genuine
    discovery of a same-named segment whose marker is absent or not a regular file."""
    present = tmp_path / "present"
    other = tmp_path / "other"
    _put(present)
    _put(other, marker=False)
    marker_path = other / "raw" / "trades" / (SEG + ".meta.json")
    if other_marker == "symlink":
        marker_path.symlink_to(other / "raw" / "trades" / SEG)
    elif other_marker == "directory":
        marker_path.mkdir()
    expected = MarkerPresence.ABSENT if other_marker == "absent" else MarkerPresence.NON_REGULAR

    present_discovery = discover_sources(present)
    other_discovery = discover_sources(other)
    markers = validate_markers(present, present_discovery)
    assert markers[0].status is MarkerValidationStatus.VALID_V2
    assert markers[0].marker_presence is MarkerPresence.REGULAR_FILE
    assert other_discovery.records[0].marker_presence is expected
    assert other_discovery.records[0].relative_path == markers[0].relative_path

    _assert_rejected_before_filesystem_access(
        monkeypatch, other, other_discovery, markers, "contradicts .*marker presence"
    )


# ---------------------------------------------------------------------------
# Race / stability guards in the hashing stage. Each test drives the real
# verify_segment_bytes path with a deterministic hook on os.read (no sleeps, no
# timing races) and asserts the fixed reason code, so removing the corresponding
# guard changes the outcome and fails the test.
# ---------------------------------------------------------------------------

def test_pathname_replaced_by_another_inode_while_hashing_is_unknown_not_stable(
    tmp_path, monkeypatch
):
    """The descriptor opened first keeps yielding the original bytes, and the replacement
    holds identical bytes and size, so the digest alone cannot reveal the swap: only the
    post-read pathname identity check can."""
    payload = b"original bytes read through the already-open descriptor"
    segment = _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    original_inode = segment.stat().st_ino
    replaced = False

    def read_then_replace_pathname(fd, count):
        nonlocal replaced
        chunk = _REAL_READ(fd, count)
        if chunk and not replaced:
            replaced = True
            replacement = segment.with_name(segment.name + ".replacement")
            replacement.write_bytes(payload)
            os.replace(replacement, segment)
        return chunk

    monkeypatch.setattr(sv.os, "read", read_then_replace_pathname)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    assert replaced
    assert segment.stat().st_ino != original_inode, "fixture must really swap the inode"
    assert segment.read_bytes() == payload
    assert result.status is S.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_REPLACED_DURING_READ",)
    assert result.observed_sha256 is None
    assert result.observed_size == len(payload)
    assert result.bytes_hashed == len(payload)


def test_same_size_rewrite_with_restored_mtime_is_not_hashed_stable(tmp_path, monkeypatch):
    """Size, inode and mtime all look unchanged after the rewrite; the only observable change is
    ctime, which user space cannot set, so this exercises the stable-identity comparison."""
    payload = b"A" * 64
    rewritten = b"B" * 64
    segment = _put(tmp_path, payload=payload, marker=False)
    before = segment.stat()
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    rewrote = False

    def read_then_rewrite_in_place(fd, count):
        nonlocal rewrote
        chunk = _REAL_READ(fd, count)
        if chunk and not rewrote:
            rewrote = True
            observed_ctime = os.fstat(fd).st_ctime_ns
            with open(segment, "r+b") as handle:
                handle.write(rewritten)
            # The kernel's ctime clock is coarse, so re-apply the mtime restoration until ctime
            # has observably advanced. A bounded condition wait, not a sleep.
            deadline = time.monotonic() + 5.0
            while True:
                os.utime(segment, ns=(before.st_atime_ns, before.st_mtime_ns))
                if os.stat(segment).st_ctime_ns != observed_ctime:
                    break
                if time.monotonic() > deadline:
                    pytest.skip("this filesystem did not advance ctime on a metadata change")
        return chunk

    monkeypatch.setattr(sv.os, "read", read_then_rewrite_in_place)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    after = segment.stat()
    assert rewrote
    assert segment.read_bytes() == rewritten != payload
    assert after.st_size == before.st_size
    assert after.st_ino == before.st_ino
    assert after.st_mtime_ns == before.st_mtime_ns, "fixture must restore the mtime"
    assert after.st_ctime_ns != before.st_ctime_ns, "ctime is the only trace of the rewrite"
    assert result.status is not S.HASHED_STABLE
    assert result.status is S.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_CHANGED_DURING_READ",)
    assert result.observed_sha256 is None


@pytest.mark.parametrize("chunks_before_eof", [0, 1, 2])
def test_early_eof_after_the_size_was_observed_is_unknown_and_the_loop_terminates(
    tmp_path, monkeypatch, chunks_before_eof
):
    """The file is truncated after the descriptor's size was observed, so the reader hits a
    genuine EOF before the observed size is reached. The call counter turns an endless
    retry loop into a failure instead of a hang."""
    chunk_size = 8
    payload = bytes(range(40))
    segment = _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    monkeypatch.setattr(sv, "READ_CHUNK_BYTES", chunk_size)
    calls = 0

    def read_with_truncation(fd, count):
        nonlocal calls
        calls += 1
        if calls > 20:
            raise AssertionError("the read loop kept running after EOF")
        if calls == chunks_before_eof + 1:
            os.truncate(segment, chunks_before_eof * chunk_size)
        return _REAL_READ(fd, count)

    monkeypatch.setattr(sv.os, "read", read_with_truncation)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    assert result.status is S.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_SHORT_READ",)
    assert result.observed_sha256 is None
    assert result.observed_size == len(payload)
    assert result.bytes_hashed == chunks_before_eof * chunk_size
    assert calls == chunks_before_eof + 1, "the loop must stop at the first empty read"


def test_sequential_reads_never_exceed_read_chunk_bytes_apart_from_the_growth_probe(
    tmp_path, monkeypatch
):
    size = 2 * sv.READ_CHUNK_BYTES + 5
    payload = (bytes(range(256)) * (size // 256 + 1))[:size]
    _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    requested = []
    returned = []

    def recording_read(fd, count):
        requested.append(count)
        chunk = _REAL_READ(fd, count)
        returned.append(len(chunk))
        return chunk

    monkeypatch.setattr(sv.os, "read", recording_read)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    assert result.status is S.HASHED_STABLE
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert requested[-1] == 1, "the last request is the documented one-byte growth probe"
    sequential = requested[:-1]
    assert len(sequential) >= 3, "a payload above two chunks needs at least three reads"
    assert all(0 < count <= sv.READ_CHUNK_BYTES for count in sequential)
    assert sum(returned[:-1]) == size
    assert returned[-1] == 0


def test_ordinary_short_nonzero_reads_are_not_errors(tmp_path, monkeypatch):
    """A sequential reader may legitimately receive less than it asked for and must continue."""
    chunk_size = 16
    payload = bytes(range(100))
    _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    monkeypatch.setattr(sv, "READ_CHUNK_BYTES", chunk_size)
    requested = []

    def short_read(fd, count):
        requested.append(count)
        return _REAL_READ(fd, min(count, 7))

    monkeypatch.setattr(sv.os, "read", short_read)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    assert result.status is S.HASHED_STABLE
    assert result.reasons == ("SEGMENT_BYTES_STABLE_AND_HASHED",)
    assert result.observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.bytes_hashed == len(payload)
    assert requested[-1] == 1
    assert all(count <= chunk_size for count in requested[:-1])
    assert len(requested) - 1 > len(payload) // chunk_size, "short reads force extra requests"


def test_read_requests_never_exceed_remaining_observed_bytes_under_midloop_growth(
    tmp_path, monkeypatch
):
    """The loop is bounded by the size observed at open: when the file grows between data
    chunks, no request may ask for more than the bytes still owed from that observed size.

    Dropping the ``min(READ_CHUNK_BYTES, remaining)`` cap would let the loop read into the
    appended bytes (and report a different reason), which the static-file chunk-size test
    cannot see. The append uses ``Path.open`` (not ``os.read``), so it never re-enters the
    wrapper; the call counter turns a runaway loop into a failure instead of a hang."""
    chunk_size = 8
    payload = bytes(range(36))                      # chunks of 8, 8, 8, 8, 4
    segment = _put(tmp_path, payload=payload, marker=False)
    discovery = discover_sources(tmp_path)
    markers = validate_markers(tmp_path, discovery)
    monkeypatch.setattr(sv, "READ_CHUNK_BYTES", chunk_size)
    calls = 0
    requested = []

    def read_with_growth(fd, count):
        nonlocal calls
        calls += 1
        requested.append(count)
        if calls > 40:
            raise AssertionError("the read loop is not bounded by the initially observed size")
        if calls == 2:
            with segment.open("ab") as handle:
                handle.write(b"Z" * 100)
        return _REAL_READ(fd, count)

    monkeypatch.setattr(sv.os, "read", read_with_growth)
    (result,) = verify_segment_bytes(tmp_path, discovery, markers)
    monkeypatch.undo()

    assert result.status is S.UNKNOWN_IO
    assert result.reasons == ("SEGMENT_GREW_DURING_READ",)
    assert result.bytes_hashed == len(payload)
    assert result.observed_size == len(payload)
    assert result.observed_sha256 is None
    assert requested == [8, 8, 8, 8, 4, 1]
