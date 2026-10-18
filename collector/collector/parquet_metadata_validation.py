"""Read-only Parquet *metadata* verification of discovered raw source files.

Evidence pipeline (this module is the fifth stage; each stage stays distinguishable):

    source discovery -> marker-path classification -> bounded marker validation
        -> segment byte verification -> **Parquet metadata verification** -> final trust classification

What a result here means
------------------------
``VALID_PARQUET`` says only that the footer of a *stable, fully hashed* file parsed as structurally
valid Parquet metadata. It does NOT say the file is complete, that the data pages decode, that rows
are semantically correct, ordered, causally available or suitable for research, or that the source is
trusted. Footer row counts are not timestamp evidence: ``first_record_ts`` / ``last_record_ts`` in a
marker are the writer's claims about arrival order and are never compared to Parquet statistics here.
This module never returns, changes or promotes a ``TrustState`` and never touches the filesystem
except to read: no repair, quarantine, marker write, or recovery.

How the parsed metadata is bound to the bytes it describes
---------------------------------------------------------
A previous ``HASHED_STABLE`` result does not prove the path still names the same bytes: a same-size
replacement after that stage is invisible to size or mtime comparisons made later. So this stage:

1. opens the file itself, descriptor-relative and no-follow, exactly like the earlier stages;
2. hashes every byte of that *one* open descriptor in a bounded sequential pass and, during that same
   pass, captures the leading magic and the trailing ``[footer | footer_len | magic]`` window;
3. compares the SHA-256 and size of what it just read with the supplied byte-verification result, and
   returns ``UNKNOWN_IO`` (never parse facts) if they differ;
4. parses the *captured* bytes with PyArrow from memory, in an isolated worker process (see "Parser
   isolation and resource failures"). The parser therefore never reopens the path and never sees a
   byte that was not part of the hashed, stable observation.

The cost is one extra full read of every inspected file (O(file size) I/O, memory bounded by
``READ_CHUNK_BYTES`` plus the footer limit in this process). It is required: it is the only way to tie
the parsed footer to the earlier digest. Only metadata is parsed; no row group or data page is decoded.

Footer resource bound
---------------------
The trailing 4-byte length is read and checked *before* any footer bytes are buffered or handed to the
parser. A footer over the limit is never buffered or parsed, but the file still gets the full hashing
pass and must match the prior byte evidence before ``FOOTER_TOO_LARGE`` is reported; a mismatch is
``UNKNOWN_IO``, exactly as for any other file.

``DEFAULT_MAX_FOOTER_BYTES`` (32 MiB) is derived from the writer, not chosen arbitrarily:
``ParquetWriter`` flushes one row group per ``write_table`` call (>= 1 row each) and rotates segments
at ``segment_rows`` (5,000 by default, never configured higher in this repository); the widest
repository schema has 26 columns and PyArrow 24 measured about 110-120 bytes of footer per column
chunk for tiny values. 5,000 row groups x 26 columns x ~120 B ~= 15.6 MB is the theoretical per-segment
ceiling (realistic segments flush every 1,000 rows, i.e. ~20 KB). 32 MiB leaves ~2x margin over that
ceiling. Legacy hourly files have no such writer-side bound; a legitimate one larger than the limit
is reported as ``FOOTER_TOO_LARGE`` -- explicitly NOT as invalid Parquet and NOT as readable -- and the
caller may raise the limit up to ``MAX_ALLOWED_FOOTER_BYTES``. PyArrow's own thrift string/container
limits are set to the declared footer length, which no string or container can exceed.

Parser isolation and resource failures
--------------------------------------
PyArrow's footer parser is native code and cannot be trusted to fail politely. Measured with PyArrow
24.0.0 on Linux, parsing a *valid* file with 40,000 row groups (a 7.9 MiB footer) needs ~76 MiB of
address space beyond the interpreter's baseline (about 9.5x the footer, the same ratio for 1k, 5k x 26
columns, 40k and 100k row groups). With less, the parser either raises
``OSError("Couldn't deserialize thrift: std::bad_alloc")`` or the whole process dies with ``SIGABRT``
(``std::terminate``); for most limits tried it was the abort, which no ``except`` clause can catch.
Two consequences are built in:

* **No footer is parsed in this process.** The captured window is sent to a private worker
  (``_parquet_footer_worker``, run as ``python -I``) over a bounded binary protocol: a fixed header
  (footer length, file size, address-space budget) and exactly ``footer_len + 8`` bytes, answered by
  one fixed 64-byte reply (a code, three integers, an exception *type name*). The worker never gets a
  path and never opens a file. All hashing, descriptor/path identity checks, prior-digest comparison
  and result construction stay here. If the worker cannot be started the result is
  ``UNKNOWN_PARSER_FAILURE``; there is no in-process fallback.
* **Resource failure is not corruption.** Outcomes of the parse step, all after the full hash pass has
  matched the prior byte evidence:

  ================================  ===================================================================
  ``VALID_PARQUET``                 footer parsed and passed the structural checks
  ``INVALID_PARQUET``               parser reported malformed content (``OSError``/``ArrowException``
                                    such as a thrift failure, or ``UnicodeDecodeError`` from an
                                    undecodable column name) or the footer contradicts itself
  ``UNSUPPORTED_METADATA``          ``ArrowNotImplementedError``
  ``UNKNOWN_RESOURCE``              recognised resource exhaustion: ``pa.ArrowMemoryError``,
                                    ``MemoryError``, or the exact ``std::bad_alloc`` ``OSError``
                                    signature. Reasons: ``PARQUET_PARSER_RESOURCE_EXHAUSTED`` plus
                                    ``PARSER_RESOURCE_<ARROW_MEMORY_ERROR|MEMORY_ERROR|STD_BAD_ALLOC>``
  ``UNKNOWN_PARSER_FAILURE``        no trustworthy result: worker did not start, died (``SIGABRT``,
                                    ``SIGKILL``...), exited non-zero, timed out, or replied with
                                    garbage. Reasons: ``PARQUET_PARSER_WORKER_FAILED`` plus
                                    ``PARSER_WORKER_<SIGNAL_<name>|EXIT_CODE_<n>|NO_REPLY|TIMED_OUT|
                                    PROTOCOL_ERROR|START_FAILED>``. The cause is *not* assumed to be
                                    memory exhaustion even when it may be. Also: the worker replied
                                    validly but reported an exception that says nothing about the file
                                    (see below). Reasons: ``PARQUET_PARSER_UNEXPECTED_EXCEPTION`` plus
                                    ``PARSER_EXCEPTION_<sanitised exception type name>``.
  ================================  ===================================================================

  ``pa.ArrowMemoryError`` is a subclass of ``pa.ArrowException`` and ``MemoryError``, so it is
  recognised before the generic "malformed content" rule; the ``std::bad_alloc`` signature is matched
  exactly rather than as a substring, so a footer cannot relabel a parse failure by embedding the text.
  Any *other* exception in the worker (not malformed-content evidence, not an unsupported feature, not
  a recognised resource failure) says nothing about the file. That one file is
  ``UNKNOWN_PARSER_FAILURE`` with only the exception's sanitised *type name* in ``reasons`` (never its
  text), the worker is retired, and every other file in the call is processed and reported normally:
  one file's parser outcome never discards another file's result. It is never turned into
  ``INVALID_PARQUET`` and never into a validity claim. A defect in this module's own code is a different
  thing: it is not a parser outcome and propagates as an ordinary exception.
  For ``UNKNOWN_RESOURCE`` and ``UNKNOWN_PARSER_FAILURE`` every footer-derived field (``num_rows``,
  ``num_row_groups``, ``num_columns``, ``declared_record_count_matches``,
  ``marker_claims_corroborated_by_bytes``) is ``None``; the facts of the completed hash pass
  (``observed_size``, ``observed_sha256``, ``byte_evidence_matches_prior``, ``declared_size_matches``,
  ``declared_sha256_matches``, ``footer_length`` as the trailer's claim) are kept.

Containment and cost. One worker serves all footers of one ``verify_parquet_metadata`` call (starting
interpreter + PyArrow costs ~0.16 s, against ~30 us to parse an ordinary small footer in-process, so
one process per footer would be ~5000x slower; a scan of N files pays the start-up once). The worker is
killed, never reused, after a resource failure, crash, timeout or malformed reply. Per request the
parent allows ``WORKER_TIMEOUT_BASE_SECONDS + footer / WORKER_MIN_THROUGHPUT_BYTES_PER_SECOND`` (at
most ~94 s at the 256 MiB maximum; measured throughput is ~55 MiB/s, the allowance assumes 4 MiB/s) and
the worker caps its own ``RLIMIT_AS`` at ``already-mapped + WORKER_ADDRESS_SPACE_FACTOR (16) x footer +
WORKER_ADDRESS_SPACE_SLACK_BYTES (64 MiB)``. For the reported 7.9 MiB footer that is 191 MiB against
~76 MiB measured (the factor alone is ~1.7x the measured 9.5x expansion; the slack covers small
footers). Memory at the maximum footer limit is an *estimate* extrapolated from the measured ratio, not
a measurement at that size: this process holds one copy of the footer window (it is handed over
without a second copy); the worker holds one more plus ~9.5x for the parse, roughly 2.9 GiB all told,
with the worker's address space hard-capped at about 4.1 GiB above its baseline. ``RLIMIT_CORE`` is zero in the worker so a
native abort cannot write a core file. The address-space limit needs Linux (``resource`` and
``/proc/self/statm``); elsewhere the process isolation and timeout still apply. These numbers are
measurements of one PyArrow version: re-measure when it is upgraded (the real-parser tests exercise
the limit in CI).

Honest limits
-------------
* This is an observation, not a snapshot transaction: it does not lock the writer. Identity (device,
  inode, size, mtime, ctime) is re-checked after the pass and the path is re-``stat``-ed no-follow;
  a replacement that preserves every observed attribute *and* every byte is, by definition,
  indistinguishable and harmless to this stage's claims.
* Ancestors of the resolved ``data_root`` are trusted; only components below it are no-follow opened.
* ``marker_validations`` and ``byte_verifications`` are checked for internal consistency only; a
  caller-built result cannot be authenticated.
* Exceptions raised by PyArrow while parsing in-memory bytes (including ``OSError``, which PyArrow
  uses for thrift failures, and ``UnicodeDecodeError`` for an undecodable column name) are content
  evidence, not I/O failures -- except recognised resource failures, which are ``UNKNOWN_RESOURCE``.
  Any other exception type is deliberately not turned into a verdict about the file: it is
  ``UNKNOWN_PARSER_FAILURE`` for that file alone, never mislabelled as corruption. A systemic worker
  defect therefore appears as ``UNKNOWN_PARSER_FAILURE`` on every parsed file (never as a valid
  result); a caller should treat a whole batch of them as an operational alarm. The
  ``std::bad_alloc`` signature is the one PyArrow 24.0.0 produced; if a PyArrow upgrade changes it, a
  resource failure would read as malformed content. The real-parser constrained-memory test is the
  canary for that.
* Where safe no-follow descriptor-relative access is unavailable every candidate is ``UNKNOWN_IO``
  (``SAFE_OPEN_UNAVAILABLE``); there is no weaker fallback.
Diagnostics are fixed codes plus errno *names*; raw exception text and absolute paths never appear.
"""
from __future__ import annotations

import errno
import hashlib
import os
import selectors
import signal
import stat as _stat
import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence, Tuple, Union

from . import _parquet_footer_worker as _worker
from .marker_validation import (
    DeclaredMarkerEvidence,
    MarkerValidation,
    _SAFE_FLAGS,
    _errno_name,
    _stable_identity,
)
from .publication import _SHA256, _is_int
from .segment_validation import (
    SegmentByteVerification,
    SegmentVerificationStatus,
    _marker_evidence,
    _open_dir,
    _skip_record,
    _split_record_path,
)
from .source_catalog import RAW_DIRNAME, DiscoveryResult, SourceRecord, _resolve_root

PARQUET_METADATA_SCHEMA_VERSION = 1
READ_CHUNK_BYTES = 1024 * 1024

PARQUET_MAGIC = b"PAR1"
#: 4-byte little-endian footer length + 4-byte trailing magic.
_TRAILER_BYTES = 8
#: Leading magic + (non-empty) footer >= 1 byte + trailer. A footer cannot be empty, so the smallest
#: possible Parquet file is 4 + 1 + 8 = 13 bytes; 12 is the smallest size at which the footer-length
#: arithmetic below is even meaningful, and an empty footer is rejected separately.
_MIN_PARQUET_BYTES = len(PARQUET_MAGIC) + _TRAILER_BYTES

#: See the module docstring for the derivation. A resource boundary, not a plausibility statement.
DEFAULT_MAX_FOOTER_BYTES = 32 * 1024 * 1024
#: Hard ceiling for the configurable limit, so the bound itself cannot be configured away.
MAX_ALLOWED_FOOTER_BYTES = 256 * 1024 * 1024


class ParquetMetadataStatus(str, Enum):
    """What was established about a file's Parquet *metadata*. Not a trust state."""

    #: Metadata was not safely attempted (see ``reasons``); nothing was opened or the earlier stage did
    #: not produce stable byte evidence.
    NOT_CHECKED = "NOT_CHECKED"
    #: The footer of a stable, hashed file parsed as structurally valid Parquet metadata. Nothing
    #: about completeness, data pages, semantics, or trust.
    VALID_PARQUET = "VALID_PARQUET"
    #: Actual file content demonstrates a malformed Parquet structure (see ``reasons``).
    INVALID_PARQUET = "INVALID_PARQUET"
    #: The trailer declares a footer larger than the configured limit. The footer was not buffered or
    #: parsed, but the file's bytes were fully hashed and proved equal to the earlier byte evidence
    #: first. This is a resource decision, not evidence of corruption.
    FOOTER_TOO_LARGE = "FOOTER_TOO_LARGE"
    #: The parser reported a feature it does not support. Not proof of corruption, not readable.
    UNSUPPORTED_METADATA = "UNSUPPORTED_METADATA"
    #: Opening, reading, stable inspection, or agreement with the earlier byte evidence could not be
    #: established. Never converted into corruption or absence.
    UNKNOWN_IO = "UNKNOWN_IO"
    #: The footer was within the configured limit and the bytes matched the earlier evidence, but the
    #: native parser *recognisably* exhausted a resource (memory) while reading it, so whether the
    #: metadata is valid was not established. NOT evidence of corruption and NOT a validity claim:
    #: no parse facts are reported. The byte-level facts of the completed hash pass are kept.
    UNKNOWN_RESOURCE = "UNKNOWN_RESOURCE"
    #: The isolated parser worker could not be started, died, timed out, answered with something
    #: untrustworthy, or reported an exception that says nothing about the file, so no parse result
    #: exists. The cause is deliberately *not* assumed to be memory exhaustion (``reasons`` carries the
    #: exit signal/code or the exception type name, never process output or exception text). NOT
    #: evidence of corruption; no parse facts; the byte-level facts of the completed hash pass are kept.
    UNKNOWN_PARSER_FAILURE = "UNKNOWN_PARSER_FAILURE"


@dataclass(frozen=True)
class ParquetMetadataVerification:
    relative_path: str
    status: ParquetMetadataStatus
    #: Stable machine-readable codes. Never raw exception text, never an absolute path.
    reasons: Tuple[str, ...]
    #: Facts observed from the single descriptor read by *this* stage.
    observed_size: Optional[int] = None
    observed_sha256: Optional[str] = None
    bytes_hashed: int = 0
    #: Footer length declared by the trailer (a claim until the footer parses). ``None`` if unread.
    footer_length: Optional[int] = None
    #: Parquet metadata facts: set only when ``status`` is ``VALID_PARQUET``, else ``None`` (this
    #: includes ``UNKNOWN_RESOURCE`` and ``UNKNOWN_PARSER_FAILURE``: nothing is parsed, nothing claimed).
    num_rows: Optional[int] = None
    num_row_groups: Optional[int] = None
    num_columns: Optional[int] = None
    #: Did what this stage read equal the earlier stage's size and SHA-256? ``None`` = not compared.
    byte_evidence_matches_prior: Optional[bool] = None
    #: Marker comparisons. ``None`` whenever there is no VALID_V2 marker declaration, or the
    #: comparison was not performed (see ``reasons``). Marker content is a claim, never evidence.
    declared_size_matches: Optional[bool] = None
    declared_sha256_matches: Optional[bool] = None
    declared_record_count_matches: Optional[bool] = None
    #: ``True`` only if BOTH declared size and SHA-256 equal what this stage hashed. The record-count
    #: comparison is performed only under this condition: a marker whose bytes do not match this
    #: file is not a claim about this file, so a count "match" would be a false confirmation.
    marker_claims_corroborated_by_bytes: Optional[bool] = None
    verification_schema_version: int = PARQUET_METADATA_SCHEMA_VERSION


def _validate_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_ALLOWED_FOOTER_BYTES:
        raise ValueError(
            f"max_footer_bytes must be an int in 1..{MAX_ALLOWED_FOOTER_BYTES}, got {value!r}")
    return value


def _result(rec: SourceRecord, status: ParquetMetadataStatus, *reasons: str, **fields
            ) -> ParquetMetadataVerification:
    return ParquetMetadataVerification(rec.relative_path, status, tuple(reasons), **fields)


def _not_checked(rec: SourceRecord, *reasons: str) -> ParquetMetadataVerification:
    return _result(rec, ParquetMetadataStatus.NOT_CHECKED, *reasons)


def _unknown(rec: SourceRecord, *reasons: str, **fields) -> ParquetMetadataVerification:
    return _result(rec, ParquetMetadataStatus.UNKNOWN_IO, *reasons, **fields)


def _check_prior(rec: SourceRecord, prior: SegmentByteVerification,
                 evidence: Optional[DeclaredMarkerEvidence]) -> None:
    """Reject an inconsistent or fabricated byte-verification entry before any file is opened.

    Internal consistency only (it cannot authenticate a caller-built object). The ValueError names
    the offending field, never its value.
    """
    if not isinstance(prior, SegmentByteVerification):
        raise ValueError(f"byte verification for {rec.relative_path!r} has an unexpected type")
    if prior.relative_path != rec.relative_path:
        raise ValueError("byte_verifications must match discovery order and paths")
    problems = []
    if prior.status is SegmentVerificationStatus.HASHED_STABLE:
        if not _is_int(prior.observed_size) or prior.observed_size < 0:
            problems.append("observed_size")
        if not isinstance(prior.observed_sha256, str) or not _SHA256.match(prior.observed_sha256):
            problems.append("observed_sha256")
        if prior.bytes_hashed != prior.observed_size:
            problems.append("bytes_hashed")
        if "SEGMENT_BYTES_STABLE_AND_HASHED" not in prior.reasons:
            problems.append("reasons")
        if not problems:
            if evidence is None:
                expected = (None, None)
            else:
                expected = (
                    prior.observed_size == evidence.size_bytes,
                    prior.observed_sha256.lower() == evidence.sha256.lower(),
                )
            if (prior.declared_size_matches, prior.declared_sha256_matches) != expected:
                problems.append("declared_matches")
    else:
        if prior.observed_sha256 is not None:
            problems.append("observed_sha256")
        if prior.declared_size_matches is not None or prior.declared_sha256_matches is not None:
            problems.append("declared_matches")
    if problems:
        raise ValueError(
            f"byte verification for {rec.relative_path!r} is inconsistent: " + ", ".join(problems))


def _read_trailer(fd: int, size: int) -> Optional[bytes]:
    """The last 8 bytes via one positioned read, or ``None`` on a short read."""
    data = os.pread(fd, _TRAILER_BYTES, size - _TRAILER_BYTES)
    return data if len(data) == _TRAILER_BYTES else None


def _path_still_names(stream_fd: int, filename: str, first: os.stat_result) -> bool:
    now = os.stat(filename, dir_fd=stream_fd, follow_symlinks=False)
    return _stat.S_ISREG(now.st_mode) and (now.st_dev, now.st_ino) == (first.st_dev, first.st_ino)


def _structure_reasons(size: int, head: bytes, window: bytes) -> Tuple[str, ...]:
    """Structural violations provable from captured bytes. Empty tuple = structure is plausible."""
    if size < _MIN_PARQUET_BYTES:
        return ("PARQUET_FILE_TOO_SHORT",)
    reasons = []
    if head != PARQUET_MAGIC:
        reasons.append("PARQUET_HEAD_MAGIC_INVALID")
    trailer = window[-_TRAILER_BYTES:]
    if trailer[4:] != PARQUET_MAGIC:
        reasons.append("PARQUET_TAIL_MAGIC_INVALID")
    footer_len = int.from_bytes(trailer[:4], "little")
    if footer_len == 0:
        reasons.append("PARQUET_FOOTER_LENGTH_ZERO")
    elif footer_len > size - _MIN_PARQUET_BYTES:
        reasons.append("PARQUET_FOOTER_LENGTH_EXCEEDS_FILE")
    return tuple(reasons)


#: Parser isolation tunables. The measurements behind each value are in the module docstring
#: ("Parser isolation and resource failures"); they are module attributes so tests can drive the real
#: worker into its failure paths deterministically.
WORKER_ADDRESS_SPACE_FACTOR = 16
WORKER_ADDRESS_SPACE_SLACK_BYTES = 64 * 1024 * 1024
WORKER_TIMEOUT_BASE_SECONDS = 30.0
WORKER_MIN_THROUGHPUT_BYTES_PER_SECOND = 4 * 1024 * 1024
_WORKER_SHUTDOWN_GRACE_SECONDS = 2.0
_WRITE_CHUNK_BYTES = 1024 * 1024

# Pipe I/O with the worker is bound to these names at import time, so nothing that patches or wraps
# ``os.read`` / ``os.write`` for the *file* read path (fault injection, tests) can intercept the
# worker protocol.
_pipe_read = os.read
_pipe_write = os.write

_RESOURCE_KIND_CODES = {
    _worker.RESOURCE_ARROW_MEMORY_ERROR: "PARSER_RESOURCE_ARROW_MEMORY_ERROR",
    _worker.RESOURCE_MEMORY_ERROR: "PARSER_RESOURCE_MEMORY_ERROR",
    _worker.RESOURCE_STD_BAD_ALLOC: "PARSER_RESOURCE_STD_BAD_ALLOC",
}

_Parsed = Tuple[ParquetMetadataStatus, Tuple[str, ...], Optional[Tuple[int, int, int]]]


def _address_space_budget(footer_len: int) -> int:
    """Address space, beyond what the worker already has mapped, that one parse may use."""
    return WORKER_ADDRESS_SPACE_FACTOR * footer_len + WORKER_ADDRESS_SPACE_SLACK_BYTES


def _parse_timeout(footer_len: int) -> float:
    """Finite wall-clock allowance for one request, including a first-request interpreter start."""
    return WORKER_TIMEOUT_BASE_SECONDS + footer_len / WORKER_MIN_THROUGHPUT_BYTES_PER_SECOND


def _worker_argv() -> Optional[List[str]]:
    """Command line of the parser worker, or ``None`` if no interpreter can be named (fail closed).
    The command line never carries a footer, a path to the source file, or any file content."""
    if not sys.executable:
        return None
    return [sys.executable, "-I", _worker.__file__]


def _parser_failure(*detail: str) -> _Parsed:
    return ParquetMetadataStatus.UNKNOWN_PARSER_FAILURE, ("PARQUET_PARSER_WORKER_FAILED", *detail), None


def _exit_detail(returncode: Optional[int]) -> str:
    """A fixed code for how the worker ended. Signal *names* and integer exit codes only; process
    output is never captured, so none can reach a result."""
    if returncode is None:
        return "PARSER_WORKER_EXITED_ABNORMALLY"
    if returncode < 0:
        try:
            return f"PARSER_WORKER_SIGNAL_{signal.Signals(-returncode).name}"
        except ValueError:
            return f"PARSER_WORKER_SIGNAL_{-returncode}"
    if returncode == 0:
        return "PARSER_WORKER_NO_REPLY"
    return f"PARSER_WORKER_EXIT_CODE_{returncode}"


def _type_name(raw: bytes) -> str:
    text = raw.rstrip(b"\x00").decode("ascii", "replace")
    return "".join(ch for ch in text if ch.isalnum() or ch == "_")[: _worker.NAME_BYTES] or "Exception"


class _ParserWorker:
    """One reusable parser child process, owned by a single ``verify_parquet_metadata`` call.

    Started lazily by the first footer that needs parsing and reused for the next ones, because the
    interpreter plus PyArrow start-up (~0.16 s measured) would otherwise dwarf the parse of an
    ordinary footer (~30 us). The worker is discarded -- killed, never reused -- after anything but a
    clean parse verdict, so a damaged or exhausted worker can never influence a later file.
    """

    def __init__(self) -> None:
        self._proc: Optional["subprocess.Popen[bytes]"] = None

    def parse(self, window, size: int, footer_len: int) -> _Parsed:
        """Parse the captured ``footer + trailer`` window in the worker. ``(status, reasons, facts)``."""
        if len(window) != footer_len + _TRAILER_BYTES:
            raise RuntimeError("internal error: captured window does not match the declared footer length")
        argv = _worker_argv()
        if argv is None:
            return _parser_failure("PARSER_WORKER_START_FAILED")
        if self._proc is None:
            try:
                self._proc = subprocess.Popen(
                    argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    close_fds=True, cwd=os.sep, start_new_session=True)
            except OSError as exc:
                return _parser_failure("PARSER_WORKER_START_FAILED", f"ERRNO_{_errno_name(exc)}")
        header = _worker.REQUEST_HEADER.pack(
            _worker.REQUEST_MAGIC, footer_len, size, _address_space_budget(footer_len))
        kind, reply = self._exchange(header, window, _parse_timeout(footer_len))
        if kind == "timeout":
            self._discard()
            return _parser_failure("PARSER_WORKER_TIMED_OUT")
        if kind == "closed":
            return _parser_failure(_exit_detail(self._reap()))
        return self._interpret(reply)

    def _exchange(self, header: bytes, body, timeout: float) -> Tuple[str, Optional[bytes]]:
        """Send one request and read exactly one fixed-size reply, never blocking past ``timeout``.

        Returns ``("reply", bytes)``, ``("timeout", None)`` or ``("closed", None)`` (the worker's
        output pipe ended before a full reply). Reads are capped at the reply size.
        """
        proc = self._proc
        assert proc is not None and proc.stdin is not None and proc.stdout is not None
        wfd, rfd = proc.stdin.fileno(), proc.stdout.fileno()
        os.set_blocking(wfd, False)
        os.set_blocking(rfd, False)
        parts = [memoryview(header), memoryview(body)]
        part, offset = 0, 0
        reply = bytearray()
        deadline = time.monotonic() + timeout
        selector = selectors.DefaultSelector()
        try:
            selector.register(rfd, selectors.EVENT_READ)
            selector.register(wfd, selectors.EVENT_WRITE)
            writing = True
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return "timeout", None
                for key, _events in selector.select(remaining):
                    if key.fd == rfd:
                        try:
                            data = _pipe_read(rfd, _worker.REPLY.size - len(reply))
                        except BlockingIOError:
                            continue
                        except OSError:
                            return "closed", None
                        if not data:
                            return "closed", None
                        reply.extend(data)
                        if len(reply) == _worker.REPLY.size:
                            return "reply", bytes(reply)
                    elif writing:
                        try:
                            written = _pipe_write(wfd, parts[part][offset: offset + _WRITE_CHUNK_BYTES])
                        except BlockingIOError:
                            continue
                        except OSError:        # worker gone or closed its input: the read side reports it
                            selector.unregister(wfd)
                            writing = False
                            continue
                        offset += written
                        if offset >= len(parts[part]):
                            part, offset = part + 1, 0
                            if part == len(parts):
                                selector.unregister(wfd)
                                writing = False
        finally:
            selector.close()
            for view in parts:
                view.release()

    def _interpret(self, reply: bytes) -> _Parsed:
        (magic, code, detail, problems, reserved,
         num_rows, num_rgs, num_cols, raw_name) = _worker.REPLY.unpack(reply)
        counts = (num_rows, num_rgs, num_cols)
        well_formed = (
            magic == _worker.REPLY_MAGIC and reserved == 0 and code in _worker.KNOWN_CODES
            and (code == _worker.CODE_VALID or counts == (0, 0, 0))
            and (code == _worker.CODE_RESOURCE or detail == 0)
            and (code == _worker.CODE_INVALID_STRUCTURE or problems == 0)
            and (code != _worker.CODE_RESOURCE or detail in _worker.KNOWN_RESOURCE_KINDS)
            and (code != _worker.CODE_INVALID_STRUCTURE
                 or (problems != 0 and problems & ~_worker.KNOWN_PROBLEM_BITS == 0))
            and (code == _worker.CODE_UNEXPECTED or raw_name.rstrip(b"\x00") == b"")
        )
        if not well_formed:
            self._discard()
            return _parser_failure("PARSER_WORKER_PROTOCOL_ERROR")
        if code == _worker.CODE_VALID:
            return ParquetMetadataStatus.VALID_PARQUET, ("PARQUET_METADATA_VALID",), counts
        if code == _worker.CODE_INVALID_PARSE:
            return ParquetMetadataStatus.INVALID_PARQUET, ("PARQUET_FOOTER_PARSE_FAILED",), None
        if code == _worker.CODE_INVALID_STRUCTURE:
            reasons = []
            if problems & _worker.PROBLEM_ROWS_SUM_MISMATCH:
                reasons.append("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH")
            if problems & _worker.PROBLEM_CHUNK_OUT_OF_BOUNDS:
                reasons.append("PARQUET_COLUMN_CHUNK_OUT_OF_BOUNDS")
            return ParquetMetadataStatus.INVALID_PARQUET, tuple(reasons), None
        if code == _worker.CODE_UNSUPPORTED:
            return ParquetMetadataStatus.UNSUPPORTED_METADATA, ("PARQUET_METADATA_UNSUPPORTED",), None
        # Resource failure or an unexpected exception: the worker survived but is not trusted again.
        self._discard()
        if code == _worker.CODE_RESOURCE:
            return (ParquetMetadataStatus.UNKNOWN_RESOURCE,
                    ("PARQUET_PARSER_RESOURCE_EXHAUSTED", _RESOURCE_KIND_CODES[detail]), None)
        # CODE_UNEXPECTED, a well-formed reply: the parser raised something that is not evidence about
        # this file. That is this file's outcome, not a reason to lose the other files' results; it is
        # neither corruption nor a validity claim, and only the sanitised type name is carried.
        return (ParquetMetadataStatus.UNKNOWN_PARSER_FAILURE,
                ("PARQUET_PARSER_UNEXPECTED_EXCEPTION", f"PARSER_EXCEPTION_{_type_name(raw_name)}"), None)

    @staticmethod
    def _close_pipes(proc: "subprocess.Popen[bytes]") -> None:
        for stream in (proc.stdin, proc.stdout):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass

    def _reap(self) -> Optional[int]:
        """Exit status of a worker whose output closed; kill it if it somehow lingers."""
        proc, self._proc = self._proc, None
        assert proc is not None
        returncode: Optional[int] = None
        try:
            returncode = proc.wait(timeout=_WORKER_SHUTDOWN_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        self._close_pipes(proc)
        return returncode

    def _discard(self) -> None:
        """Kill the worker without ceremony."""
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            proc.kill()
        except OSError:
            pass
        proc.wait()
        self._close_pipes(proc)

    def close(self) -> None:
        """End of the verification run: let an idle worker exit on EOF, kill it if it does not."""
        proc, self._proc = self._proc, None
        if proc is None:
            return
        if proc.stdin is not None:
            try:
                proc.stdin.close()
            except OSError:
                pass
        try:
            proc.wait(timeout=_WORKER_SHUTDOWN_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        self._close_pipes(proc)


def _inspect_open_file(
    stream_fd: int,
    filename: str,
    rec: SourceRecord,
    prior: SegmentByteVerification,
    evidence: Optional[DeclaredMarkerEvidence],
    file_flags: int,
    limit: int,
    worker: _ParserWorker,
) -> ParquetMetadataVerification:
    try:
        fd = os.open(filename, file_flags, dir_fd=stream_fd)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            return _unknown(rec, "SEGMENT_SYMLINK_AT_OPEN")
        if isinstance(exc, FileNotFoundError):
            return _unknown(rec, "SEGMENT_MISSING_AT_OPEN")
        if exc.errno == errno.ENOTDIR:
            return _unknown(rec, "SEGMENT_NOT_REGULAR_AT_OPEN")
        return _unknown(rec, "SEGMENT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")

    total = 0
    try:
        try:
            first = os.fstat(fd)
        except OSError as exc:
            return _unknown(rec, "SEGMENT_FSTAT_FAILED", f"ERRNO_{_errno_name(exc)}")
        if not _stat.S_ISREG(first.st_mode):
            return _unknown(rec, "SEGMENT_NOT_REGULAR_AT_OPEN", observed_size=first.st_size)
        size = first.st_size
        if size < 0:
            return _unknown(rec, "SEGMENT_SIZE_INVALID_AT_OPEN")
        if size != prior.observed_size:
            # Cheap, exact: the file is already known not to be the one that was hashed.
            return _unknown(
                rec, "BYTES_CHANGED_SINCE_BYTE_VERIFICATION", "BYTE_VERIFICATION_SIZE_DIFFERS",
                observed_size=size, byte_evidence_matches_prior=False,
            )

        # Decide the capture window from the trailer, so that no footer byte is buffered before the
        # declared length has been checked against the limit.
        window_len = min(size, _TRAILER_BYTES)
        footer_len: Optional[int] = None
        pre_trailer: Optional[bytes] = None
        oversized = False
        if size >= _MIN_PARQUET_BYTES:
            try:
                pre_trailer = _read_trailer(fd, size)
            except OSError as exc:
                return _unknown(rec, "SEGMENT_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                                observed_size=size)
            if pre_trailer is None:
                return _unknown(rec, "SEGMENT_SHORT_READ", observed_size=size)
            declared = int.from_bytes(pre_trailer[:4], "little")
            if pre_trailer[4:] == PARQUET_MAGIC and 0 < declared <= size - _MIN_PARQUET_BYTES:
                footer_len = declared
                if declared > limit:
                    # A resource decision about the footer, not a reason to skip byte identity: keep
                    # the capture window at the 8-byte trailer (the footer is never buffered or
                    # parsed) but still run the full hashing pass below, and only report
                    # FOOTER_TOO_LARGE once that pass has proved the bytes equal the prior evidence.
                    oversized = True
                else:
                    window_len = _TRAILER_BYTES + declared

        # One bounded sequential pass over the single descriptor: hash everything, capture only the
        # leading magic and the trailing window.
        digest = hashlib.sha256()
        head = bytearray()
        window = bytearray()
        window_start = size - window_len
        offset = 0
        while offset < size:
            try:
                chunk = os.read(fd, min(READ_CHUNK_BYTES, size - offset))
            except OSError as exc:
                return _unknown(rec, "SEGMENT_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                                observed_size=size, bytes_hashed=total)
            if not chunk:
                return _unknown(rec, "SEGMENT_SHORT_READ", observed_size=size, bytes_hashed=total)
            digest.update(chunk)
            if offset < len(PARQUET_MAGIC):
                head += chunk[: len(PARQUET_MAGIC) - offset]
            end = offset + len(chunk)
            if end > window_start:
                window += chunk[max(0, window_start - offset):]
            offset = end
            total = end

        try:
            extra = os.read(fd, 1)
            second = os.fstat(fd)
            same_path = _path_still_names(stream_fd, filename, first)
        except FileNotFoundError:
            return _unknown(rec, "SEGMENT_REPLACED_DURING_READ", observed_size=size, bytes_hashed=total)
        except OSError as exc:
            return _unknown(rec, "SEGMENT_STABILITY_CHECK_FAILED", f"ERRNO_{_errno_name(exc)}",
                            observed_size=size, bytes_hashed=total)
        if extra:
            return _unknown(rec, "SEGMENT_GREW_DURING_READ", observed_size=size, bytes_hashed=total)
        if not same_path:
            return _unknown(rec, "SEGMENT_REPLACED_DURING_READ", observed_size=size, bytes_hashed=total)
        if (_stable_identity(first) != _stable_identity(second) or total != size
                or second.st_size != size):
            return _unknown(rec, "SEGMENT_CHANGED_DURING_READ", observed_size=size, bytes_hashed=total)

        actual_sha = digest.hexdigest()
        observed = dict(observed_size=size, observed_sha256=actual_sha, bytes_hashed=total)
        if actual_sha.lower() != prior.observed_sha256.lower():
            return _unknown(
                rec, "BYTES_CHANGED_SINCE_BYTE_VERIFICATION", "BYTE_VERIFICATION_SHA256_DIFFERS",
                byte_evidence_matches_prior=False, **observed,
            )
        if pre_trailer is not None and bytes(window[-_TRAILER_BYTES:]) != pre_trailer:
            # Cannot happen for a stable file; fail closed rather than trust either observation.
            return _unknown(rec, "TRAILER_CHANGED_DURING_READ",
                            byte_evidence_matches_prior=True, **observed)

        # Marker comparison facts about the bytes (independent of the earlier stage's copy).
        size_match = sha_match = None
        if evidence is not None:
            size_match = size == evidence.size_bytes
            sha_match = actual_sha.lower() == evidence.sha256.lower()
        base = dict(observed, byte_evidence_matches_prior=True,
                    declared_size_matches=size_match, declared_sha256_matches=sha_match)
        mismatch_reasons = []
        if size_match is False:
            mismatch_reasons.append("DECLARED_SIZE_MISMATCH")
        if sha_match is False:
            mismatch_reasons.append("DECLARED_SHA256_MISMATCH")
        if evidence is not None and evidence.legacy:
            mismatch_reasons.append("DECLARED_MARKER_LEGACY")

        # Precedence (all after the stable pass and the SHA-256/size match against the prior
        # byte-verification evidence): structural errors provable from the captured leading magic
        # and trailer come first, and only a structurally plausible file can be reported as having
        # an oversized footer. The oversized footer is never buffered, so these checks use only the
        # 4-byte head and the 8-byte trailer, which are captured in every case.
        structure = _structure_reasons(size, bytes(head), bytes(window))
        if structure:
            declared_len = (int.from_bytes(window[-_TRAILER_BYTES:][:4], "little")
                            if size >= _MIN_PARQUET_BYTES else None)
            return _result(rec, ParquetMetadataStatus.INVALID_PARQUET, *structure, *mismatch_reasons,
                           footer_length=declared_len, **base)

        if oversized:
            # Basic structure is plausible and the bytes equal the prior evidence. No footer byte
            # was captured, so nothing footer-derived is reported.
            return _result(
                rec, ParquetMetadataStatus.FOOTER_TOO_LARGE, "FOOTER_EXCEEDS_LIMIT", *mismatch_reasons,
                footer_length=footer_len, **base,
            )

        if footer_len is None:  # unreachable: structure passed, so the pre-read trailer was plausible
            return _unknown(rec, "TRAILER_CHANGED_DURING_READ", **base)

        # The captured, hashed window goes to the isolated worker as-is (no copy, no path). Whatever
        # the worker does -- verdict, resource failure, crash, timeout -- is a parser outcome; only
        # the VALID verdict carries parse facts.
        status, reasons, facts = worker.parse(window, size, footer_len)
        if status is not ParquetMetadataStatus.VALID_PARQUET:
            return _result(rec, status, *reasons, *mismatch_reasons, footer_length=footer_len, **base)

        num_rows, num_rgs, num_cols = facts
        corroborated: Optional[bool] = None
        count_match: Optional[bool] = None
        if evidence is not None:
            corroborated = bool(size_match and sha_match)
            if corroborated:
                count_match = _is_int(evidence.record_count) and num_rows == evidence.record_count
                if not count_match:
                    mismatch_reasons.append("DECLARED_RECORD_COUNT_MISMATCH")
            else:
                mismatch_reasons.append("RECORD_COUNT_COMPARISON_SKIPPED_BYTES_UNCORROBORATED")
        return _result(
            rec, ParquetMetadataStatus.VALID_PARQUET, *reasons, *mismatch_reasons,
            footer_length=footer_len, num_rows=num_rows, num_row_groups=num_rgs, num_columns=num_cols,
            declared_record_count_matches=count_match,
            marker_claims_corroborated_by_bytes=corroborated, **base,
        )
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def verify_parquet_metadata(
    data_root: Union[str, os.PathLike[str]],
    discovery: DiscoveryResult,
    marker_validations: Sequence[MarkerValidation],
    byte_verifications: Sequence[SegmentByteVerification],
    *,
    max_footer_bytes: int = DEFAULT_MAX_FOOTER_BYTES,
) -> Tuple[ParquetMetadataVerification, ...]:
    """Validate Parquet footers without writes, recovery, or trust-state promotion.

    ``marker_validations`` and ``byte_verifications`` must be the ordered results of
    ``validate_markers`` and ``verify_segment_bytes`` for the same ``discovery``. Only ``VALID_V2``
    markers supply declarations; unmarked sources and legacy hourly Parquet are inspected with no
    marker evidence inferred. A record is opened only if the byte-verification stage produced
    ``HASHED_STABLE`` evidence for it; otherwise it is ``NOT_CHECKED``.

    Raises ``ValueError``, before any filesystem access and without partial results, if the inputs are
    misaligned or hold an inconsistent or fabricated entry, or if ``max_footer_bytes`` is out of range.
    These checks establish internal consistency only, not authenticity. Nothing a file or the parser
    does raises: malformed content, resource exhaustion, a crashed or hung worker and an unexpected
    parser exception are all per-file results, so one file's outcome never discards another's. (A defect
    in this module's own code is not a parser outcome and propagates.)

    Observation, not a snapshot transaction (see the module docstring for the binding argument and the
    resource bound).
    """
    limit = _validate_limit(max_footer_bytes)
    markers = tuple(marker_validations)
    priors = tuple(byte_verifications)
    records = discovery.records
    if len(markers) != len(records) or len(priors) != len(records):
        raise ValueError(
            "marker_validations and byte_verifications must each contain one result per discovery record")

    evidence_by_path = {}
    for rec, marker, prior in zip(records, markers, priors):
        if marker.relative_path != rec.relative_path:
            raise ValueError("marker_validations must match discovery order and paths")
        evidence = _marker_evidence(rec, marker)
        _check_prior(rec, prior, evidence)
        if _skip_record(rec) is not None and prior.status is SegmentVerificationStatus.HASHED_STABLE:
            raise ValueError(
                f"byte verification for {rec.relative_path!r} hashed a record discovery marked unusable")
        evidence_by_path[rec.relative_path] = evidence

    root = _resolve_root(data_root)
    candidates = []
    for rec, prior in zip(records, priors):
        skipped = _skip_record(rec)
        if skipped is not None:
            candidates.append(_not_checked(rec, *skipped.reasons))
        elif prior.status is not SegmentVerificationStatus.HASHED_STABLE:
            candidates.append(_not_checked(
                rec, "BYTE_VERIFICATION_NOT_HASHED_STABLE", f"PRIOR_STATUS_{prior.status.value}"))
        else:
            candidates.append(None)

    if _SAFE_FLAGS is None:
        return tuple(
            c if c is not None else _unknown(rec, "SAFE_OPEN_UNAVAILABLE")
            for rec, c in zip(records, candidates)
        )

    dir_flags, file_flags = _SAFE_FLAGS
    output: list[ParquetMetadataVerification] = []
    open_fds: list[int] = []
    root_fd: Optional[Union[int, Tuple[str, ...]]] = None
    raw_fd: Optional[Union[int, Tuple[str, ...]]] = None
    stream_fds: dict[str, Union[int, Tuple[str, ...]]] = {}
    worker = _ParserWorker()          # lazy: no process is started unless a footer needs parsing

    try:
        for rec, prior, candidate in zip(records, priors, candidates):
            if candidate is not None:
                output.append(candidate)
                continue
            split = _split_record_path(rec)
            if split is None:
                output.append(_unknown(rec, "SOURCE_PATH_UNUSABLE"))
                continue
            stream, filename = split
            if root_fd is None:
                try:
                    fd = os.open(str(root), dir_flags)
                    open_fds.append(fd)
                    root_fd = fd
                except OSError as exc:
                    root_fd = ("DATA_ROOT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
            if isinstance(root_fd, tuple):
                output.append(_unknown(rec, *root_fd))
                continue
            if raw_fd is None:
                raw_fd = _open_dir(RAW_DIRNAME, root_fd, dir_flags, open_fds)
            if isinstance(raw_fd, tuple):
                output.append(_unknown(rec, *raw_fd))
                continue
            if stream not in stream_fds:
                stream_fds[stream] = _open_dir(stream, raw_fd, dir_flags, open_fds)
            stream_fd = stream_fds[stream]
            if isinstance(stream_fd, tuple):
                output.append(_unknown(rec, *stream_fd))
                continue
            output.append(_inspect_open_file(
                stream_fd, filename, rec, prior, evidence_by_path.get(rec.relative_path),
                file_flags, limit, worker,
            ))
    finally:
        worker.close()
        for fd in open_fds:
            try:
                os.close(fd)
            except OSError:
                pass
    return tuple(output)
