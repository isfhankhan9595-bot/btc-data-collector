"""Private parser worker for ``parquet_metadata_validation``. Not a public interface.

Why this process exists
-----------------------
PyArrow's footer parser is native code. Under address-space exhaustion it has been observed (PyArrow
24.0.0, a valid file with 40,000 row groups and a ~7.9 MiB footer) to fail in two different ways that
no Python ``except`` clause can tell apart in advance: sometimes it raises ``OSError("Couldn't
deserialize thrift: std::bad_alloc")``, and for most limits it dies with ``SIGABRT`` after
``terminate called after throwing an instance of 'std::bad_alloc'``. The second outcome kills the
whole interpreter. The verifier therefore never parses in its own process: it hands the captured,
already-hashed footer window to this worker and treats whatever happens here as a *parser* outcome.

What this module is
-------------------
* Run as ``python -I <this file>``. It must import only the standard library and PyArrow (``-I``
  puts neither the script directory nor the working directory on ``sys.path``), so it cannot reach
  any other repository module.
* A strictly sequential request/reply server on stdin/stdout. A request is a fixed header followed by
  exactly ``footer_len + 8`` bytes (the footer plus the 8-byte trailer, never a path). A reply is one
  fixed-size record: a code, a few small integers and an exception *type name* (never exception
  text). Nothing in a reply can carry a path, parser diagnostics or stderr.
* The worker never opens the source file, never reads the filesystem, and has no way to learn a path.
* ``parse_footer`` and ``classify_exception`` are pure functions of their inputs and are imported
  directly by the tests.

Resource containment (Linux; elsewhere the isolation and the parent's timeout still apply)
-----------------------------------------------------------------------------------------
Before each parse the soft ``RLIMIT_AS`` is set to ``<address space already mapped> + budget``, where
the parent derives ``budget`` from the declared footer length. The previous soft limit is restored
before the reply is written. ``RLIMIT_CORE`` is set to zero at start so a native abort cannot write a
core file. If the platform offers no ``resource`` module or no ``/proc/self/statm``, no address-space
limit is applied; the parent's timeout and process isolation remain.
"""
from __future__ import annotations

import os
import struct
import sys
from typing import NamedTuple, Optional

import pyarrow as pa
import pyarrow.parquet as pq

REQUEST_MAGIC = b"PQW1"
REPLY_MAGIC = b"PQR1"
#: magic, declared footer length, whole file size, address-space budget in bytes.
REQUEST_HEADER = struct.Struct("<4sQQQ")
NAME_BYTES = 32
#: magic, code, detail, problems, reserved, num_rows, num_row_groups, num_columns, exception type name.
REPLY = struct.Struct(f"<4sBBBBQQQ{NAME_BYTES}s")

TRAILER_BYTES = 8
#: Leading magic (4) + trailer (8): the smallest file for which the footer-length arithmetic holds.
MIN_PARQUET_BYTES = 12
#: Must equal ``parquet_metadata_validation.MAX_ALLOWED_FOOTER_BYTES`` (a test asserts it).
MAX_FOOTER_BYTES = 256 * 1024 * 1024
#: A budget above this is not a plausible request (1 TiB); the parent's own cap is far below it.
MAX_BUDGET_BYTES = 1 << 40
_IO_CHUNK = 1024 * 1024

# Reply codes.
CODE_VALID = 1
CODE_INVALID_PARSE = 2          # parser reported malformed content (thrift/format failure)
CODE_INVALID_STRUCTURE = 3      # footer parsed but contradicts itself; ``problems`` says how
CODE_UNSUPPORTED = 4            # parser reported an unsupported feature
CODE_RESOURCE = 5               # parser exhausted a resource; ``detail`` says which kind
CODE_UNEXPECTED = 6             # any other exception: a programming error, to be raised loudly
KNOWN_CODES = frozenset({CODE_VALID, CODE_INVALID_PARSE, CODE_INVALID_STRUCTURE, CODE_UNSUPPORTED,
                         CODE_RESOURCE, CODE_UNEXPECTED})

# ``problems`` bit flags (CODE_INVALID_STRUCTURE).
PROBLEM_ROWS_SUM_MISMATCH = 1
PROBLEM_CHUNK_OUT_OF_BOUNDS = 2
KNOWN_PROBLEM_BITS = PROBLEM_ROWS_SUM_MISMATCH | PROBLEM_CHUNK_OUT_OF_BOUNDS

# ``detail`` values (CODE_RESOURCE).
RESOURCE_ARROW_MEMORY_ERROR = 1
RESOURCE_MEMORY_ERROR = 2
RESOURCE_STD_BAD_ALLOC = 3
KNOWN_RESOURCE_KINDS = frozenset({RESOURCE_ARROW_MEMORY_ERROR, RESOURCE_MEMORY_ERROR,
                                  RESOURCE_STD_BAD_ALLOC})

#: The one OSError message that means "the native parser ran out of memory": reproduced with PyArrow
#: 24.0.0 under a constrained address space. Matched exactly (after trailing whitespace) rather than
#: by substring, so a footer that merely embeds the text in a name cannot relabel a parse failure.
BAD_ALLOC_OSERROR_MESSAGE = "Couldn't deserialize thrift: std::bad_alloc"

_ARROW_MEMORY_ERROR = getattr(pa, "ArrowMemoryError", None)

EXIT_PROTOCOL_ERROR = 3


class Outcome(NamedTuple):
    code: int
    detail: int = 0
    problems: int = 0
    num_rows: int = 0
    num_row_groups: int = 0
    num_columns: int = 0
    name: str = ""


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def classify_exception(exc: BaseException) -> Outcome:
    """Map an exception raised while parsing in-memory footer bytes to an ``Outcome``.

    Order matters: ``pa.ArrowMemoryError`` is both a ``MemoryError`` and a ``pa.ArrowException``, so
    memory exhaustion is recognised *before* the generic ``ArrowException``/``OSError`` rule that
    means "malformed content". Only exception types that are positive evidence of malformed content
    are classified as such; everything else is ``CODE_UNEXPECTED`` and must be loud.
    """
    if isinstance(exc, pa.ArrowNotImplementedError):
        return Outcome(CODE_UNSUPPORTED)
    if _ARROW_MEMORY_ERROR is not None and isinstance(exc, _ARROW_MEMORY_ERROR):
        return Outcome(CODE_RESOURCE, detail=RESOURCE_ARROW_MEMORY_ERROR)
    if isinstance(exc, MemoryError):
        return Outcome(CODE_RESOURCE, detail=RESOURCE_MEMORY_ERROR)
    if isinstance(exc, OSError) and str(exc).rstrip() == BAD_ALLOC_OSERROR_MESSAGE:
        return Outcome(CODE_RESOURCE, detail=RESOURCE_STD_BAD_ALLOC)
    if isinstance(exc, (OSError, pa.ArrowException)):
        return Outcome(CODE_INVALID_PARSE)
    return Outcome(CODE_UNEXPECTED, name=type(exc).__name__)


def _chunk_out_of_bounds(metadata, data_end: int) -> bool:
    """True only if some column chunk's byte range provably lies outside the data region
    ``[len(magic), data_end)``. Metadata arithmetic only; no data page is read."""
    for rg in range(metadata.num_row_groups):
        group = metadata.row_group(rg)
        for col in range(group.num_columns):
            chunk = group.column(col)
            starts = [o for o in (chunk.dictionary_page_offset, chunk.data_page_offset)
                      if _is_int(o) and o > 0]
            if not starts:
                continue
            start = min(starts)
            total = chunk.total_compressed_size
            if start < 4:  # len(PARQUET_MAGIC)
                return True
            if _is_int(total) and total >= 0 and start + total > data_end:
                return True
    return False


def parse_footer(window, size: int, footer_len: int) -> Outcome:
    """Parse captured footer bytes from memory. Pure; the only I/O is the parser reading ``window``.

    PyArrow's parse of an in-memory buffer cannot perform I/O, so its ``OSError`` (thrift failures)
    and ``ArrowException`` family are content evidence -- except for the recognised resource
    failures, which are classified first (see ``classify_exception``).
    """
    try:
        parsed = pq.ParquetFile(
            pa.BufferReader(window),
            thrift_string_size_limit=footer_len,
            thrift_container_size_limit=footer_len,
        )
        metadata = parsed.metadata
        num_rows, num_rgs, num_cols = metadata.num_rows, metadata.num_row_groups, metadata.num_columns
        rows_sum = sum(metadata.row_group(i).num_rows for i in range(num_rgs))
        out_of_bounds = _chunk_out_of_bounds(metadata, size - TRAILER_BYTES - footer_len)
    except Exception as exc:  # noqa: BLE001 - every exception is classified, never swallowed
        return classify_exception(exc)
    problems = 0
    if rows_sum != num_rows:
        problems |= PROBLEM_ROWS_SUM_MISMATCH
    if out_of_bounds:
        problems |= PROBLEM_CHUNK_OUT_OF_BOUNDS
    if problems:
        return Outcome(CODE_INVALID_STRUCTURE, problems=problems)
    return Outcome(CODE_VALID, num_rows=num_rows, num_row_groups=num_rgs, num_columns=num_cols)


def pack_reply(outcome: Outcome) -> bytes:
    name = outcome.name.encode("ascii", "replace")[:NAME_BYTES]
    return REPLY.pack(REPLY_MAGIC, outcome.code, outcome.detail, outcome.problems, 0,
                      outcome.num_rows, outcome.num_row_groups, outcome.num_columns, name)


def validate_header(magic: bytes, footer_len: int, size: int, budget: int) -> bool:
    """True if a request header is well-formed and within the hard bounds. Pure."""
    return (magic == REQUEST_MAGIC
            and 1 <= footer_len <= MAX_FOOTER_BYTES
            and size >= footer_len + MIN_PARQUET_BYTES
            and 0 <= budget <= MAX_BUDGET_BYTES)


class _AddressSpaceLimiter:
    """Sets the soft ``RLIMIT_AS`` to ``already-mapped + budget`` around one parse."""

    def __init__(self) -> None:
        try:
            import resource
        except ImportError:  # not POSIX: no address-space limit, isolation and timeout remain
            self._resource = None
            self._original = None
            return
        self._resource = resource
        self._original = resource.getrlimit(resource.RLIMIT_AS)

    @staticmethod
    def _mapped_bytes() -> Optional[int]:
        try:
            with open("/proc/self/statm", "rb") as handle:
                pages = int(handle.read().split()[0])
            return pages * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError, IndexError):
            return None

    def apply(self, budget: int) -> bool:
        if self._resource is None:
            return False
        mapped = self._mapped_bytes()
        if mapped is None:
            return False
        soft, hard = self._original
        wanted = mapped + budget
        if hard != self._resource.RLIM_INFINITY:
            wanted = min(wanted, hard)
        self._resource.setrlimit(self._resource.RLIMIT_AS, (wanted, hard))
        return True

    def restore(self) -> None:
        if self._resource is not None:
            try:
                self._resource.setrlimit(self._resource.RLIMIT_AS, self._original)
            except (ValueError, OSError):
                pass


def _read_exact(fd: int, count: int, *, eof_ok: bool = False) -> Optional[bytes]:
    """Exactly ``count`` bytes. ``None`` only for a clean EOF before the first byte when ``eof_ok``;
    EOF anywhere else is a protocol error."""
    parts = []
    got = 0
    while got < count:
        chunk = os.read(fd, min(_IO_CHUNK, count - got))
        if not chunk:
            if got == 0 and eof_ok:
                return None
            raise EOFError("truncated frame")
        parts.append(chunk)
        got += len(chunk)
    return b"".join(parts)


def _read_body(fd: int, count: int) -> bytearray:
    """``count`` bytes into one pre-sized buffer (no second full-size copy of the footer)."""
    body = bytearray(count)
    view = memoryview(body)
    got = 0
    while got < count:
        chunk = os.read(fd, min(_IO_CHUNK, count - got))
        if not chunk:
            raise EOFError("truncated frame")
        view[got:got + len(chunk)] = chunk
        got += len(chunk)
    view.release()
    return body


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _run_one(body: bytearray, size: int, footer_len: int, budget: int,
             limiter: _AddressSpaceLimiter) -> Outcome:
    try:
        limiter.apply(budget)
        return parse_footer(body, size, footer_len)
    except MemoryError:
        return Outcome(CODE_RESOURCE, detail=RESOURCE_MEMORY_ERROR)
    finally:
        limiter.restore()


def main() -> int:
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except (ImportError, ValueError, OSError):
        pass
    # The protocol owns a private copy of the original stdout; fd 1 itself is pointed at /dev/null so
    # nothing native code might print can corrupt a reply.
    reply_fd = os.dup(1)
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 1)
    os.close(devnull)
    limiter = _AddressSpaceLimiter()
    while True:
        try:
            header = _read_exact(0, REQUEST_HEADER.size, eof_ok=True)
            if header is None:
                return 0
            magic, footer_len, size, budget = REQUEST_HEADER.unpack(header)
            if not validate_header(magic, footer_len, size, budget):
                return EXIT_PROTOCOL_ERROR
            body = _read_body(0, footer_len + TRAILER_BYTES)
        except EOFError:
            return EXIT_PROTOCOL_ERROR
        outcome = _run_one(body, size, footer_len, budget, limiter)
        del body
        _write_all(reply_fd, pack_reply(outcome))


if __name__ == "__main__":
    sys.exit(main())
