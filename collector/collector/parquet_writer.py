"""Crash-bounded raw parquet storage."""
from __future__ import annotations

import json
import os
import time
from datetime import date as _date, datetime, timezone
from pathlib import Path
from typing import IO, Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .numeric import column_value
from .utils import logger
from .storage_layout import (
    SegmentKind, check_stream_namespace, iter_segments, parse_segment_name, segment_path,
)

try:  # POSIX advisory locks. Absent on Windows; see _acquire_writer_lock.
    import fcntl
except ImportError:  # pragma: no cover - the collector deploys on Linux
    fcntl = None  # type: ignore[assignment]

#: Held (exclusively, for the writer's lifetime) inside every stream directory.
#: Not a segment name, so ``parse_segment_name`` and every reader ignore it.
WRITER_LOCK_FILENAME = ".writer.lock"


class StorageWriterLockedError(RuntimeError):
    """Another live writer already owns this stream directory."""


def _acquire_writer_lock(stream_dir: Path) -> Optional[IO[bytes]]:
    """Take the stream directory's exclusive writer lock, or raise.

    Sequence allocation is scan-then-create: it reads the directory, picks
    ``max + 1``, and only later opens ``<seg>.tmp``. Nothing reserves that
    number, so two live writers on one directory pick the same one and open
    the *same* ``.tmp`` path -- interleaving two parquet streams into one
    file. Worse, ``_recover_orphans`` deletes every ``*.seg.tmp`` it finds,
    which from a second process is the first process's live segment, and then
    reports a DATA_DROP for it. Separate stream names remove the *expected*
    collision; this lock makes the unexpected one (a duplicate service start,
    a mis-named stream) fail loudly before it can damage anything.

    ``flock`` is released by the kernel when the holder dies, so a SIGKILLed
    collector never leaves the next one locked out (restart-safe). It is held
    on an open-file object so an abandoned writer releases it on collection.
    """
    if fcntl is None:  # pragma: no cover
        logger.warning("storage_writer_lock_unavailable", stream_dir=str(stream_dir),
                       reason="fcntl not available on this platform; single-writer is unenforced")
        return None
    handle = open(stream_dir / WRITER_LOCK_FILENAME, "a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            handle.seek(0)
            holder = handle.read().decode("utf-8", "replace").strip() or "unknown"
        except OSError:
            holder = "unknown"
        handle.close()
        raise StorageWriterLockedError(
            f"stream directory {stream_dir} is already being written by another live "
            f"writer ({holder}); two writers on one stream would share segment "
            f"sequence numbers and .tmp files"
        ) from None
    except BaseException:
        handle.close()
        raise
    try:  # diagnostic only: tells a refused writer who holds the stream
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n".encode("ascii"))
        handle.flush()
    except OSError:
        pass
    return handle


def _epoch_ms(value: Any) -> Optional[int]:
    """Normalise a schema-legal timestamp value to epoch milliseconds.

    The stream schemas declare ``timestamp`` as ``pa.timestamp("ms", tz="UTC")``,
    so PyArrow legitimately accepts ints, datetimes and ``pandas.Timestamp``
    objects for that column. Segment metadata is JSON, which accepts none of the
    datetime forms. Normalising here keeps a schema-legal record from raising
    inside ``_close_segment`` and destroying an otherwise publishable segment.

    Unknown types return ``None`` (metadata records absence) rather than raising:
    losing a metadata hint must never cost us the segment itself.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return None if value != value else int(value)  # NaN -> None
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(moment.timestamp() * 1000)
    if isinstance(value, _date):
        return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)
    # pandas.Timestamp / numpy.datetime64 and anything else exposing a
    # conversion protocol, without importing pandas into the hot path.
    for attribute in ("to_pydatetime", "item"):
        converter = getattr(value, attribute, None)
        if callable(converter):
            try:
                converted = converter()
            except (ValueError, TypeError, OverflowError):
                return None
            if isinstance(converted, datetime):
                return _epoch_ms(converted)
            if isinstance(converted, int):
                return converted
    return None


class ParquetWriter:
    """Write immutable, atomically-published Parquet segments for one stream."""

    def __init__(
        self, stream_name: str, schema: pa.Schema, base_dir: str = "data", *,
        segment_rows: int = 5_000, segment_seconds: float = 30.0,
        quality_event_sink: Optional[Callable[[Dict[str, Any]], None]] = None,
        exchange: str = "BINANCE",
        on_segment_published: Optional[Callable[[Tuple[str, int], Path], None]] = None,
        on_segment_durable: Optional[Callable[[Path, int], None]] = None,
        age_from_first_row: bool = False,
    ) -> None:
        if segment_rows <= 0 or segment_seconds <= 0:
            raise ValueError("segment_rows and segment_seconds must be positive")
        check_stream_namespace(exchange, stream_name)
        self.stream_name, self.schema, self.base_dir = stream_name, schema, base_dir
        #: Attributed on every quality event this writer emits about itself
        #: (crashed segments, data drops). Defaults to BINANCE because every
        #: writer in this codebase was one until Bybit's; a writer created
        #: for another venue must say so, or its own storage failures are
        #: durably misattributed to a venue that did not cause them.
        self.exchange = exchange
        self.stream_dir = Path(base_dir) / "raw" / stream_name
        self.stream_dir.mkdir(parents=True, exist_ok=True)
        self._closed = False
        #: P0-4: called after a segment is durably published (fsync+rename+dir
        #: fsync) with ``((hour, seq), final_path)``. None = no behaviour change.
        self.on_segment_published = on_segment_published
        #: P0-3, deliberately a DIFFERENT API from ``on_segment_published``
        #: above (different signature ``(final_path, record_count)``, different
        #: failure contract). Called once per segment, only after the segment
        #: is durably published: fsync'd, atomically renamed, and the parent
        #: directory fsync'd. Never called for a segment whose publication
        #: raised. A hook failure is logged and NEVER un-publishes the segment
        #: and NEVER poisons the writer (unlike ``on_segment_published``): the
        #: hook's owner must latch its own failure state.
        self.on_segment_durable = on_segment_durable
        #: When True, ``segment_seconds`` is measured from the segment's first
        #: row instead of from when the (possibly long-idle) segment was
        #: opened. Without it, the first event after a quiet spell publishes
        #: as a 1-row file -- the tiny-file pattern for sparse streams.
        self._age_from_first_row = age_from_first_row
        self._first_row_monotonic: Optional[float] = None
        self._publication_failure: Optional[BaseException] = None
        self._lock_handle: Optional[IO[bytes]] = _acquire_writer_lock(self.stream_dir)
        try:
            self.segment_rows, self.segment_seconds = segment_rows, segment_seconds
            self.quality_event_sink = quality_event_sink
            self.buffer: List[Dict[str, Any]] = []
            self.current_hour = self._get_current_hour_str()
            self.writer: Optional[pq.ParquetWriter] = None
            self._tmp_filepath: Optional[Path] = None
            self._counter_filepath: Optional[Path] = None
            self._segment_opened_monotonic = time.monotonic()
            self.record_count = 0
            self._first_record_ts: Optional[int] = None
            self._last_record_ts: Optional[int] = None
            self._sequence_cache: dict[str, int] = {}
            self._seq = self._next_sequence(self.current_hour)
            self._recover_orphans()
            self._open_segment()
        except BaseException:
            # A writer that never finished constructing must not keep the
            # directory locked (e.g. a refused legacy/segment collision).
            self._release_lock()
            raise

    def _release_lock(self) -> None:
        handle, self._lock_handle = self._lock_handle, None
        if handle is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def _get_current_hour_str(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")

    def _emit_quality(self, event_type: str, reason: str) -> None:
        event = {
            "exchange": self.exchange,
            "stream": self.stream_name,
            "event_type": event_type,
            "reason": reason,
            "gap_size_ms": None,
            "rows_lost": None,
            "local_ts": int(time.time() * 1000),
        }
        logger.warning("storage_quality_event", **event)
        if self.quality_event_sink:
            self.quality_event_sink(event)

    def _next_sequence(self, hour: str) -> int:
        """Return the next unused segment sequence for ``hour``.

        A single directory scan populates the cache for *every* logical hour
        present on disk. The previous implementation rescanned the stream
        directory on each hour rollover, which put an O(files) scan on the
        ingest path of a long-running collector.

        Collision semantics are preserved and kept hour-local: an
        unresolvable legacy/segment collision in some unrelated hour must not
        stop us writing the current hour, so such hours are left uncached and
        raise only when they are actually requested.
        """
        cached = self._sequence_cache.get(hour)
        if cached is not None:
            return cached

        date, hour_number = hour.rsplit("-", 1)
        requested = (date, int(hour_number))

        # One pass over the stream directory, grouped by logical hour.
        max_sequence: dict[tuple[str, int], int] = {}
        has_legacy: dict[tuple[str, int], bool] = {}
        # Derived rather than read from self.stream_dir: sequence allocation
        # runs during __init__ and must not depend on attribute ordering.
        stream_dir = Path(self.base_dir) / "raw" / self.stream_name
        if stream_dir.exists():
            for path in stream_dir.iterdir():
                if not path.is_file():
                    continue
                parsed = parse_segment_name(path)
                if parsed is None:
                    continue
                row_date, row_hour, sequence, kind = parsed
                key = (row_date, row_hour)
                if kind is SegmentKind.LEGACY_HOURLY:
                    has_legacy[key] = True
                elif sequence is not None:
                    current = max_sequence.get(key)
                    if current is None or sequence > current:
                        max_sequence[key] = sequence

        for key in set(max_sequence) | set(has_legacy):
            hour_key = f"{key[0]}-{key[1]:02d}"
            legacy_present = has_legacy.get(key, False)
            highest = max_sequence.get(key)

            if legacy_present and highest is not None:
                # Ambiguous logical hour. Never guess: defer to iter_segments,
                # which is the single authority on collision policy. Only the
                # hour actually being requested is allowed to raise here.
                if key == requested:
                    list(
                        iter_segments(
                            self.base_dir,
                            self.stream_name,
                            date=key[0],
                            hour=key[1],
                            on_collision="raise",
                        )
                    )
                continue

            if highest is not None:
                self._sequence_cache[hour_key] = highest + 1
            elif legacy_present:
                self._sequence_cache[hour_key] = 1
                if key == requested:
                    self._emit_quality(
                        "STORAGE_MIGRATION",
                        "legacy_hourly_file_present; starting sequenced writer at 1",
                    )

        # An hour with nothing on disk starts at sequence 0.
        return self._sequence_cache.setdefault(hour, 0)

    def _segment_paths(self) -> tuple[Path, Path, Path]:
        final = segment_path(self.base_dir, self.stream_name, self.current_hour, self._seq)
        return final, Path(str(final) + ".tmp"), Path(str(final) + ".count.json")

    def _get_filename(self, hour_str: str) -> str:
        return str(segment_path(self.base_dir, self.stream_name, hour_str, self._seq))

    def _emit_drop(self, rows_lost: Optional[int], reason: str = "crashed_segment_discarded") -> None:
        event = {"exchange": self.exchange, "stream": self.stream_name, "event_type": "DATA_DROP",
                 "reason": reason, "gap_size_ms": None, "rows_lost": rows_lost,
                 "local_ts": int(time.time() * 1000)}
        logger.warning("segment_data_drop", **event)
        if self.quality_event_sink:
            self.quality_event_sink(event)

    def _recover_orphans(self) -> None:
        for meta_tmp in self.stream_dir.glob("*.meta.json.tmp"):
            try:
                meta_tmp.unlink()
            except OSError as exc:
                logger.error("metadata_orphan_discard_failed", file=str(meta_tmp), error=str(exc))
        for tmp in self.stream_dir.glob("*.seg.tmp"):
            count_path = Path(str(tmp).removesuffix(".tmp") + ".count.json")
            rows: Optional[int] = None
            try:
                with count_path.open(encoding="utf-8") as handle:
                    rows = int(json.load(handle).get("rows"))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                rows = None
            try:
                tmp.unlink()
                count_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.error("segment_orphan_discard_failed", file=str(tmp), error=str(exc))
                continue
            if rows is None or rows > 0:
                self._emit_drop(rows)

    def _open_segment(self) -> None:
        final, tmp, counter = self._segment_paths()
        if final.exists():
            raise FileExistsError(f"refusing to overwrite published segment: {final}")
        self._tmp_filepath, self._counter_filepath = tmp, counter
        self.writer = pq.ParquetWriter(str(tmp), self.schema, compression="snappy")
        self.record_count = 0
        self._first_record_ts = self._last_record_ts = None
        self._segment_opened_monotonic = time.monotonic()
        self._first_row_monotonic = None

    def _segment_age(self) -> float:
        start = self._segment_opened_monotonic
        if self._age_from_first_row:
            if self._first_row_monotonic is None:
                return 0.0
            start = self._first_row_monotonic
        return time.monotonic() - start

    def _persist_counter(self) -> None:
        assert self._counter_filepath is not None
        temporary = Path(str(self._counter_filepath) + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump({"rows": self.record_count}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self._counter_filepath)

    def current_segment_token(self) -> Tuple[str, int]:
        return (self.current_hour, self._seq)

    def write(self, record: Dict[str, Any], *,
              bind: Optional[Callable[[Tuple[str, int]], None]] = None) -> None:
        """Append ``record``. ``bind(token)``, if given, is called with the
        token of the segment that will receive the record -- after any hour
        rollover, before the append -- so a caller can attribute the record to
        its segment exactly (a rotation after the append cannot race it)."""
        if self._publication_failure is not None:
            raise RuntimeError(
                f"ParquetWriter for {self.stream_name!r} refuses writes: the segment-publication "
                f"hook failed ({self._publication_failure!r}); dedup/storage state is uncertain"
            ) from self._publication_failure
        hour = self._get_current_hour_str()
        if hour != self.current_hour:
            if self._closed:
                # close() released the stream lock. Rolling over would open a
                # new segment in a directory this writer no longer owns.
                raise RuntimeError(
                    f"ParquetWriter for {self.stream_name!r} is closed and no longer owns "
                    f"its stream directory; refusing to open a new segment")
            # Not close(): the writer keeps its stream directory across an
            # hour rollover, so the lock must stay held.
            self._finalize_segment()
            # _finalize_segment can set _publication_failure (the just-closed
            # segment's publication hook can fail) -- re-check immediately,
            # before this record is bound/appended to the newly-opened
            # segment. The check at the top of write() ran before this
            # rollover happened, so it cannot see a failure caused by this
            # same call; without this second check, the triggering record
            # would be silently admitted into a writer that is already in a
            # fail-closed state, one write late.
            if self._publication_failure is not None:
                raise RuntimeError(
                    f"ParquetWriter for {self.stream_name!r} refuses this write: the segment-"
                    f"publication hook failed during this write's own hour rollover "
                    f"({self._publication_failure!r}); dedup/storage state is uncertain"
                ) from self._publication_failure
            self.current_hour, self._seq = hour, self._next_sequence(hour)
            self._open_segment()
        if bind is not None:
            bind((self.current_hour, self._seq))
        self.buffer.append(record)
        if self._first_row_monotonic is None:
            self._first_row_monotonic = time.monotonic()
        ts = _epoch_ms(record.get("timestamp"))
        if self._first_record_ts is None:
            self._first_record_ts = ts
        self._last_record_ts = ts
        if (len(self.buffer) >= 1_000 or self.record_count + len(self.buffer) >= self.segment_rows
                or self._segment_age() >= self.segment_seconds):
            self.flush()
        if self.record_count >= self.segment_rows or self._segment_age() >= self.segment_seconds:
            self._close_segment(open_next=True)

    def flush(self) -> None:
        if not self.buffer:
            return
        assert self.writer is not None
        columns = {field.name: [column_value(field.name, record) for record in self.buffer] for field in self.schema}
        self.writer.write_table(pa.Table.from_pydict(columns, schema=self.schema))
        self.record_count += len(self.buffer)
        self.buffer.clear()
        self._persist_counter()

    def _close_segment(self, *, open_next: bool) -> None:
        if self.writer is None:
            return
        self.flush()
        final, tmp, counter = self._segment_paths()
        self.writer.close()
        self.writer = None
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        if final.exists():
            raise FileExistsError(f"refusing to overwrite published segment: {final}")
        os.replace(tmp, final)
        parent_fd = os.open(str(final.parent), os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        counter.unlink(missing_ok=True)
        # P0-3: durable-publication callback. Placed strictly AFTER the
        # rename and the directory fsync -- the only point at which a caller
        # may treat the rows of this segment as durable.
        if self.on_segment_durable is not None:
            try:
                self.on_segment_durable(final, self.record_count)
            except Exception as exc:  # noqa: BLE001 - segment is durable; the owner latches its own state
                logger.error("segment_durable_hook_failed", file=str(final), error=f"{type(exc).__name__}: {exc}")
        # The segment is already durably published above. A metadata failure
        # must therefore never propagate: it would kill the ingest task over a
        # sidecar hint while the data itself is safely on disk. Surface it as a
        # durable quality event instead of silently swallowing it.
        meta = Path(str(final) + ".meta.json")
        meta_tmp = Path(str(meta) + ".tmp")
        try:
            with meta_tmp.open("w", encoding="utf-8") as handle:
                json.dump({"record_count": self.record_count, "first_record_ts": self._first_record_ts,
                           "last_record_ts": self._last_record_ts}, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(meta_tmp, meta)
            parent_fd = os.open(str(final.parent), os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        except (OSError, TypeError, ValueError) as exc:
            meta_tmp.unlink(missing_ok=True)
            self._emit_quality(
                "STORAGE_METADATA_FAILED",
                f"segment published but metadata sidecar failed: {type(exc).__name__}: {exc}",
            )
        logger.info("closed_parquet_segment", stream=self.stream_name, file=str(final), rows=self.record_count)
        if self.on_segment_published is not None:
            try:
                self.on_segment_published((self.current_hour, self._seq), final)
            except Exception as exc:  # noqa: BLE001 - segment is durable; fail CLOSED on the next write
                self._publication_failure = exc
                self._emit_quality("DEDUP_STATE_FAILED",
                                   f"segment published but publication hook failed: {type(exc).__name__}: {exc}")
        self._sequence_cache[self.current_hour] = self._seq + 1
        if open_next:
            self._seq += 1
            self._open_segment()

    def has_unpublished_rows(self) -> bool:
        return self.writer is not None and (self.record_count > 0 or bool(self.buffer))

    def publish_open_segment(self) -> None:
        """Publish the open segment now (if it holds rows) and open the next.
        A no-op when nothing is pending, so an idle stream never produces an
        empty segment. Raises if publication fails (nothing is claimed durable)."""
        if not self.has_unpublished_rows():
            return
        self._close_segment(open_next=True)

    def publish_if_due(self) -> bool:
        """Time-based publish. ``segment_seconds`` is otherwise only evaluated
        inside write(); there is no background timer, so a stream that goes
        idle would keep its tail in RAM indefinitely. A caller with an idle
        tick calls this. Returns True if a segment was published."""
        if self.has_unpublished_rows() and self._segment_age() >= self.segment_seconds:
            self._close_segment(open_next=True)
            return True
        return False

    def close(self) -> None:
        """Publish the open segment and release the stream directory lock."""
        try:
            self._finalize_segment()
        finally:
            self._closed = True
            self._release_lock()

    def _finalize_segment(self) -> None:
        if self.writer is None:
            return
        if self.record_count == 0 and not self.buffer:
            self.writer.close()
            self.writer = None
            if self._tmp_filepath:
                self._tmp_filepath.unlink(missing_ok=True)
            if self._counter_filepath:
                self._counter_filepath.unlink(missing_ok=True)
            return
        self._close_segment(open_next=False)
