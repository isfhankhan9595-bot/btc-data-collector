"""Typed fatal-storage exception taxonomy (F5).

This is a deliberate LEAF module: it imports nothing from the package so that
``parquet_writer``, ``segment_dedup``, ``raw_capture``, ``websocket_client`` and
the runners can all share one exception type without an import cycle.

Why a dedicated type
--------------------
Classification of "this storage failure means the writer can no longer vouch
for its stream" MUST be ``isinstance(exc, FatalStorageError)`` -- never a test
on ``RuntimeError`` (far too broad: closed-writer refusals and ordinary
handler bugs are ``RuntimeError`` too), never a substring of the message
("FAILED"), never a class-name string. A bare ``OSError`` is NOT inherited
either: an ``OSError`` raised by an ordinary handler must stay ordinary, and a
fatal storage failure must never be caught by an ``except OSError`` written for
a recoverable I/O condition elsewhere.

What the fields mean
--------------------
``stream``      the writer's storage stream (``raw_wire``, ``trades``, ...).
``component``   which part of the storage stack failed (``parquet_writer``,
                ``dedup``).
``stage``       the publication stage that failed (``flush``, ``rename``,
                ``dir_fsync``, ``open_next_segment``, ``publication_hook``,
                ``marker``, ``dedup_state`` ...).
``durability``  what is known about the rows involved: ``unpublished``,
                ``unconfirmed``, ``published`` (durable, but the writer can
                no longer continue) or ``None`` when not applicable.

``record_accepted``  set ONLY by ``ParquetWriter.write``: whether the record
                that call was writing had been appended to a segment buffer
                before the failure was raised (``True``), or the call failed
                before the record was admitted anywhere (``False``). ``None``
                means no such evidence exists (an exception that did not come
                from ``write``). ``stage`` and ``durability`` describe the
                WRITER's segment, not this record: an ``open_next_segment``
                failure raised on an hour rollover is "published" for the
                previous segment while the triggering record was never
                written. ``trigger_event_disposition`` combines both.

The original cause is chained (``raise FatalStorageError(...) from exc``), so
the stage-specific evidence (``OSError(ENOSPC)`` and so on) is preserved on
``__cause__``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

__all__ = ["FatalStorageError", "WriterFailureSnapshot",
           "EVENT_PUBLISHED", "EVENT_UNCONFIRMED", "EVENT_LOST"]

#: Disposition of the single record whose ``write`` raised (see
#: ``FatalStorageError.trigger_event_disposition``).
EVENT_PUBLISHED = "published"      # its row is in a durably published segment
EVENT_UNCONFIRMED = "unconfirmed"  # durability could not be established either way
EVENT_LOST = "lost"                # demonstrably not durable (never admitted, or in an unpublished segment)


class FatalStorageError(RuntimeError):
    """A storage-correctness failure: the affected writer is FAILED.

    Not an ``OSError`` (see module docstring). A subclass of ``RuntimeError``
    only so that pre-F5 callers that already handled a failed writer's
    refusal as ``RuntimeError`` keep working; NO classification may rely on
    that relationship.
    """

    def __init__(self, *args: object, stream: Optional[str] = None,
                 component: Optional[str] = None, stage: Optional[str] = None,
                 durability: Optional[str] = None) -> None:
        super().__init__(*args)
        self.stream = stream
        self.component = component
        self.stage = stage
        self.durability = durability
        self.record_accepted: Optional[bool] = None

    def trigger_event_disposition(self) -> str:
        """What is PROVEN about the record whose ``write`` raised this.

        Never claims more than the evidence supports:

        * ``record_accepted is False`` -> ``lost``: the record was never admitted
          to any segment, whatever ``stage``/``durability`` say about the writer's
          earlier segment (a rollover failure, or a refusal by an already-failed
          writer).
        * accepted + durability ``published`` -> ``published``: the segment that
          holds the record was durably published before the writer failed (e.g.
          ``open_next_segment`` after a full segment).
        * accepted + ``unpublished`` -> ``lost`` (segment never renamed into place).
        * accepted + ``unconfirmed``, or any unrecognised durability -> ``unconfirmed``.
        * no ``record_accepted`` evidence at all -> ``unconfirmed``: nothing is
          fabricated in either direction.
        """
        if self.record_accepted is None:
            return EVENT_UNCONFIRMED
        if self.record_accepted is False:
            return EVENT_LOST
        if self.durability == "published":
            return EVENT_PUBLISHED
        if self.durability == "unpublished":
            return EVENT_LOST
        return EVENT_UNCONFIRMED

    def describe(self) -> dict:
        """Structured, JSON-safe view for logs, alerts and failure records."""
        cause = self.__cause__
        return {
            "stream": self.stream,
            "component": self.component,
            "stage": self.stage,
            "durability": self.durability,
            "error": f"{type(self).__name__}: {self}",
            "cause": None if cause is None else f"{type(cause).__name__}: {cause}",
        }


@dataclass(frozen=True)
class WriterFailureSnapshot:
    """Read-only view of a writer's latched failure (never a repair handle).

    Lets a supervisor notice a latched failure while the writer is quiet, i.e.
    without waiting for the next ``write()`` to raise. Holding one confers no
    ability to clear the latch.
    """

    stream: str
    component: str
    stage: Optional[str]
    durability: Optional[str]
    #: ``"storage"`` (segment publication did not complete) or ``"publication"``
    #: (segment durable, but its marker / dedup hook failed).
    kind: str
    error: str
