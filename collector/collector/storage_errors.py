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

The original cause is chained (``raise FatalStorageError(...) from exc``), so
the stage-specific evidence (``OSError(ENOSPC)`` and so on) is preserved on
``__cause__``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

__all__ = ["FatalStorageError", "WriterFailureSnapshot"]


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
