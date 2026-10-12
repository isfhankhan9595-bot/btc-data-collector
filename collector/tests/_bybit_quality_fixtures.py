"""Shared fixture helper for tests that need Bybit's quality writer to publish one row at a time.

Bybit batches quality rows in production (P0-3: ``run_bybit_collector.QUALITY_SEGMENT_ROWS`` rows /
``QUALITY_SEGMENT_SECONDS`` s). Tests whose subject is WAL / checkpoint / failure semantics at the
publication boundary want that boundary to fire at ``write()`` time, i.e. one-row segments.

The constant MUST be in force while the app is CONSTRUCTED. Setting ``app.quality_writer.segment_rows
= 1`` afterwards is wrong: the constructor has already generated storage events and replayed any WAL
backlog under production batching, so those rows sit buffered instead of published.

The override is restored in ``finally``; the writer keeps the value it was constructed with, so the
app under test stays one-row after the scope exits while nothing else in the process is affected.
"""
from __future__ import annotations

import contextlib

import collector.run_bybit_collector as rbc


@contextlib.contextmanager
def one_row_bybit_quality_segments():
    saved = rbc.QUALITY_SEGMENT_ROWS
    rbc.QUALITY_SEGMENT_ROWS = 1
    try:
        yield
    finally:
        rbc.QUALITY_SEGMENT_ROWS = saved
