"""Micro-batch compaction.

A hot row can change hundreds of times per second. Within one poll batch only the
highest-LSN event per key matters for a *current-state* table, so we collapse the batch
before touching the database. Correctness doesn't depend on this (the sink's version
guard does that); it's purely a write-amplification optimisation.
"""

from __future__ import annotations

from typing import Iterable

from .events import ChangeEvent


def compact(events: Iterable[ChangeEvent]) -> list[ChangeEvent]:
    latest: dict[tuple[str, tuple], ChangeEvent] = {}
    for e in events:
        k = (e.table, e.key)
        cur = latest.get(k)
        if cur is None or e.version > cur.version:
            latest[k] = e
    # Deterministic order (by WAL position) keeps logs and tests readable.
    return sorted(latest.values(), key=lambda e: e.version)
