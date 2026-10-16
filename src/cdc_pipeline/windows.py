"""Event-time tumbling windows with watermarks and allowed lateness.

Used for the real-time "revenue per minute" metric. Semantics mirror Flink/Spark
Structured Streaming so the trade-offs are explicit:

* **Watermark** = max event time seen - ``max_out_of_orderness``. It is the processor's
  claim that "no event older than this will arrive (in the normal case)".
* A window ``[start, end)`` is **emitted** once the watermark passes ``end``.
* Events for an already-emitted window that are within ``allowed_lateness`` produce an
  **updated** result (a correction — downstream must upsert by window key).
* Events later than that are **not dropped silently**: they go to a side output so they
  can be reconciled by the batch layer (the nightly job is the source of truth).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class WindowResult:
    window_start: int
    window_end: int
    count: int
    total: float
    is_update: bool  # True when this re-emits a window that was already published


@dataclass(frozen=True)
class LateEvent:
    event_time: int
    value: float
    watermark: int


@dataclass
class TumblingWindowAggregator:
    size_ms: int
    max_out_of_orderness_ms: int = 5_000
    allowed_lateness_ms: int = 60_000

    _windows: dict[int, list[float]] = field(default_factory=dict)  # start -> [count, total]
    _emitted: set[int] = field(default_factory=set)
    _max_event_time: int | None = None
    late_events: list[LateEvent] = field(default_factory=list)

    @property
    def watermark(self) -> int | None:
        if self._max_event_time is None:
            return None
        return self._max_event_time - self.max_out_of_orderness_ms

    def _start(self, ts: int) -> int:
        return ts - (ts % self.size_ms)

    def process(self, event_time: int, value: float) -> list[WindowResult]:
        """Ingest one event; return any window results that became final or were corrected."""
        start = self._start(event_time)
        end = start + self.size_ms
        wm = self.watermark
        out: list[WindowResult] = []

        if wm is not None and end + self.allowed_lateness_ms <= wm:
            self.late_events.append(LateEvent(event_time, value, wm))
            return out

        acc = self._windows.setdefault(start, [0, 0.0])
        acc[0] += 1
        acc[1] += value

        if start in self._emitted:  # late but within allowed lateness -> correction
            out.append(WindowResult(start, end, int(acc[0]), round(acc[1], 2), is_update=True))

        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
        out.extend(self._fire())
        return out

    def _fire(self) -> list[WindowResult]:
        wm = self.watermark
        out = []
        for start in sorted(self._windows):
            end = start + self.size_ms
            if wm is None or end > wm:
                break
            if start not in self._emitted:
                c, t = self._windows[start]
                out.append(WindowResult(start, end, int(c), round(t, 2), is_update=False))
                self._emitted.add(start)
        # State cleanup: drop windows that can no longer receive corrections.
        for start in [s for s in self._windows if s + self.size_ms + self.allowed_lateness_ms <= (wm or 0)]:
            self._windows.pop(start)
            self._emitted.discard(start)
        return out

    def process_all(self, events: Iterable[tuple[int, float]]) -> list[WindowResult]:
        results: list[WindowResult] = []
        for ts, v in events:
            results.extend(self.process(ts, v))
        return results

    @property
    def open_windows(self) -> int:
        return len(self._windows)
