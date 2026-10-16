from __future__ import annotations

from cdc_pipeline.windows import TumblingWindowAggregator

MIN = 60_000


def agg(**kw):
    return TumblingWindowAggregator(size_ms=MIN, max_out_of_orderness_ms=5_000, allowed_lateness_ms=MIN, **kw)


def test_window_fires_when_watermark_passes_end():
    a = agg()
    assert a.process(1_000, 10) == []
    assert a.process(30_000, 5) == []
    out = a.process(MIN + 6_000, 1)  # watermark = 61_000 > 60_000
    assert [(r.window_start, r.count, r.total, r.is_update) for r in out] == [(0, 2, 15.0, False)]


def test_out_of_order_within_bound_is_counted_before_firing():
    a = agg()
    a.process(59_000, 1)
    a.process(62_000, 1)    # watermark 57_000: window [0, 60s) still open
    a.process(58_000, 1)    # out of order but before watermark passes end
    out = a.process(70_000, 1)
    assert out[0].count == 2 and not out[0].is_update


def test_late_event_within_allowed_lateness_emits_correction():
    a = agg()
    a.process(10_000, 10)
    a.process(70_000, 1)    # fires window 0
    out = a.process(20_000, 5)
    assert len(out) == 1 and out[0].is_update and out[0].total == 15.0 and out[0].count == 2


def test_too_late_event_goes_to_side_output_not_dropped():
    a = agg()
    a.process(10_000, 10)
    a.process(3 * MIN, 1)   # watermark 175s: window 0 (end 60s + lateness 60s) is closed
    out = a.process(20_000, 99)
    assert out == []
    assert len(a.late_events) == 1 and a.late_events[0].value == 99


def test_state_is_cleaned_up():
    a = agg()
    for minute in range(10):
        a.process(minute * MIN + 1, 1)
    # Only windows still eligible for corrections are retained.
    assert a.open_windows <= 3
