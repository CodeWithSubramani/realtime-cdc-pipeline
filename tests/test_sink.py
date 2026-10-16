"""The sink must converge to the same state regardless of duplicates, reordering or replays."""

from __future__ import annotations

import itertools
import sqlite3

import pytest

from cdc_pipeline.batching import compact
from cdc_pipeline.events import parse
from cdc_pipeline.sink import SqlSink, TableSpec

from .helpers import PK, envelope

ORDERS = TableSpec(
    source_table="public.orders",
    target_table="orders_current",
    primary_key=("order_id",),
    columns=(("order_id", "BIGINT"), ("customer_id", "BIGINT"), ("status", "TEXT"),
             ("amount", "NUMERIC(12, 2)"), ("currency", "TEXT"), ("created_at", "BIGINT"), ("updated_at", "BIGINT")),
)


@pytest.fixture
def sink():
    conn = sqlite3.connect(":memory:")
    s = SqlSink(conn, [ORDERS])
    s.create_tables()
    yield s
    conn.close()


def state(sink):
    return sink.conn.execute(
        "SELECT order_id, status, _lsn, _deleted FROM orders_current ORDER BY order_id"
    ).fetchall()


def ev(op, lsn, **kw):
    return parse(envelope(op, lsn=lsn, **kw), PK)


def test_insert_update_delete_lifecycle(sink):
    sink.apply([ev("c", 10), ev("u", 20, status="paid")])
    assert state(sink) == [(1, "paid", 20, 0)]
    sink.apply([ev("d", 30, status="paid")])
    assert state(sink) == [(1, "paid", 30, 1)]  # soft delete keeps last image


def test_out_of_order_older_event_is_ignored(sink):
    sink.apply([ev("u", 20, status="shipped")])
    changed = sink.apply([ev("u", 15, status="paid")])
    assert changed == 0
    assert state(sink) == [(1, "shipped", 20, 0)]


def test_replay_is_idempotent(sink):
    batch = [ev("c", 10), ev("u", 20, status="paid")]
    sink.apply(batch)
    assert sink.apply(batch) == 0
    assert state(sink) == [(1, "paid", 20, 0)]


def test_late_insert_cannot_resurrect_deleted_row(sink):
    sink.apply([ev("d", 50)])
    sink.apply([ev("c", 10)])
    assert state(sink)[0][3] == 1


def test_every_permutation_converges(sink):
    events = [ev("c", 10), ev("u", 20, status="paid"), ev("u", 30, status="shipped"), ev("d", 40, status="shipped")]
    expected = None
    for perm in itertools.permutations(events):
        sink.conn.execute("DELETE FROM orders_current")
        for e in perm:  # one event per transaction: worst case interleaving
            sink.apply([e])
        result = state(sink)
        expected = expected or result
        assert result == expected
    assert expected == [(1, "shipped", 40, 1)]


def test_failed_batch_rolls_back_entirely(sink):
    class Boom(Exception):
        pass

    good = ev("c", 10, order_id=1)

    def exploding():
        yield good
        raise Boom()

    with pytest.raises(Boom):
        sink.apply(exploding())
    assert state(sink) == []


def test_unreplicated_tables_are_skipped(sink):
    other = parse(envelope("c"), {"public.orders": ["order_id"]})
    object.__setattr__(other, "table", "public.audit_log")
    assert sink.apply([other]) == 0


def test_compaction_keeps_latest_per_key():
    events = [ev("c", 10, order_id=1), ev("u", 30, order_id=1, status="paid"),
              ev("u", 20, order_id=1, status="x"), ev("c", 15, order_id=2)]
    out = compact(events)
    assert [(e.key, e.lsn) for e in out] == [((2,), 15), ((1,), 30)]


@pytest.mark.parametrize("bad", ["orders; DROP TABLE x", "Robert'); --"])
def test_identifiers_are_validated(bad):
    with pytest.raises(ValueError):
        SqlSink(sqlite3.connect(":memory:"),
                [TableSpec("public.orders", bad, ("order_id",), (("order_id", "BIGINT"),))])
