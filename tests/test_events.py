from __future__ import annotations

import json

import pytest

from cdc_pipeline.events import Op, PoisonMessage, parse

from .helpers import PK, envelope


@pytest.mark.parametrize("op", ["c", "u", "r"])
def test_upserts_use_after_image(op):
    e = parse(envelope(op, order_id=5, lsn=123, status="paid"), PK)
    assert e.op is Op(op) and e.op.is_upsert
    assert e.table == "public.orders"
    assert e.key == (5,)
    assert e.row["status"] == "paid"
    assert e.version == 123


def test_delete_uses_before_image():
    e = parse(envelope("d", order_id=9), PK)
    assert e.op is Op.DELETE and e.key == (9,)


def test_schema_wrapped_envelope():
    assert parse(envelope("c", schema_wrapped=True), PK).key == (1,)


def test_tombstone_is_ignored():
    assert parse(None, PK) is None


@pytest.mark.parametrize(
    "raw,reason",
    [
        (b"{not json", "not valid JSON"),
        (b"[1, 2]", "not an object"),
        (json.dumps({"op": "t", "source": {"table": "orders", "lsn": 1}}).encode(), "unsupported op"),
        (json.dumps({"op": "c", "source": {"table": "payments", "lsn": 1}, "after": {}}).encode(), "no primary key"),
        (json.dumps({"op": "c", "source": {"table": "orders", "lsn": 1}, "after": None}).encode(), "without after"),
        (json.dumps({"op": "c", "source": {"table": "orders"}, "after": {"order_id": 1}}).encode(), "missing source.lsn"),
        (json.dumps({"op": "c", "source": {"table": "orders", "lsn": 1}, "after": {"x": 1}}).encode(), "primary key"),
    ],
)
def test_poison_messages(raw, reason):
    with pytest.raises(PoisonMessage, match=reason):
        parse(raw, PK)
