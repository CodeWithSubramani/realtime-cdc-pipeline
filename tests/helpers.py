from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

PK = {"public.orders": ["order_id"], "public.customers": ["customer_id"]}


def envelope(op: str, *, order_id: int = 1, lsn: int = 100, status: str = "placed",
             amount: str = "10.00", schema_wrapped: bool = False, **extra: Any) -> bytes:
    row = {"order_id": order_id, "customer_id": 7, "status": status, "amount": amount,
           "currency": "INR", "created_at": 1_700_000_000_000, "updated_at": 1_700_000_000_000 + lsn}
    row.update(extra)
    value = {
        "before": row if op in ("u", "d") else None,
        "after": None if op == "d" else row,
        "source": {"schema": "public", "table": "orders", "lsn": lsn, "txId": lsn // 10, "ts_ms": 1_700_000_000_000},
        "op": op,
        "ts_ms": 1_700_000_000_123,
    }
    if schema_wrapped:
        value = {"schema": {"type": "struct"}, "payload": value}
    return json.dumps(value).encode()


@dataclass
class FakeMessage:
    _value: bytes | None
    _offset: int
    _topic: str = "shop.public.orders"
    _partition: int = 0
    _key: bytes | None = b"k"

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


@dataclass
class FakeConsumer:
    batches: list[list[FakeMessage]]
    commits: int = 0

    def consume(self, num_messages: int, timeout: float):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous: bool = True):
        self.commits += 1


@dataclass
class FakeProducer:
    produced: list[dict] = field(default_factory=list)
    flushed: int = 0

    def produce(self, topic, value=None, key=None, headers=None):
        self.produced.append({"topic": topic, "value": value, "headers": dict(headers or [])})

    def flush(self, timeout: float = 0):
        self.flushed += 1
        return 0
