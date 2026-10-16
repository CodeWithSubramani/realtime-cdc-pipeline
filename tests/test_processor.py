from __future__ import annotations

import sqlite3

import pytest

from cdc_pipeline.processor import Processor, SinkUnavailable
from cdc_pipeline.sink import SqlSink

from .helpers import PK, FakeConsumer, FakeMessage, FakeProducer, envelope
from .test_sink import ORDERS


def make(batches, sink=None, **kw):
    if sink is None:
        conn = sqlite3.connect(":memory:")
        sink = SqlSink(conn, [ORDERS])
        sink.create_tables()
    consumer, dlq = FakeConsumer(batches), FakeProducer()
    return Processor(consumer, sink, dlq, PK, sleep=lambda s: None, **kw), consumer, dlq, sink


def test_happy_path_commits_after_apply():
    msgs = [FakeMessage(envelope("c", lsn=10), 0), FakeMessage(envelope("u", lsn=20, status="paid"), 1)]
    p, consumer, dlq, sink = make([msgs])
    stats = p.run_once()
    assert stats.received == 2 and stats.applied_events == 1  # compacted
    assert consumer.commits == 1
    assert sink.conn.execute("SELECT status FROM orders_current").fetchone() == ("paid",)


def test_poison_goes_to_dlq_and_batch_continues():
    msgs = [FakeMessage(b"garbage", 0), FakeMessage(envelope("c", lsn=10), 1), FakeMessage(None, 2)]
    p, consumer, dlq, _ = make([msgs])
    stats = p.run_once()
    assert stats.dead_lettered == 1 and stats.tombstones == 1 and stats.applied_events == 1
    assert dlq.produced[0]["topic"] == "cdc.dlq"
    assert dlq.produced[0]["headers"]["source_offset"] == b"0"
    assert dlq.flushed == 1  # DLQ durable before commit
    assert consumer.commits == 1


class FlakySink:
    def __init__(self, failures):
        self.failures = failures
        self.calls = 0

    def apply(self, events):
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("warehouse unavailable")
        return len(list(events))


def test_transient_sink_failure_is_retried():
    sink = FlakySink(failures=2)
    p, consumer, _, _ = make([[FakeMessage(envelope("c", lsn=10), 0)]], sink=sink, max_retries=3)
    stats = p.run_once()
    assert stats.retries == 2 and sink.calls == 3
    assert consumer.commits == 1


def test_exhausted_retries_do_not_commit_offsets():
    sink = FlakySink(failures=99)
    p, consumer, _, _ = make([[FakeMessage(envelope("c", lsn=10), 0)]], sink=sink, max_retries=2)
    with pytest.raises(SinkUnavailable):
        p.run_once()
    assert consumer.commits == 0  # Kafka will redeliver after restart


def test_empty_poll_is_a_noop():
    p, consumer, _, _ = make([])
    assert p.run_once().received == 0
    assert consumer.commits == 0
