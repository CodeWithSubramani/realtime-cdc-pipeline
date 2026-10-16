"""The consume -> parse -> compact -> apply -> commit loop.

Delivery semantics
------------------
Kafka offsets are committed **only after** the sink transaction commits, giving
at-least-once delivery. Because the sink's upsert is guarded by the WAL position
(``_lsn``), re-applying an event is a no-op — so the *observable* result is
effectively-once without distributed transactions.

Failure handling
----------------
* Poison message (can never succeed) -> dead-letter topic with error headers, then
  continue. The DLQ is flushed *before* offsets are committed so nothing is lost.
* Transient sink failure -> bounded exponential backoff, same batch retried.
* Retries exhausted -> raise. The process dies without committing; the orchestrator
  restarts it and Kafka redelivers from the last committed offset.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence

from .batching import compact
from .events import ChangeEvent, PoisonMessage, parse
from .sink import Sink

log = logging.getLogger(__name__)


class Message(Protocol):  # subset of confluent_kafka.Message
    def value(self) -> bytes | None: ...
    def key(self) -> bytes | None: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def error(self) -> Any: ...


class Consumer(Protocol):
    def consume(self, num_messages: int, timeout: float) -> Sequence[Message]: ...
    def commit(self, asynchronous: bool = ...) -> Any: ...


class Producer(Protocol):
    def produce(self, topic: str, value: bytes | None = ..., key: bytes | None = ...,
                headers: list[tuple[str, bytes]] | None = ...) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...


@dataclass
class BatchStats:
    received: int = 0
    tombstones: int = 0
    dead_lettered: int = 0
    applied_events: int = 0
    rows_changed: int = 0
    retries: int = 0


class SinkUnavailable(RuntimeError):
    pass


class Processor:
    def __init__(
        self,
        consumer: Consumer,
        sink: Sink,
        dlq: Producer,
        primary_keys: dict[str, list[str]],
        *,
        dlq_topic: str = "cdc.dlq",
        batch_size: int = 500,
        poll_timeout_s: float = 1.0,
        max_retries: int = 5,
        base_backoff_s: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        transient_errors: tuple[type[BaseException], ...] = (Exception,),
    ):
        self.consumer = consumer
        self.sink = sink
        self.dlq = dlq
        self.primary_keys = primary_keys
        self.dlq_topic = dlq_topic
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.max_retries = max_retries
        self.base_backoff_s = base_backoff_s
        self.sleep = sleep
        self.transient_errors = transient_errors

    def _dead_letter(self, msg: Message, exc: Exception) -> None:
        self.dlq.produce(
            self.dlq_topic,
            value=msg.value(),
            key=msg.key(),
            headers=[
                ("error", str(exc).encode()),
                ("source_topic", msg.topic().encode()),
                ("source_partition", str(msg.partition()).encode()),
                ("source_offset", str(msg.offset()).encode()),
            ],
        )

    def _apply_with_retry(self, events: list[ChangeEvent], stats: BatchStats) -> None:
        for attempt in range(self.max_retries + 1):
            try:
                stats.rows_changed = self.sink.apply(events)
                return
            except self.transient_errors as exc:
                if attempt == self.max_retries:
                    raise SinkUnavailable(f"sink failed after {attempt + 1} attempts") from exc
                stats.retries += 1
                delay = self.base_backoff_s * (2 ** attempt)
                log.warning("sink apply failed (%s); retry %d in %.1fs", exc, attempt + 1, delay)
                self.sleep(delay)

    def run_once(self) -> BatchStats:
        stats = BatchStats()
        msgs = self.consumer.consume(self.batch_size, self.poll_timeout_s)
        if not msgs:
            return stats

        events: list[ChangeEvent] = []
        for m in msgs:
            if m.error():
                # Partition EOF and similar informational errors are skipped; fatal ones
                # surface via the client's error callback / exceptions.
                continue
            stats.received += 1
            try:
                e = parse(m.value(), self.primary_keys, topic=m.topic(),
                          partition=m.partition(), offset=m.offset())
            except PoisonMessage as exc:
                log.error("dead-lettering %s[%d]@%d: %s", m.topic(), m.partition(), m.offset(), exc)
                self._dead_letter(m, exc)
                stats.dead_lettered += 1
                continue
            if e is None:
                stats.tombstones += 1
            else:
                events.append(e)

        if stats.dead_lettered:
            self.dlq.flush(10.0)

        batch = compact(events)
        stats.applied_events = len(batch)
        if batch:
            self._apply_with_retry(batch, stats)

        self.consumer.commit(asynchronous=False)
        return stats

    def run_forever(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        while not should_stop():
            s = self.run_once()
            if s.received:
                log.info("batch: %s", s)
