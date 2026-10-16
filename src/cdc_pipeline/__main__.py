"""Entrypoint: ``python -m cdc_pipeline``."""

from __future__ import annotations

import logging
import signal

from .config import Settings, load_tables
from .processor import Processor
from .sink import SqlSink


def main() -> None:
    # Heavy client libraries are imported here so unit tests don't need them.
    import psycopg
    from confluent_kafka import Consumer, Producer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = Settings.from_env()
    tables = load_tables(s.tables_file)

    consumer = Consumer({
        "bootstrap.servers": s.bootstrap_servers,
        "group.id": s.group_id,
        "enable.auto.commit": False,          # we commit after the sink transaction
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "partition.assignment.strategy": "cooperative-sticky",  # fewer stop-the-world rebalances
    })
    consumer.subscribe(s.topics)
    dlq = Producer({"bootstrap.servers": s.bootstrap_servers, "enable.idempotence": True, "acks": "all"})

    conn = psycopg.connect(s.warehouse_dsn)
    sink = SqlSink(conn, tables, paramstyle="format")
    sink.create_tables()

    processor = Processor(
        consumer, sink, dlq,
        primary_keys={t.source_table: list(t.primary_key) for t in tables},
        dlq_topic=s.dlq_topic,
        batch_size=s.batch_size,
        transient_errors=(psycopg.OperationalError,),
    )

    stopping = {"flag": False}

    def _stop(*_):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        processor.run_forever(lambda: stopping["flag"])
    finally:
        consumer.close()  # commits nothing extra; leaves the group cleanly
        dlq.flush(10)
        conn.close()


if __name__ == "__main__":
    main()
