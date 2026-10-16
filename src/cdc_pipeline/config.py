"""Runtime configuration (12-factor: everything from env, tables from YAML)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from .sink import TableSpec


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    group_id: str
    topics: list[str]
    dlq_topic: str
    warehouse_dsn: str
    tables_file: Path
    batch_size: int

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=os.getenv("CONSUMER_GROUP", "cdc-warehouse-sink"),
            topics=os.getenv("CDC_TOPICS", "shop.public.orders,shop.public.customers").split(","),
            dlq_topic=os.getenv("DLQ_TOPIC", "cdc.dlq"),
            warehouse_dsn=os.getenv("WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5433/warehouse"),
            tables_file=Path(os.getenv("TABLES_FILE", "config/tables.yml")),
            batch_size=int(os.getenv("BATCH_SIZE", "500")),
        )


def load_tables(path: Path) -> list[TableSpec]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [
        TableSpec(
            source_table=t["source"],
            target_table=t["target"],
            primary_key=tuple(t["primary_key"]),
            columns=tuple((c["name"], c["type"]) for c in t["columns"]),
        )
        for t in raw["tables"]
    ]
