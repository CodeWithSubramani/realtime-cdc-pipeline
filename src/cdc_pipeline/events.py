"""Parsing Debezium change events into a typed, engine-agnostic ``ChangeEvent``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Op(str, Enum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # initial snapshot row

    @property
    def is_upsert(self) -> bool:
        return self is not Op.DELETE


class PoisonMessage(ValueError):
    """The message can never be processed (malformed / unsupported). Route to the DLQ."""


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: Op
    key: tuple[Any, ...]
    row: dict[str, Any]        # `after` for upserts, `before` (or key-only) for deletes
    lsn: int                   # monotonically increasing position in the Postgres WAL
    tx_id: int | None
    source_ts_ms: int          # commit time at the source
    topic: str = ""
    partition: int = -1
    offset: int = -1

    @property
    def version(self) -> int:
        """Ordering key used for last-writer-wins. WAL position, not wall clock."""
        return self.lsn


def _unwrap(value: dict[str, Any]) -> dict[str, Any]:
    # JsonConverter with schemas.enable=true wraps everything in {"schema":..,"payload":..}
    if "payload" in value and "schema" in value:
        value = value["payload"]
    return value


def parse(raw_value: bytes | str | None, primary_key: dict[str, list[str]], *,
          topic: str = "", partition: int = -1, offset: int = -1) -> ChangeEvent | None:
    """Parse one Kafka record value.

    Returns ``None`` for tombstones (null values Debezium emits after a delete so log
    compaction can drop the key) — they carry no information for the sink.
    Raises ``PoisonMessage`` for anything that can't be processed, ever.
    """
    if raw_value is None:
        return None
    try:
        value = _unwrap(json.loads(raw_value))
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise PoisonMessage(f"not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise PoisonMessage("envelope is not an object")

    try:
        op = Op(value.get("op"))
    except ValueError as exc:
        raise PoisonMessage(f"unsupported op {value.get('op')!r}") from exc

    source = value.get("source") or {}
    table = source.get("table")
    if not table:
        raise PoisonMessage("missing source.table")
    qualified = f"{source.get('schema', 'public')}.{table}"
    pk = primary_key.get(qualified) or primary_key.get(table)
    if not pk:
        raise PoisonMessage(f"no primary key configured for {qualified}")

    row = value.get("before") if op is Op.DELETE else value.get("after")
    if not isinstance(row, dict):
        raise PoisonMessage(f"op={op.value} without {'before' if op is Op.DELETE else 'after'} image")
    missing = [k for k in pk if row.get(k) is None]
    if missing:
        raise PoisonMessage(f"primary key column(s) {missing} missing from row image")

    lsn = source.get("lsn")
    if lsn is None:
        raise PoisonMessage("missing source.lsn (cannot order events)")

    return ChangeEvent(
        table=qualified,
        op=op,
        key=tuple(row[k] for k in pk),
        row=row,
        lsn=int(lsn),
        tx_id=source.get("txId"),
        source_ts_ms=int(source.get("ts_ms") or value.get("ts_ms") or 0),
        topic=topic,
        partition=partition,
        offset=offset,
    )
