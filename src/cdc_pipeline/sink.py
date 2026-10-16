"""Idempotent, order-safe SQL sink.

The core trick: the version guard lives *inside* the upsert statement

    INSERT ... ON CONFLICT (pk) DO UPDATE SET ... WHERE target._lsn < excluded._lsn

so the database — not the consumer — decides whether an event is newer. That makes the
sink safe under redelivery (at-least-once Kafka), partition rebalances, replays from an
old offset and parallel writers, with no read-before-write round trip.

The same SQL runs on PostgreSQL and SQLite (>= 3.24), so tests exercise real semantics.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from .events import ChangeEvent, Op

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def _q(ident: str) -> str:
    if not _IDENT.match(ident):
        raise ValueError(f"unsafe identifier {ident!r}")
    return f'"{ident}"'


@dataclass(frozen=True)
class TableSpec:
    source_table: str            # e.g. "public.orders"
    target_table: str            # e.g. "orders_current"
    primary_key: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]  # (name, SQL type) of replicated business columns

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)


class Sink(Protocol):
    def apply(self, events: Iterable[ChangeEvent]) -> int: ...


class SqlSink:
    META = ("_lsn", "_deleted", "_source_ts_ms")

    def __init__(self, connection: Any, tables: Iterable[TableSpec], paramstyle: str = "qmark"):
        self.conn = connection
        self.tables = {t.source_table: t for t in tables}
        self.ph = "?" if paramstyle == "qmark" else "%s"
        self._sql = {name: self._build(t) for name, t in self.tables.items()}

    # -- DDL ---------------------------------------------------------------------------
    def create_tables(self) -> None:
        cur = self.conn.cursor()
        for t in self.tables.values():
            cols = ", ".join(f"{_q(c)} {_type(sql_type)}" for c, sql_type in t.columns)
            pk = ", ".join(_q(c) for c in t.primary_key)
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {_q(t.target_table)} ({cols}, "
                f"_lsn BIGINT NOT NULL, _deleted BOOLEAN NOT NULL DEFAULT FALSE, "
                f"_source_ts_ms BIGINT, PRIMARY KEY ({pk}))"
            )
        self.conn.commit()

    # -- DML ---------------------------------------------------------------------------
    def _build(self, t: TableSpec) -> dict[str, str]:
        tgt = _q(t.target_table)
        cols = [*t.column_names, *self.META]
        col_list = ", ".join(_q(c) for c in cols)
        values = ", ".join(self.ph for _ in cols)
        pk = ", ".join(_q(c) for c in t.primary_key)
        set_upsert = ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in cols if c not in t.primary_key)
        upsert = (
            f"INSERT INTO {tgt} ({col_list}) VALUES ({values}) "
            f"ON CONFLICT ({pk}) DO UPDATE SET {set_upsert} "
            f"WHERE {tgt}._lsn < excluded._lsn"
        )
        # Deletes reuse the same guarded upsert with ``_deleted = TRUE`` and the full
        # before-image (REPLICA IDENTITY FULL). Writing the whole image — not just the key —
        # means a delete that arrives *before* older updates still converges to the same row,
        # and a late, older insert can never resurrect it.
        return {"upsert": upsert}

    def _params(self, t: TableSpec, e: ChangeEvent) -> tuple[Any, ...]:
        return (*(e.row.get(c) for c in t.column_names), e.lsn, e.op is Op.DELETE, e.source_ts_ms)

    def apply(self, events: Iterable[ChangeEvent]) -> int:
        """Apply events in ONE transaction. Returns number of rows actually changed."""
        changed = 0
        cur = self.conn.cursor()
        try:
            for e in events:
                t = self.tables.get(e.table)
                if t is None:
                    continue  # not a replicated table; filtered by design
                cur.execute(self._sql[e.table]["upsert"], self._params(t, e))
                changed += max(cur.rowcount, 0)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()
        return changed


_TYPE = re.compile(r"^[A-Za-z]+( [A-Za-z]+)*( ?\(\d+(, ?\d+)?\))?$")


def _type(sql_type: str) -> str:
    if not _TYPE.match(sql_type):
        raise ValueError(f"unsafe SQL type {sql_type!r}")
    return sql_type
