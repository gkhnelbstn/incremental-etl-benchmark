"""PostgreSQL side: four ways to keep a copy of dbo.ledger_entries in sync.

All four use the same transport (a batched read from SQL Server, then COPY into PostgreSQL),
so the timings differ only by how many rows each strategy decides to move.

- full_reload: truncate and copy everything, in one transaction.
- windowed:    replace the last WINDOW_DAYS of posting_date; older rows are assumed final.
- watermark:   copy rows with modified_at above the last load's maximum, then upsert.
- rowversion:  copy rows with a rowversion above the last load's, upsert, then delete keys
               that no longer exist in the source.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

import psycopg
import pymssql

from etl_bench import source
from etl_bench.source import COLUMNS

WINDOW_DAYS = 30
SELECT_ALL = f"SELECT {COLUMNS} FROM dbo.ledger_entries"

_TABLE = """
create table target.{name} (
    entry_id      bigint        primary key,
    account_id    integer       not null,
    posting_date  date          not null,
    amount        numeric(14,2) not null,
    currency      char(3)       not null,
    memo          varchar(40)   not null,
    modified_at   timestamp(3)  not null
) with (autovacuum_enabled = false)
"""


def reset_target(pg: psycopg.Connection) -> None:
    """Empty target tables, one per strategy, plus the table that holds watermarks.

    Autovacuum is off on the targets so the storage numbers show what each strategy leaves
    behind, not when autovacuum happened to run.
    """
    pg.execute("drop schema if exists target cascade")
    pg.execute("create schema target")
    for name in [*SYNC, "source_copy"]:
        pg.execute(_TABLE.format(name=name))
    pg.execute("create table target.sync_state (strategy text primary key, value text not null)")


def copy_into(
    pg: psycopg.Connection, table: str, rows: Iterable[tuple], columns: str = COLUMNS
) -> int:
    n = 0
    with pg.cursor().copy(f"copy {table} ({columns}) from stdin") as copy:
        for row in rows:
            copy.write_row(row)
            n += 1
    return n


def _upsert(pg: psycopg.Connection, table: str, rows: Iterable[tuple]) -> int:
    pg.execute(f"create temp table stage (like target.{table}) on commit drop")
    n = copy_into(pg, "stage", rows)
    pg.execute(
        f"""
        insert into target.{table} select * from stage
        on conflict (entry_id) do update set
            account_id = excluded.account_id, posting_date = excluded.posting_date,
            amount = excluded.amount, currency = excluded.currency,
            memo = excluded.memo, modified_at = excluded.modified_at
        """
    )
    return n


def _state(pg: psycopg.Connection, strategy: str) -> str | None:
    row = pg.execute(
        "select value from target.sync_state where strategy = %s", (strategy,)
    ).fetchone()
    return row[0] if row else None


def _save_state(pg: psycopg.Connection, strategy: str, value: object) -> None:
    pg.execute(
        """insert into target.sync_state values (%s, %s)
           on conflict (strategy) do update set value = excluded.value""",
        (strategy, str(value)),
    )


def full_reload(ms: pymssql.Connection, pg: psycopg.Connection) -> int:
    with pg.transaction():
        pg.execute("truncate target.full_reload")
        return copy_into(pg, "target.full_reload", source.extract(ms, SELECT_ALL))


def windowed(ms: pymssql.Connection, pg: psycopg.Connection) -> int:
    first = _state(pg, "windowed") is None
    cutoff = str(
        source.scalar(
            ms, f"SELECT DATEADD(day, -{WINDOW_DAYS}, MAX(posting_date)) FROM dbo.ledger_entries"
        )
    )[:10]
    sql = SELECT_ALL if first else f"{SELECT_ALL} WHERE posting_date >= '{cutoff}'"
    with pg.transaction():
        if not first:
            pg.execute("delete from target.windowed where posting_date >= %s::date", (cutoff,))
        n = copy_into(pg, "target.windowed", source.extract(ms, sql))
        _save_state(pg, "windowed", cutoff)
    return n


def watermark(ms: pymssql.Connection, pg: psycopg.Connection) -> int:
    old = _state(pg, "watermark") or "1900-01-01"
    # Read the upper bound first, so rows written during the copy wait for the next run.
    new = source.scalar(ms, "SELECT MAX(modified_at) FROM dbo.ledger_entries")
    sql = (
        f"{SELECT_ALL} WHERE modified_at > CAST('{old}' AS datetime2(3))"
        f" AND modified_at <= CAST('{new}' AS datetime2(3))"
    )
    with pg.transaction():
        n = _upsert(pg, "watermark", source.extract(ms, sql))
        _save_state(pg, "watermark", new)
    return n


def rowversion(ms: pymssql.Connection, pg: psycopg.Connection) -> int:
    old = int(_state(pg, "rowversion") or 0)
    # MIN_ACTIVE_ROWVERSION, not @@DBTS: it stops below versions of still-open transactions.
    new = source.scalar(ms, "SELECT CAST(MIN_ACTIVE_ROWVERSION() AS bigint) - 1")
    sql = (
        f"{SELECT_ALL} WHERE row_ver > CAST({old} AS binary(8))"
        f" AND row_ver <= CAST({int(new)} AS binary(8))"
    )
    with pg.transaction():
        n = _upsert(pg, "rowversion", source.extract(ms, sql))
        if old:
            # rowversion cannot see a deleted row, so compare key sets instead.
            pg.execute("create temp table source_keys (entry_id bigint) on commit drop")
            n += copy_into(
                pg,
                "source_keys",
                source.extract(ms, "SELECT entry_id FROM dbo.ledger_entries"),
                "entry_id",
            )
            pg.execute("analyze source_keys")
            pg.execute(
                """delete from target.rowversion t
                   where not exists (select 1 from source_keys k where k.entry_id = t.entry_id)"""
            )
        _save_state(pg, "rowversion", new)
    return n


SYNC: dict[str, Callable[[pymssql.Connection, psycopg.Connection], int]] = {
    "full_reload": full_reload,
    "windowed": windowed,
    "watermark": watermark,
    "rowversion": rowversion,
}
