"""SQL Server side: the source ledger, how it is generated and how it changes between loads.

Everything is set-based T-SQL and deterministic: the same row count and round number always
give the same rows, so every strategy sees exactly the same source, run after run.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pymssql

COLUMNS = "entry_id, account_id, posting_date, amount, currency, memo, modified_at"

# Rows 1..@limit from a cross join; no table needed. Good for up to 10 million rows.
_NUMBERS = """
WITH d(n) AS (SELECT 1 FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) t(n)),
     nums(n) AS (
       SELECT TOP ({limit}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
       FROM d a, d b, d c, d e, d f, d g, d h
     )
"""

_CREATE = """
DROP TABLE IF EXISTS dbo.ledger_entries;
CREATE TABLE dbo.ledger_entries (
    entry_id      bigint        NOT NULL PRIMARY KEY,
    account_id    int           NOT NULL,
    posting_date  date          NOT NULL,
    amount        decimal(14,2) NOT NULL,
    currency      char(3)       NOT NULL,
    memo          varchar(40)   NOT NULL,
    modified_at   datetime2(3)  NOT NULL,
    row_ver       rowversion    NOT NULL
);
"""

# Three years of postings, entry_id roughly in posting order as in a real ledger.
_FILL = (
    "DECLARE @rows bigint = {rows};"
    + _NUMBERS.format(limit="@rows")
    + """
INSERT INTO dbo.ledger_entries WITH (TABLOCK)
       (entry_id, account_id, posting_date, amount, currency, memo, modified_at)
SELECT n,
       1000 + n * 7 % 4000,
       DATEADD(day, CAST((n - 1) * 1096 / @rows AS int), CAST('2023-01-01' AS date)),
       (n * 7919 % 2000000 - 400000) / 100.0,
       CASE n % 10 WHEN 0 THEN 'USD' WHEN 1 THEN 'GBP' ELSE 'EUR' END,
       CONCAT('entry ', n),
       DATEADD(minute, CAST(n % 600 AS int),
               CAST(DATEADD(day, CAST((n - 1) * 1096 / @rows AS int),
                            CAST('2023-01-01' AS date)) AS datetime2(3)))
FROM nums;
CREATE INDEX ix_ledger_posting_date ON dbo.ledger_entries (posting_date);
CREATE INDEX ix_ledger_modified_at ON dbo.ledger_entries (modified_at);
CREATE INDEX ix_ledger_row_ver ON dbo.ledger_entries (row_ver);
"""
)

# One round of change between two loads. Rows are picked by an MD5 bucket of (key, round);
# CHECKSUM looks random but its low bits are too regular for a modulo test.
# Round r happens on day 2025-12-31 + r.
_CHANGE = (
    """
DECLARE @r int = {round};
DECLARE @clock datetime2(3) = DATEADD(hour, @r, CAST('2026-01-01' AS datetime2(3)));
DECLARE @today date = DATEADD(day, @r, CAST('2025-12-31' AS date));
DECLARE @rows bigint = (SELECT COUNT_BIG(*) FROM dbo.ledger_entries);
DECLARE @next bigint = (SELECT MAX(entry_id) FROM dbo.ledger_entries);
DECLARE @new bigint = @rows / 200, @late bigint = @rows / 2000;

-- 0.5% of rows corrected, spread across all of history. modified_at is set.
UPDATE dbo.ledger_entries SET amount = amount + 1.00, modified_at = @clock
WHERE CAST(SUBSTRING(HASHBYTES('MD5', CONCAT(entry_id, ':', @r, ':1')), 1, 4) AS bigint) % 200 = 0;

-- 0.02%: a bulk fix script that changes rows without touching modified_at.
UPDATE dbo.ledger_entries SET memo = LEFT(CONCAT(memo, ' fix'), 40)
WHERE CAST(SUBSTRING(HASHBYTES('MD5', CONCAT(entry_id, ':', @r, ':2')), 1, 4) AS bigint) % 5000 = 0;

-- 0.02%: hard deletes (reversed entries removed instead of cancelled).
DELETE FROM dbo.ledger_entries
WHERE CAST(SUBSTRING(HASHBYTES('MD5', CONCAT(entry_id, ':', @r, ':3')), 1, 4) AS bigint) % 5000 = 0;
"""
    + _NUMBERS.format(limit="@new + @late")
    + """
-- 0.5% new postings for today, plus 0.05% late postings dated 90 to 390 days back.
INSERT INTO dbo.ledger_entries
       (entry_id, account_id, posting_date, amount, currency, memo, modified_at)
SELECT @next + n,
       1000 + (@next + n) * 7 % 4000,
       CASE WHEN n <= @new THEN @today ELSE DATEADD(day, -CAST(90 + n % 300 AS int), @today) END,
       ((@next + n) * 7919 % 2000000 - 400000) / 100.0,
       'EUR',
       CONCAT('entry ', @next + n),
       @clock
FROM nums;
"""
)


def connect(database: str | None = None) -> pymssql.Connection:
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "127.0.0.1"),
        port=os.environ.get("MSSQL_PORT", "1433"),
        user=os.environ.get("MSSQL_USER", "sa"),
        password=os.environ["MSSQL_PASSWORD"],
        database=database or os.environ.get("MSSQL_DATABASE", "ledger"),
        autocommit=True,
    )


def reset(rows: int) -> None:
    """Recreate the database and fill dbo.ledger_entries with ``rows`` rows."""
    name = os.environ.get("MSSQL_DATABASE", "ledger")
    with connect("master") as master:
        cur = master.cursor()
        cur.execute(f"IF DB_ID('{name}') IS NULL CREATE DATABASE [{name}]")
        cur.execute(f"ALTER DATABASE [{name}] SET RECOVERY SIMPLE")
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(_CREATE)
        cur.execute(_FILL.format(rows=int(rows)))


def apply_changes(conn: pymssql.Connection, round_no: int) -> None:
    conn.cursor().execute(_CHANGE.format(round=int(round_no)))


def scalar(conn: pymssql.Connection, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()[0]


def extract(conn: pymssql.Connection, sql: str, batch: int = 50_000) -> Iterator[tuple]:
    """Stream a query's rows in batches; the shared transport for every strategy."""
    cur = conn.cursor()
    cur.execute(sql)
    while rows := cur.fetchmany(batch):
        yield from rows
