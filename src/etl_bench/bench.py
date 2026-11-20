"""Run every strategy against the same changing source and print one markdown table.

    etl-bench run --rows 5000000 --rounds 5

One initial load per strategy, then ``rounds`` change rounds. After each round every strategy
syncs once, in rotating order so none always gets SQL Server's warm cache. At the end each
target is compared row by row with a fresh copy of the source, and its storage is measured.
"""

from __future__ import annotations

import argparse
import statistics
import time
from dataclasses import dataclass, field

import psycopg

from etl_bench import source, strategies


@dataclass
class Result:
    strategy: str
    initial_s: float = 0.0
    sync_s: list[float] = field(default_factory=list)
    rows_read: list[int] = field(default_factory=list)
    missing: int = 0  # in the source, not in the target
    stale: int = 0  # in both, with different values
    extra: int = 0  # in the target, deleted from the source
    size_mb: float = 0.0
    vacuumed_mb: float = 0.0

    @property
    def wrong(self) -> int:
        return self.missing + self.stale + self.extra


def _compare(pg: psycopg.Connection, table: str) -> tuple[int, int, int]:
    return pg.execute(
        f"""
        select count(*) filter (where t.entry_id is null),
               count(*) filter (where s.entry_id is not null and t.entry_id is not null
                   and (s.account_id, s.posting_date, s.amount, s.currency, s.memo, s.modified_at)
                       is distinct from
                       (t.account_id, t.posting_date, t.amount, t.currency, t.memo, t.modified_at)),
               count(*) filter (where s.entry_id is null)
        from target.source_copy s
        full join target.{table} t on t.entry_id = s.entry_id
        """
    ).fetchone()


def _storage(pg: psycopg.Connection, table: str) -> tuple[float, float]:
    """Table plus index size, before and after VACUUM FULL rewrites it compactly.

    The gap is bloat: space held by row versions that updates and deletes left behind.
    """
    size_sql = f"select pg_total_relation_size('target.{table}') / 1048576.0"
    before = pg.execute(size_sql).fetchone()[0]
    pg.execute(f"vacuum full target.{table}")
    return float(before), float(pg.execute(size_sql).fetchone()[0])


def run(rows: int, rounds: int, log=print) -> list[Result]:
    t = time.perf_counter()
    source.reset(rows)
    log(f"source: {rows:,} rows generated in SQL Server in {time.perf_counter() - t:.1f}s")
    names = list(strategies.SYNC)
    results = {name: Result(name) for name in names}

    with source.connect() as ms, psycopg.connect(autocommit=True) as pg:
        strategies.reset_target(pg)
        for name in names:
            t = time.perf_counter()
            strategies.SYNC[name](ms, pg)
            results[name].initial_s = time.perf_counter() - t
            log(f"initial load  {name:<12} {results[name].initial_s:7.1f}s")

        for r in range(1, rounds + 1):
            source.apply_changes(ms, r)
            k = r % len(names)
            for name in names[k:] + names[:k]:
                t = time.perf_counter()
                n = strategies.SYNC[name](ms, pg)
                results[name].sync_s.append(time.perf_counter() - t)
                results[name].rows_read.append(n)
            log(f"round {r}/{rounds} synced")

        # Storage first: the comparison below reads every page, and reads prune dead rows.
        for name in names:
            results[name].size_mb, results[name].vacuumed_mb = _storage(pg, name)

        # Untimed: a fresh full copy of the source is the answer key.
        pg.execute("truncate target.source_copy")
        strategies.copy_into(pg, "target.source_copy", source.extract(ms, strategies.SELECT_ALL))
        for name in names:
            res = results[name]
            res.missing, res.stale, res.extra = _compare(pg, name)

    return list(results.values())


def table(results: list[Result], rows: int, rounds: int) -> str:
    lines = [
        f"{rows:,} source rows, {rounds} change rounds. Sync time and rows read are medians"
        " over the rounds; wrong rows are counted after the last round.",
        "",
        "| Strategy | Initial load (s) | Sync (s) | Rows read per sync | Wrong rows"
        " (missing / stale / extra) | Size (MB) | After VACUUM FULL (MB) |",
        "| --- | ---: | ---: | ---: | --- | ---: | ---: |",
    ]
    for r in results:
        lines.append(
            f"| {r.strategy} | {r.initial_s:.1f} | {statistics.median(r.sync_s):.2f}"
            f" | {statistics.median(r.rows_read):,.0f}"
            f" | {r.wrong:,} ({r.missing:,} / {r.stale:,} / {r.extra:,})"
            f" | {r.size_mb:.0f} | {r.vacuumed_mb:.0f} |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(prog="etl-bench")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="generate the source, run all strategies, print the table")
    run_p.add_argument("--rows", type=int, default=5_000_000)
    run_p.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    if not 1_000 <= args.rows <= 10_000_000:
        parser.error("--rows must be between 1,000 and 10,000,000")
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    results = run(args.rows, args.rounds)
    print()
    print(table(results, args.rows, args.rounds))
