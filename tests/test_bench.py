"""End-to-end check: each strategy is fast or correct in exactly the way the README claims."""

from __future__ import annotations

import os

import pytest

TEST_DB = "etl_bench_test"


@pytest.fixture
def databases(monkeypatch) -> None:
    """Point both connections at throwaway databases so tests never touch the real ones."""
    psycopg = pytest.importorskip("psycopg")
    if not (os.environ.get("PGHOST") and os.environ.get("MSSQL_PASSWORD")):
        pytest.skip("PGHOST or MSSQL_PASSWORD not set; skipping database tests")
    try:
        admin = psycopg.connect(dbname=os.environ.get("PGDATABASE", "postgres"), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with admin:
        admin.execute(f"drop database if exists {TEST_DB} with (force)")
        admin.execute(f"create database {TEST_DB}")
    monkeypatch.setenv("PGDATABASE", TEST_DB)
    monkeypatch.setenv("MSSQL_DATABASE", TEST_DB)


def test_strategies_trade_speed_for_correctness_as_documented(databases) -> None:
    from etl_bench import bench

    results = {r.strategy: r for r in bench.run(rows=20_000, rounds=3, log=lambda _: None)}
    full, win, wm, rv = (results[n] for n in ("full_reload", "windowed", "watermark", "rowversion"))

    # Always right: the full reload, and rowversion with its key-set delete pass.
    assert full.wrong == 0
    assert rv.wrong == 0

    # Windowed misses old corrections, late postings and old deletes.
    assert win.stale > 0
    assert win.missing > 0
    assert win.extra > 0

    # The modified_at watermark sees every insert, but not silent fixes or hard deletes.
    assert wm.missing == 0
    assert wm.stale > 0
    assert wm.extra > 0

    # Incremental reads are a small fraction of a full reload.
    assert max(wm.rows_read) * 10 < min(full.rows_read)
