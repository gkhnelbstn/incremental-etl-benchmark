# incremental-etl-benchmark

Full reload vs windowed vs incremental ETL from SQL Server to PostgreSQL, measured for speed, storage **and correctness**.

This is a public, generated-data version of a pattern I used at a logistics company to cut a nightly accounting load from about 42 hours to minutes (details anonymized). The interesting part is not that incremental loads are faster. It is *which* incremental load silently drifts from the source, and by how much.

| | |
| --- | --- |
| **Question** | How much faster are incremental loads than a full reload, and what does each shortcut get wrong? |
| **Data** | `dbo.ledger_entries` in SQL Server 2022: 5,000,000 generated ledger rows over three years, generated set-based in T-SQL and deterministic. |
| **Method** | Four strategies keep their own copy in PostgreSQL 17. After one initial load, 5 rounds of source changes follow, and every strategy syncs once per round. All four use the same transport: a batched read, then `COPY`. |
| **Check** | After the last round, each copy is compared row by row with a fresh full copy of the source and counted as missing, stale or extra. |
| **Result** | A `modified_at` watermark syncs in **4 s instead of 109 s** but ends up with **10,125 wrong rows**. Only full reload and rowversion with a delete pass stay exact. |

## Results

5,000,000 source rows, 5 change rounds. Sync time and rows read are medians over the rounds; wrong rows are counted after the last round.

| Strategy | Initial load (s) | Sync (s) | Rows read per sync | Wrong rows (missing / stale / extra) | Size (MB) | After VACUUM FULL (MB) |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| full_reload | 121.6 | 109.02 | 5,079,896 | 0 (0 / 0 / 0) | 519 | 518 |
| windowed | 113.7 | 6.86 | 203,039 | 142,877 (12,625 / 125,397 / 4,855) | 598 | 517 |
| watermark | 143.4 | 3.96 | 52,898 | 10,125 (0 / 5,088 / 5,037) | 604 | 519 |
| rowversion | 144.1 | 25.58 | 5,133,783 | 0 (0 / 0 / 0) | 603 | 518 |

Machine: Intel Core i5-12500H, 32 GB RAM, Windows 11, both databases in Docker Desktop (16 CPUs, 23.5 GB for the VM). Treat absolute times as machine-specific; the ratios and the wrong-row counts are the result.

## What changes in the source between loads

Each round is one simulated day. The mix is deliberately ordinary for a ledger:

| Change per round | Share of rows | Sets `modified_at`? |
| --- | ---: | --- |
| Corrections spread across all of history | 0.5% | yes |
| New postings dated today | 0.5% | yes |
| Late postings dated 90 to 390 days back | 0.05% | yes |
| Bulk fix script that updates rows directly | 0.02% | **no** |
| Hard deletes (reversals removed instead of cancelled) | 0.02% | row is gone |

## The four strategies, and when each one breaks

**full_reload**: `TRUNCATE` and copy everything in one transaction.
- Always correct and always compact.
- Cost grows with table size, not with the size of the change: 109 s here even when 0.5% of rows changed.
- `TRUNCATE` takes an exclusive lock, so readers wait for the whole copy. Swapping in a freshly loaded table avoids that, but needs twice the space.

**windowed**: delete the last 30 days of `posting_date` and copy them again. It assumes older rows are final.
- That assumption is what breaks. Corrections to old rows are never picked up (125,397 stale rows).
- Late postings dated before the window never arrive (12,625 missing rows).
- Old rows deleted in the source stay in the copy (4,855 extra rows).
- It is fine only for data that really is immutable after a cut-off, such as a closed accounting period. Even then you should check that the period is closed.

**watermark**: copy rows whose `modified_at` is above the last load's maximum, then upsert.
- Fast, and it catches every insert and every normal update, late ones included: 0 missing rows.
- It trusts every writer to set `modified_at`. A fix script that doesn't leaves 5,088 stale rows.
- A hard delete leaves no row to see, so 5,037 deleted rows stay in the copy.
- The upper bound is read before the copy, so rows written during the copy wait for the next run instead of being skipped.

**rowversion**: copy rows whose SQL Server `rowversion` is above the last load's, upsert, then remove keys that no longer exist in the source.
- `rowversion` changes on every write, whatever the writer does, so silent fixes are caught.
- The upper bound is `MIN_ACTIVE_ROWVERSION() - 1`, not `@@DBTS`, so versions of still-open transactions are not skipped.
- Deletes need the key-set pass. It reads all 5.08M keys each run, and that is most of its 25.6 s.
- The next step is SQL Server Change Tracking or CDC, which records deletes. That removes the key scan, but it has to be enabled on the source database, and that is not always allowed.

## Storage

Autovacuum is off on the target tables, so the sizes show what each strategy leaves behind rather than when autovacuum happened to run.
- Upserts and window deletes leave old row versions behind. After 5 rounds the incremental copies are about 15% larger than their compact size (598–604 MB vs 517–519 MB after `VACUUM FULL`).
- A full reload rewrites the table and stays compact.
- In production, autovacuum lets the table reuse that space but does not give it back to the operating system. Size the disk for the bloated figure.

## Run it

Requirements: Docker and [uv](https://docs.astral.sh/uv/).

```sh
cp .env.example .env        # set both passwords
docker compose up -d --wait # SQL Server 2022 Developer + PostgreSQL 17
uv sync
set -a; . ./.env; set +a    # etl-bench reads the same variables
uv run etl-bench run --rows 5000000 --rounds 5
```

The last command generates the source, runs all four strategies and prints the results table. The 5M run takes about 25 minutes on the machine above. `--rows 200000` takes under a minute.

`compose.yaml` uses SQL Server **Developer edition**. It is free for development and testing, and not licensed for production. Starting it accepts Microsoft's SQL Server license (`ACCEPT_EULA=Y`).

## Fairness notes

- **Same transport for all four:** a `fetchmany` loop from SQL Server, then PostgreSQL `COPY`. No strategy pays for row-by-row inserts, so time differences come from the number of rows moved.
- **Same changes for all four:** every strategy syncs after the same change round.
- **Rotating order:** the order rotates each round, so no strategy always gets SQL Server's warm cache.
- **Initial loads:** `watermark` and `rowversion` load through a staging table and upsert, as they do on every run. That is why their first load is slower than a plain copy.

## Tests and CI

- `tests/test_bench.py` runs the benchmark on 20,000 rows and checks the claims above:
  - full reload and rowversion have 0 wrong rows;
  - windowed has missing, stale and extra rows;
  - watermark has no missing rows but has stale and extra ones;
  - incremental reads are under a tenth of a full reload.
- GitHub Actions starts SQL Server and PostgreSQL as service containers, runs lint and the tests, then runs `etl-bench run --rows 50000 --rounds 3` and writes its table to the job summary.

## Layout

```
src/etl_bench/source.py      SQL Server: table, generator, change rounds, batched extract
src/etl_bench/strategies.py  PostgreSQL: the four sync strategies
src/etl_bench/bench.py       the benchmark run, row-by-row comparison, storage, CLI
tests/test_bench.py          end-to-end check of the documented failure modes
```

## License

MIT
