# incremental-etl-benchmark

Full reload vs windowed vs incremental merge ETL from SQL Server to PostgreSQL, benchmarked.

**Status: planned.** This repo is a work in progress; the checklist lives in the epic issue and on the [Portfolio build-out](https://github.com/users/gkhnelbstn/projects/6) board.

## Plan

- What: the pattern that took your accounting ETL from ~42 hours to minutes: full reload vs windowed extraction vs incremental merge, SQL Server -> PostgreSQL.
- Must have: reproducible benchmark on generated data (a few million rows), timings table, storage comparison, explanation of when each approach breaks.
- Done when: one command produces the timings table.
