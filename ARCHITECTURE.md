# Architecture — `articles` ETL (Postgres → ClickHouse, hourly)

```
┌────────────────────────┐      ┌──────────────────────────────────────────┐      ┌────────────────────────────┐
│ SOURCE: Postgres       │      │ AIRFLOW 3 (standalone, LocalExecutor)    │      │ WAREHOUSE: ClickHouse      │
│                        │      │                                          │      │                            │
│ public.articles        │ ───► │ 1. articles_backfill  (manual, monthly)  │ ───► │ raw.articles               │
│  idx: id (PK)          │ read │ 2. articles_hourly    (@hourly)          │ ins. │  ReplacingMergeTree        │
│  idx: updated_at       │      │ 3. articles_reconcile (@daily)           │      │  (_version, is_deleted)    │
│  idx: created_at       │      │                                          │      │                            │
│  trigger: updated_at   │      │ dags/common.py: extract / clean / load   │      │ analytics.articles (view)  │
│                        │      │                                          │      │  FINAL, live articles only │
│ seed.sql               │      └──────────────────────────────────────────┘      └────────────────────────────┘
│ simulate_changes.sql   │
└────────────────────────┘
```

## Data

the source is seeded with synthetic data (`source/seed.sql`):

- Articles from **2016-01-01 to today**, daily volume growing linearly (~100/day in 2016 → ~1,000/day now at `scale=1`, ≈2M rows). Real expected volume (100k → 1M+/day) is scaled down to fit a laptop; `scale` raises it.
- `content` is 200–1,500 chars of filler text (the heavy column).
- ~20% of rows edited later (`updated_at > created_at`), ~3% soft-deleted (`deleted_at` set), ~10% drafts (`published_at IS NULL`).
- Some 2016 "legacy" rows carry quirks the pipeline must handle: padded/blank titles, `NULL author_id`, `published_at < created_at`.
- `simulate_changes.sql` inserts, updates, soft-deletes and **hard-deletes** rows to demo the hourly + reconcile flows.
- No real people, no PII.

## Warehouse table

`raw.articles` = source columns + `is_deleted UInt8`, `_version DateTime64`, `_synced_at DateTime64`.

- `ENGINE = ReplacingMergeTree(_version, is_deleted)`, `ORDER BY id`, `PARTITION BY toYear(created_at)`.
  - Yearly, not monthly: an hourly batch of edits touches articles from any month since 2016, and ClickHouse rejects inserts spanning >100 partitions (found in testing). ~11 partitions also matches ClickHouse's guidance to keep partitions coarse.
- Re-inserting the same `id` is safe: highest `_version` wins → every DAG is **idempotent** (retries and overlap windows never duplicate).
- `_version = updated_at` for rows read from the source (a trigger bumps `updated_at` on every update, including soft deletes); `_version = now()` for hard-delete tombstones.
- Semantics:
  - **soft delete** (row still in source, `deleted_at` set) → mirrored as-is, `deleted_at` populated, `is_deleted = 0`.
  - **hard delete** (row gone from source) → tombstone with `is_deleted = 1`; `FINAL` hides it.
- Analysts query `analytics.articles` (`FINAL`, `deleted_at IS NULL`). `raw.articles FINAL` = exact mirror of the source.

## DAG 1 — `articles_backfill` (bonus Q1: history since 2016)

- Manual trigger, run once. Dynamic task mapping over **monthly chunks** (2016-01 → current month), `max_active_tis_per_dag` caps load on the source.
- Each chunk: server-side cursor on `created_at` in month → batches of 50k → clean → insert.
- Post-check: source `count(*)` vs warehouse `count() FINAL` for closed months; mismatch fails the task (retry is safe).
- Separate from the hourly DAG: `catchup` on an hourly schedule since 2016 would be ~95k tiny runs.

## DAG 2 — `articles_hourly` (the task)

- `@hourly`, `catchup=False`, `max_active_runs=1`.
- **Watermark** = `max(updated_at)` already in the warehouse; extract `WHERE updated_at >= watermark - 15min`. Self-healing: after downtime or failed runs, the next run picks up everything since the last successful load. If the warehouse is empty it loads only the last hour (history is the backfill's job).
- 15-min overlap catches late-committing transactions; dedup makes it free.
- Captures **inserts, updates, soft deletes**. Cannot see **hard deletes** — those rows no longer exist.

## DAG 3 — `articles_reconcile` (bonus Q2: hard deletes)

- `@daily`, off-peak.
- Split `id` into 100k buckets; compute `count` + `sum(id)` per bucket on **both** sides (cheap, index-only on source).
- Only for mismatched buckets: fetch both id sets and diff.
  - In warehouse, not in source → re-check against source (guards against rows loaded mid-run) → insert tombstone (`is_deleted=1`, `_version=now()`).
  - In source, not in warehouse → re-pull those rows (also heals anything hourly missed).
- Logs counts fixed per run; a steady non-zero count means something upstream is wrong.
- Production answer: **CDC (Debezium on the Postgres WAL)** gives real-time deletes; documented, not built (out of scope).

## Repo layout

```
docker-compose.yml
.env.example
airflow/Dockerfile               # apache/airflow:3.3.2 + clickhouse-connect
source/init.sql                  # articles table, indexes, updated_at trigger
source/seed.sql                  # synthetic history 2016 → now
source/simulate_changes.sql      # inserts / updates / soft + hard deletes
warehouse/init.sql               # raw.articles + analytics.articles view
dags/common.py                   # connections, extract, clean, load
dags/articles_backfill.py
dags/articles_hourly.py
dags/articles_reconcile.py
README.md                        # run steps, bonus answers, AI disclosure
```

## Versions

| Component | Version | Note |
|---|---|---|
| Apache Airflow | 3.3.2 (python3.12) | 3.4.0 is a week old; 3.3.2 is the first 3.3.x covering the 2026 RBAC / deserialization CVEs |
| ClickHouse | 26.8.36.3 | LTS line |
| Postgres | 17.12 | source + Airflow metadata |
| clickhouse-connect | 1.9.0 | |

## Demo

1. `docker compose up -d` → seed source.
2. Trigger `articles_backfill` → counts match per month.
3. Run `simulate_changes.sql`.
4. Trigger `articles_hourly` → inserts/updates/soft deletes appear; hard deletes still present.
5. Trigger `articles_reconcile` → hard deletes tombstoned; source and warehouse match.
