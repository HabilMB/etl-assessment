# Overview

Hourly ETL of the `articles` table from **Postgres** (source) into **ClickHouse** (warehouse), orchestrated by **Airflow 3**.
It includes a one-time historical backfill since 2016 and a daily reconciliation job that syncs hard deletes.

Design details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Run it

Requirements: Docker with Compose v2, and about 4 GB of free disk.

1. Create a `.env` next to `docker-compose.yml`:

   ```
   SOURCE_DB_USER=etl
   SOURCE_DB_PASSWORD=<choose one>
   CH_USER=etl
   CH_PASSWORD=<choose one>
   AIRFLOW_DB_PASSWORD=<choose one>
   # AIRFLOW_PORT=8080   # change if 8080 is taken
   ```

2. Start the stack and seed the source (≈2.2M articles, ~40 s):

   ```bash
   docker compose up -d --build
   docker compose exec source-db sh -c 'psql -U "$POSTGRES_USER" -d kumparan -v scale=1 -f /sql/seed.sql'
   ```

3. Open Airflow at http://localhost:8080. The user is `admin`. Get the generated password with:

   ```bash
   docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```

4. Demo flow:

   | Step | Action | Expected |
   |---|---|---|
   | 1 | Unpause and trigger `articles_backfill` | One mapped task per month (2016-01 → now). Each closed month's source count equals the warehouse count. |
   | 2 | `docker compose exec source-db sh -c 'psql -U "$POSTGRES_USER" -d kumparan -f /sql/simulate_changes.sql'` | 200 inserts, 300 edits, 100 soft deletes and 50 hard deletes in the source |
   | 3 | Unpause or trigger `articles_hourly` | Inserts, edits and soft deletes appear in the warehouse. The 50 hard-deleted ids are **still** there. |
   | 4 | Trigger `articles_reconcile` | The 50 ids are tombstoned, and source and warehouse match exactly. |

   check the result in ClickHouse:

   ```bash (or just connect through DBMS client)
   docker compose exec clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "SELECT count() FROM raw.articles FINAL"'
   docker compose exec source-db sh -c 'psql -U "$POSTGRES_USER" -d kumparan -c "SELECT count(*) FROM articles"'
   ```
    
## How it works

| DAG | Schedule | What it does |
|---|---|---|
| `articles_hourly` | `@hourly` | Incremental load of rows with `updated_at >= max(updated_at in warehouse) - 15 min` |
| `articles_backfill` | manual, run once | History since 2016, one mapped task per month (at most 4 at a time) plus a count check per month |
| `articles_reconcile` | `@daily` | Compares id buckets on both sides. It tombstones hard deletes and re-pulls missing rows. |

The warehouse table is `raw.articles`, a `ReplacingMergeTree(_version, is_deleted)` ordered by `id`.
Every write is an insert, and the row with the highest `_version` wins, so it's idempotent. Retries, the overlap window and a re-run backfill never create duplicates.
Analysts query `analytics.articles`, which shows live articles only.

## Answer to "bonus" section 

1: The ETL/ELT that you made is new, and the data that is in the database is from 2016. What
should you consider?

- use backfill to manually ingest data. loads a few months at a time in paralel(4 in this case).  
- use server side cursor in code and load the data in batches so in doesn't hog client service's memory
- old data might be dirty and include null values. catch the cases and clean it
- verify each month loads, source count and warehouse count must match, or the task fails.

2: What if the table design is using a `hard delete` method. That means once the data is deleted,
the row is also get deleted. So the data in the data warehouse and in the database should
sync.

my solution : `articles_reconcile` DAG.

1. Group ids into buckets of 100k. On both sides, compute `count` and `sum(id)` per bucket. On the source this is an index-only scan.
2. Only for buckets that differ, fetch both id sets and diff them.
3. Ids that exist in the warehouse but not the source are re-checked against the source, then **tombstoned** (`is_deleted = 1`, `_version = now()`). The history stays auditable in `raw.articles`, and `FINAL` hides those rows.
4. Ids that exist in the source but not the warehouse are re-pulled. This also heals anything an hourly run missed.

## AI usage disclosure

- **Claude (Anthropic), via Claude Code**, helped me:
  - brainstorm the approach to the problem
  - discuss the stack and architecture
  - help simulate data through sql scripts
  - generate code (compose file, SQL, DAGs)
  - helped documentations
- **My own work:** 
  - deciding the stack and architecture for the best solution
  - ran, checked, and monitor the demo
  - questions the design and code choices
  - edited the readme/documentations to be more accurate and representative
