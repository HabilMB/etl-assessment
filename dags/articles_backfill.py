from contextlib import closing
from datetime import date, datetime, timedelta, timezone

from airflow.sdk import dag, task

from common import copy_articles, source_conn, warehouse_client


def next_month(d):
    return (d.replace(day=1) + timedelta(days=32)).replace(day=1)


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["articles"],
)
def articles_backfill():
    @task
    def list_months():
        with closing(source_conn()) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT to_char(m, 'YYYY-MM-DD')
                FROM generate_series(
                    date_trunc('month', (SELECT min(created_at) FROM articles)),
                    date_trunc('month', now()),
                    interval '1 month') AS m
                """
            )
            return [r[0] for r in cur.fetchall()]

    # caps concurrent load on the production source
    @task(max_active_tis_per_dag=4)
    def load_month(month):
        start = date.fromisoformat(month)
        end = next_month(start)
        ch = warehouse_client()
        loaded = copy_articles(
            "created_at >= %(start)s AND created_at < %(end)s",
            {"start": start.isoformat(), "end": end.isoformat()},
            ch,
        )

        # the current month keeps changing under the hourly DAG, so only closed months are verified
        if end > date.today().replace(day=1):
            return loaded
        with closing(source_conn()) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM articles WHERE created_at >= %s AND created_at < %s",
                (start.isoformat(), end.isoformat()),
            )
            src_count = cur.fetchone()[0]
        wh_count = ch.query(
            "SELECT count() FROM articles FINAL WHERE toYYYYMM(created_at) = {m:UInt32}",
            parameters={"m": start.year * 100 + start.month},
        ).result_rows[0][0]
        if src_count != wh_count:
            raise ValueError(
                f"{month}: source has {src_count} rows, warehouse has {wh_count}. "
                "Retry this task; if it persists, run articles_reconcile."
            )
        return loaded

    load_month.expand(month=list_months())


articles_backfill()
