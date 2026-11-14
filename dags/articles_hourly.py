import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from common import copy_articles, warehouse_client

log = logging.getLogger(__name__)

OVERLAP = timedelta(minutes=15)


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["articles"],
)
def articles_hourly():
    @task
    def sync_changes():
        ch = warehouse_client()
        watermark = ch.query("SELECT maxOrNull(updated_at) FROM articles").result_rows[0][0]
        if watermark is None:
            log.warning("warehouse is empty, loading only the last hour; run articles_backfill for history")
            since = datetime.now(timezone.utc) - timedelta(hours=1) - OVERLAP
        else:
            since = watermark - OVERLAP
        log.info("watermark=%s, extracting updated_at >= %s", watermark, since)
        return copy_articles("updated_at >= %(since)s", {"since": since}, ch)

    sync_changes()


articles_hourly()
