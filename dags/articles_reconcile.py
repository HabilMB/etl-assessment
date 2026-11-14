import logging
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from common import WH_COLUMNS, copy_articles, source_conn, warehouse_client

log = logging.getLogger(__name__)

BUCKET = 100_000

TOMBSTONE_SQL = f"""
INSERT INTO articles ({", ".join(WH_COLUMNS)})
SELECT {", ".join(WH_COLUMNS[:-2])}, 1, now64(3)
FROM articles FINAL
WHERE id IN {{ids:Array(UInt64)}}
"""


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["articles"],
)
def articles_reconcile():
    @task
    def reconcile():
        ch = warehouse_client()
        stats = {"buckets_checked": 0, "buckets_mismatched": 0, "repulled": 0, "tombstoned": 0}

        with closing(source_conn()) as conn, conn.cursor() as cur:
            cur.execute("SELECT id / %s, count(*), sum(id) FROM articles GROUP BY 1", (BUCKET,))
            src = {b: (c, int(s)) for b, c, s in cur.fetchall()}
            wh = {
                b: (c, int(s))
                for b, c, s in ch.query(
                    "SELECT intDiv(id, {b:UInt64}), count(), sum(id) FROM articles FINAL GROUP BY 1",
                    parameters={"b": BUCKET},
                ).result_rows
            }
            buckets = sorted(src.keys() | wh.keys())
            stats["buckets_checked"] = len(buckets)

            for b in buckets:
                if src.get(b) == wh.get(b):
                    continue
                stats["buckets_mismatched"] += 1
                lo, hi = b * BUCKET, (b + 1) * BUCKET

                cur.execute("SELECT id FROM articles WHERE id >= %s AND id < %s", (lo, hi))
                src_ids = {r[0] for r in cur.fetchall()}
                wh_ids = {
                    r[0]
                    for r in ch.query(
                        "SELECT id FROM articles FINAL WHERE id >= {lo:UInt64} AND id < {hi:UInt64}",
                        parameters={"lo": lo, "hi": hi},
                    ).result_rows
                }

                missing = sorted(src_ids - wh_ids)
                if missing:
                    stats["repulled"] += copy_articles("id = ANY(%(ids)s)", {"ids": missing}, ch)

                gone = wh_ids - src_ids
                if gone:
                    # re-check right before deleting: a row inserted after the bucket scan must not be tombstoned
                    cur.execute("SELECT id FROM articles WHERE id = ANY(%s)", (list(gone),))
                    gone -= {r[0] for r in cur.fetchall()}
                if gone:
                    ch.command(TOMBSTONE_SQL, parameters={"ids": sorted(gone)})
                    stats["tombstoned"] += len(gone)
                    log.info("bucket %s: tombstoned hard-deleted ids %s", b, sorted(gone))

        log.info("reconcile done: %s", stats)
        return stats

    reconcile()


articles_reconcile()
