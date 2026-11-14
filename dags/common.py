import logging
from collections import Counter
from contextlib import closing

import clickhouse_connect
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook

log = logging.getLogger(__name__)

SOURCE_CONN_ID = "source_pg"
WAREHOUSE_CONN_ID = "warehouse_ch"
BATCH_SIZE = 50_000

SRC_COLUMNS = ["id", "title", "content", "published_at", "author_id", "created_at", "updated_at", "deleted_at"]
WH_COLUMNS = SRC_COLUMNS + ["is_deleted", "_version"]


def source_conn():
    conn = PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn()
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("SET statement_timeout = '30min'")
    return conn


def warehouse_client():
    c = BaseHook.get_connection(WAREHOUSE_CONN_ID)
    return clickhouse_connect.get_client(
        host=c.host,
        port=c.port,
        username=c.login,
        password=c.password,
        database=c.schema,
        connect_timeout=10,
        send_receive_timeout=600,
    )


def clean(row, stats):
    id_, title, content, published_at, author_id, created_at, updated_at, deleted_at = row
    title = (title or "").strip() or None
    if title is None:
        stats["blank_title"] += 1
    if author_id is None:
        stats["null_author"] += 1
    if published_at is not None and published_at < created_at:
        stats["published_before_created"] += 1
    return [id_, title, content, published_at, author_id, created_at, updated_at, deleted_at, 0, updated_at]


def copy_articles(where, params, ch):
    """Stream source rows matching `where` into raw.articles. `where` must be a constant, never user input."""
    stats = Counter()
    sql = f"SELECT {', '.join(SRC_COLUMNS)} FROM articles WHERE {where}"
    with closing(source_conn()) as conn, conn.cursor(name="extract_articles") as cur:
        cur.itersize = BATCH_SIZE
        cur.execute(sql, params)
        while rows := cur.fetchmany(BATCH_SIZE):
            ch.insert("articles", [clean(r, stats) for r in rows], column_names=WH_COLUMNS)
            stats["rows"] += len(rows)
    log.info("copied articles where %s %s: %s", where, params, dict(stats))
    return stats["rows"]
