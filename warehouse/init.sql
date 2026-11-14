CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.articles
(
    id           UInt64,
    title        Nullable(String),
    content      Nullable(String),
    published_at Nullable(DateTime64(3, 'UTC')),
    author_id    Nullable(UInt64),
    created_at   DateTime64(3, 'UTC'),
    updated_at   DateTime64(3, 'UTC'),
    deleted_at   Nullable(DateTime64(3, 'UTC')),
    is_deleted   UInt8,
    _version     DateTime64(3, 'UTC'),
    _synced_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
-- yearly, not monthly: an hourly batch of edits can touch articles from any month since 2016,
-- and ClickHouse rejects inserts spanning >100 partitions
PARTITION BY toYear(created_at)
ORDER BY id;

CREATE VIEW IF NOT EXISTS analytics.articles AS
SELECT id, title, content, published_at, author_id, created_at, updated_at
FROM raw.articles FINAL
WHERE deleted_at IS NULL;
