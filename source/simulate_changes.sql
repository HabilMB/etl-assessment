-- Simulates an hour of activity: inserts, edits, soft deletes and hard deletes.
-- Usage: psql -f /sql/simulate_changes.sql
BEGIN;

INSERT INTO articles (title, content, published_at, author_id)
SELECT 'Fresh article ' || g, repeat(md5(random()::text) || ' ', 20), now(), 1 + floor(random() * 5000)::bigint
FROM generate_series(1, 200) g;

-- edits hit old rows too , the hourly job must pick them up via updated_at
UPDATE articles SET title = title || ' (edited)'
WHERE id IN (SELECT id FROM articles WHERE deleted_at IS NULL ORDER BY random() LIMIT 300);

UPDATE articles SET deleted_at = now()
WHERE id IN (SELECT id FROM articles WHERE deleted_at IS NULL ORDER BY random() LIMIT 100);

CREATE TEMP TABLE hard_deleted ON COMMIT DROP AS
SELECT id FROM articles ORDER BY random() LIMIT 50;

DELETE FROM articles WHERE id IN (SELECT id FROM hard_deleted);

SELECT count(*) AS hard_deleted_ids, min(id), max(id) FROM hard_deleted;

COMMIT;
