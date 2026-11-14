-- Synthetic articles from 2016-01-01 to now.
-- Usage: psql -v scale=1 -f /sql/seed.sql   (scale=1 ≈ 2M rows)
\if :{?scale}
\else
  \set scale 1
\endif

\timing on

INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
SELECT
    -- legacy rows: padded or blank titles
    CASE
        WHEN legacy AND r1 < 0.05 THEN '   '
        WHEN legacy AND r1 < 0.15 THEN '  Article ' || n || '  '
        ELSE 'Article ' || n || ' ' || left(md5(n::text), 8)
    END,
    repeat(md5(random()::text) || ' ', 6 + (random() * 40)::int),
    CASE
        WHEN r2 < 0.10 THEN NULL                                 -- draft
        WHEN legacy AND r2 < 0.13 THEN created - interval '1 day' -- legacy import: published before created
        ELSE created + (random() * interval '2 hours')
    END,
    CASE
        WHEN legacy AND r3 < 0.05 THEN NULL
        ELSE 1 + floor(power(random(), 3) * 5000)::bigint        -- few authors write most articles
    END,
    created,
    greatest(edited, deleted),  -- a soft delete also bumps updated_at
    deleted
FROM (
    SELECT *,
        CASE WHEN r4 < 0.20 THEN least(created + random() * interval '30 days', now()) ELSE created END AS edited,
        CASE WHEN r5 < 0.03 THEN least(created + random() * interval '60 days', now()) END AS deleted
    FROM (
    SELECT
        row_number() OVER (ORDER BY created) AS n,
        created,
        created < '2017-01-01' AS legacy,
        random() AS r1, random() AS r2, random() AS r3, random() AS r4, random() AS r5
    FROM (
        SELECT d + random() * interval '1 day' AS created
        FROM generate_series('2016-01-01'::timestamptz, now() - interval '1 day', interval '1 day') AS d,
             LATERAL generate_series(
                 1,
                 ((100 + 900 * extract(epoch FROM d - '2016-01-01') / extract(epoch FROM now() - '2016-01-01')) * :scale)::int
             )
    ) days
    ) numbered
) src
ORDER BY n;

ANALYZE articles;

SELECT count(*) AS rows, min(created_at), max(created_at) FROM articles;
