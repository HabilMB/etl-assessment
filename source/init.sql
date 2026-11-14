CREATE TABLE articles (
    id           BIGSERIAL PRIMARY KEY,
    title        TEXT,
    content      TEXT,
    published_at TIMESTAMPTZ,
    author_id    BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ
);

CREATE INDEX articles_updated_at_idx ON articles (updated_at);
CREATE INDEX articles_created_at_idx ON articles (created_at);

-- The hourly ETL relies on updated_at moving on every change, so the DB enforces it
-- instead of trusting every writer to remember.
CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER articles_touch_updated_at
    BEFORE UPDATE ON articles
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
