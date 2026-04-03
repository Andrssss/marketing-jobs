-- Migration: Create marketing_job_posts table
-- Same database as job_posts, separate table for marketing/office/admin jobs

CREATE TABLE IF NOT EXISTS marketing_job_posts (
  id          SERIAL PRIMARY KEY,
  source      TEXT NOT NULL,
  title       TEXT NOT NULL,
  url         TEXT,
  experience  TEXT,
  first_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for keyword search on title
CREATE INDEX IF NOT EXISTS idx_marketing_job_posts_title_lower
  ON marketing_job_posts (LOWER(title));

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_marketing_job_posts_first_seen
  ON marketing_job_posts (first_seen DESC);

-- Index for source filtering
CREATE INDEX IF NOT EXISTS idx_marketing_job_posts_source
  ON marketing_job_posts (source);

-- Unique constraint to prevent duplicates (same source + url)
CREATE UNIQUE INDEX IF NOT EXISTS idx_marketing_job_posts_source_url
  ON marketing_job_posts (source, url)
  WHERE url IS NOT NULL;
