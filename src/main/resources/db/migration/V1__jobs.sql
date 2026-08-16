CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  queue_name VARCHAR(80) NOT NULL,
  payload TEXT NOT NULL,
  status VARCHAR(30) NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
  next_attempt_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ
);
CREATE INDEX ix_jobs_status_created ON jobs(status, created_at DESC);
