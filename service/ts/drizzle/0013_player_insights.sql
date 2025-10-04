CREATE TABLE IF NOT EXISTS player_insights (
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  sport TEXT,
  discipline TEXT,
  scope_key TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  snapshot JSONB NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  etag TEXT NOT NULL,
  digest TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (player_id, organization_id, scope_key)
);

CREATE TABLE IF NOT EXISTS player_insight_jobs (
  job_id TEXT PRIMARY KEY,
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  sport TEXT,
  discipline TEXT,
  scope_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  payload JSONB,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS player_insight_jobs_run_at_idx ON player_insight_jobs (run_at);
CREATE INDEX IF NOT EXISTS player_insight_jobs_status_idx ON player_insight_jobs (status);

CREATE UNIQUE INDEX IF NOT EXISTS player_insight_jobs_unique_scope ON player_insight_jobs (
  player_id,
  organization_id,
  scope_key
) WHERE status IN ('PENDING', 'IN_PROGRESS');
