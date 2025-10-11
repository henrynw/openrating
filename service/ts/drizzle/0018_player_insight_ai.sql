CREATE TABLE IF NOT EXISTS player_insight_ai (
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  sport TEXT,
  discipline TEXT,
  scope_key TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  narrative TEXT,
  model TEXT,
  tokens_prompt INTEGER,
  tokens_completion INTEGER,
  tokens_total INTEGER,
  generated_at TIMESTAMPTZ,
  last_requested_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  poll_after_ms INTEGER,
  error_code TEXT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (player_id, organization_id, scope_key, prompt_version)
);

CREATE INDEX IF NOT EXISTS player_insight_ai_digest_idx ON player_insight_ai (snapshot_digest);

CREATE TABLE IF NOT EXISTS player_insight_ai_jobs (
  job_id TEXT PRIMARY KEY,
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  sport TEXT,
  discipline TEXT,
  scope_key TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS player_insight_ai_jobs_run_at_idx ON player_insight_ai_jobs (run_at);
CREATE INDEX IF NOT EXISTS player_insight_ai_jobs_status_idx ON player_insight_ai_jobs (status);

CREATE UNIQUE INDEX IF NOT EXISTS player_insight_ai_jobs_unique_scope ON player_insight_ai_jobs (
  player_id,
  organization_id,
  scope_key,
  prompt_version
) WHERE status IN ('PENDING', 'IN_PROGRESS');
