CREATE TABLE IF NOT EXISTS rating_replay_queue (
  ladder_id TEXT PRIMARY KEY REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  earliest_start_time TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
