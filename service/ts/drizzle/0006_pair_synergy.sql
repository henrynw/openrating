CREATE TABLE pair_synergies (
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL,
  players JSONB NOT NULL,
  gamma DOUBLE PRECISION NOT NULL DEFAULT 0,
  matches INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ladder_id, pair_key)
);

CREATE TABLE pair_synergy_history (
  id SERIAL PRIMARY KEY,
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL,
  match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  gamma_before DOUBLE PRECISION NOT NULL,
  gamma_after DOUBLE PRECISION NOT NULL,
  delta DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
