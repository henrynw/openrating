CREATE TABLE IF NOT EXISTS players (
  player_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  external_ref TEXT,
  given_name TEXT,
  family_name TEXT,
  sex TEXT,
  birth_year INTEGER,
  country_code TEXT,
  region_id TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS rating_ladders (
  ladder_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  sport TEXT NOT NULL,
  discipline TEXT NOT NULL,
  format TEXT NOT NULL,
  tier TEXT NOT NULL DEFAULT 'UNSPECIFIED',
  region_id TEXT NOT NULL DEFAULT 'GLOBAL',
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS rating_ladders_org_format_idx
  ON rating_ladders (organization_id, sport, discipline, format, tier, region_id);

CREATE TABLE IF NOT EXISTS matches (
  match_id TEXT PRIMARY KEY,
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  provider_id TEXT NOT NULL,
  organization_id TEXT NOT NULL,
  sport TEXT NOT NULL,
  discipline TEXT NOT NULL,
  format TEXT NOT NULL,
  tier TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  raw_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS match_sides (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  side TEXT NOT NULL,
  players_count INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS match_sides_match_side_idx
  ON match_sides (match_id, side);

CREATE TABLE IF NOT EXISTS match_side_players (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  match_side_id INTEGER NOT NULL REFERENCES match_sides(id) ON DELETE CASCADE,
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE RESTRICT,
  position INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS match_games (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  game_no INTEGER NOT NULL,
  score_a INTEGER NOT NULL,
  score_b INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS match_games_match_game_idx
  ON match_games (match_id, game_no);

CREATE TABLE IF NOT EXISTS player_ratings (
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  mu DOUBLE PRECISION NOT NULL,
  sigma DOUBLE PRECISION NOT NULL,
  matches_count INTEGER NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  PRIMARY KEY (player_id, ladder_id)
);

CREATE TABLE IF NOT EXISTS player_rating_history (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  player_id TEXT NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  mu_before DOUBLE PRECISION NOT NULL,
  mu_after DOUBLE PRECISION NOT NULL,
  sigma_after DOUBLE PRECISION NOT NULL,
  delta DOUBLE PRECISION NOT NULL,
  win_prob_pre DOUBLE PRECISION,
  mov_weight DOUBLE PRECISION,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
