CREATE TABLE IF NOT EXISTS organizations (
  organization_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS providers (
  provider_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS sports (
  sport_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS regions (
  region_id TEXT PRIMARY KEY,
  organization_id TEXT REFERENCES organizations(organization_id) ON DELETE CASCADE,
  parent_region_id TEXT REFERENCES regions(region_id) ON DELETE SET NULL,
  type TEXT NOT NULL,
  name TEXT NOT NULL,
  country_code TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS venues (
  venue_id TEXT PRIMARY KEY,
  organization_id TEXT REFERENCES organizations(organization_id) ON DELETE CASCADE,
  region_id TEXT REFERENCES regions(region_id) ON DELETE SET NULL,
  name TEXT NOT NULL,
  address TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
  player_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  external_ref TEXT,
  given_name TEXT,
  family_name TEXT,
  sex TEXT,
  birth_year INTEGER,
  country_code TEXT,
  region_id TEXT REFERENCES regions(region_id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS rating_ladders (
  ladder_id TEXT PRIMARY KEY,
  sport TEXT NOT NULL REFERENCES sports(sport_id) ON DELETE RESTRICT,
  discipline TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS rating_ladders_sport_discipline_idx
  ON rating_ladders (sport, discipline);

CREATE TABLE IF NOT EXISTS matches (
  match_id TEXT PRIMARY KEY,
  ladder_id TEXT NOT NULL REFERENCES rating_ladders(ladder_id) ON DELETE CASCADE,
  provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE RESTRICT,
  organization_id TEXT NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  sport TEXT NOT NULL REFERENCES sports(sport_id) ON DELETE RESTRICT,
  discipline TEXT NOT NULL,
  format TEXT NOT NULL,
  tier TEXT NOT NULL,
  venue_id TEXT REFERENCES venues(venue_id) ON DELETE SET NULL,
  region_id TEXT REFERENCES regions(region_id) ON DELETE SET NULL,
  start_time TIMESTAMPTZ NOT NULL,
  raw_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS matches_org_sport_idx
  ON matches (organization_id, sport, discipline, tier);

CREATE INDEX IF NOT EXISTS matches_start_time_idx
  ON matches (start_time DESC);

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
