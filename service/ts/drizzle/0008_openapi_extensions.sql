ALTER TABLE players
  ADD COLUMN IF NOT EXISTS competitive_profile jsonb,
  ADD COLUMN IF NOT EXISTS attributes jsonb;

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS classification jsonb,
  ADD COLUMN IF NOT EXISTS sanctioning_body text,
  ADD COLUMN IF NOT EXISTS season text,
  ADD COLUMN IF NOT EXISTS purse double precision,
  ADD COLUMN IF NOT EXISTS purse_currency text,
  ADD COLUMN IF NOT EXISTS media_links jsonb;

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS timing jsonb,
  ADD COLUMN IF NOT EXISTS statistics jsonb,
  ADD COLUMN IF NOT EXISTS segments jsonb,
  ADD COLUMN IF NOT EXISTS side_participants jsonb;

ALTER TABLE match_games
  ADD COLUMN IF NOT EXISTS statistics jsonb,
  ADD COLUMN IF NOT EXISTS segments jsonb;
