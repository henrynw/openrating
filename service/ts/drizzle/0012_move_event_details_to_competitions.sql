BEGIN;

ALTER TABLE competitions
  ADD COLUMN IF NOT EXISTS classification jsonb,
  ADD COLUMN IF NOT EXISTS purse double precision,
  ADD COLUMN IF NOT EXISTS purse_currency text,
  ADD COLUMN IF NOT EXISTS media_links jsonb;

ALTER TABLE events
  DROP COLUMN IF EXISTS classification,
  DROP COLUMN IF EXISTS purse,
  DROP COLUMN IF EXISTS purse_currency,
  DROP COLUMN IF EXISTS media_links;

DROP TABLE IF EXISTS event_participants;

CREATE TABLE IF NOT EXISTS competition_participants (
  competition_id text NOT NULL REFERENCES competitions(competition_id) ON DELETE CASCADE,
  player_id text NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  seed integer,
  status text,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (competition_id, player_id)
);

COMMIT;
