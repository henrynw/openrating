CREATE TABLE IF NOT EXISTS events (
  event_id text PRIMARY KEY,
  organization_id text NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  type text NOT NULL,
  name text NOT NULL,
  slug text NOT NULL,
  description text,
  start_date timestamptz,
  end_date timestamptz,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS events_org_slug_idx ON events (organization_id, slug);

CREATE TABLE IF NOT EXISTS event_participants (
  event_id text NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
  player_id text NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
  seed integer,
  status text,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, player_id)
);

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS event_id text;

ALTER TABLE matches
  ADD CONSTRAINT matches_event_id_fkey FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS matches_event_id_idx ON matches (event_id);
