ALTER TABLE events
  ADD COLUMN IF NOT EXISTS provider_id TEXT,
  ADD COLUMN IF NOT EXISTS external_ref TEXT;

ALTER TABLE competitions
  ADD COLUMN IF NOT EXISTS provider_id TEXT,
  ADD COLUMN IF NOT EXISTS external_ref TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS events_provider_external_ref_idx
  ON events (provider_id, external_ref)
  WHERE external_ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS competitions_provider_external_ref_idx
  ON competitions (provider_id, external_ref)
  WHERE external_ref IS NOT NULL;
