CREATE TABLE IF NOT EXISTS subjects (
  subject_id TEXT PRIMARY KEY,
  auth_provider TEXT NOT NULL,
  display_name TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE TABLE IF NOT EXISTS subject_grants (
  id SERIAL PRIMARY KEY,
  subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON DELETE CASCADE,
  organization_id TEXT NOT NULL,
  sport TEXT,
  region_id TEXT,
  permission TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  UNIQUE(subject_id, organization_id, COALESCE(sport, ''), COALESCE(region_id, ''), permission)
);

CREATE INDEX IF NOT EXISTS subject_grants_subject_idx
  ON subject_grants (subject_id);

CREATE INDEX IF NOT EXISTS subject_grants_org_sport_idx
  ON subject_grants (organization_id, COALESCE(sport, ''), COALESCE(region_id, ''));
