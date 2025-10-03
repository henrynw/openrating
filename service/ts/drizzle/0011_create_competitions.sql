BEGIN;

CREATE TABLE IF NOT EXISTS competitions (
  competition_id text PRIMARY KEY,
  event_id text NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
  organization_id text NOT NULL REFERENCES organizations(organization_id) ON DELETE CASCADE,
  name text NOT NULL,
  slug text NOT NULL,
  sport text,
  discipline text,
  format text,
  tier text,
  status text,
  draw_size integer,
  start_date timestamptz,
  end_date timestamptz,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, slug)
);

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS competition_id text REFERENCES competitions(competition_id);

WITH match_groups AS (
  SELECT
    m.event_id,
    m.organization_id,
    m.sport,
    m.discipline,
    m.format,
    m.tier,
    MIN(m.start_time) AS first_start,
    MAX(m.start_time) AS last_start,
    COUNT(*) AS match_count
  FROM matches m
  WHERE m.event_id IS NOT NULL
  GROUP BY m.event_id, m.organization_id, m.sport, m.discipline, m.format, m.tier
),
inserted AS (
  INSERT INTO competitions (
    competition_id,
    event_id,
    organization_id,
    name,
    slug,
    sport,
    discipline,
    format,
    tier,
    status,
    draw_size,
    start_date,
    end_date,
    metadata
  )
  SELECT
    md5(mg.event_id || ':' || COALESCE(mg.sport, '') || ':' || COALESCE(mg.discipline, '') || ':' || COALESCE(mg.format, '') || ':' || COALESCE(mg.tier, '')),
    mg.event_id,
    mg.organization_id,
    CONCAT_WS(' - ', e.name, COALESCE(mg.format, mg.discipline, mg.sport, 'Competition')),
    LOWER(regexp_replace(CONCAT_WS('-', COALESCE(mg.format, mg.discipline, mg.sport, 'competition')), '[^a-z0-9]+', '-', 'g')) || '-' || SUBSTRING(md5(mg.event_id || COALESCE(mg.format, '') || COALESCE(mg.discipline, '') || COALESCE(mg.sport, '')) FOR 6),
    mg.sport,
    mg.discipline,
    mg.format,
    mg.tier,
    NULL,
    NULL,
    mg.first_start,
    mg.last_start,
    jsonb_build_object('match_count', mg.match_count)
  FROM match_groups mg
  JOIN events e ON e.event_id = mg.event_id
  ON CONFLICT DO NOTHING
  RETURNING competition_id, event_id, sport, discipline, format, tier
)
UPDATE matches m
SET competition_id = ins.competition_id
FROM inserted ins
WHERE m.event_id = ins.event_id
  AND m.sport IS NOT DISTINCT FROM ins.sport
  AND m.discipline IS NOT DISTINCT FROM ins.discipline
  AND m.format IS NOT DISTINCT FROM ins.format
  AND m.tier IS NOT DISTINCT FROM ins.tier
  AND m.competition_id IS NULL;

COMMIT;
