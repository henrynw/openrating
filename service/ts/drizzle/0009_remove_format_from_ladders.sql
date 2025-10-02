-- Remove format field from rating_ladders to consolidate ratings by sport+discipline only
-- This allows a player to have a single rating across all formats within a discipline
-- (e.g., MS and BS badminton singles players share the same rating)

DROP INDEX IF EXISTS rating_ladders_org_format_idx;

ALTER TABLE rating_ladders DROP COLUMN IF EXISTS format;

CREATE UNIQUE INDEX IF NOT EXISTS rating_ladders_org_discipline_idx
  ON rating_ladders (organization_id, sport, discipline, tier, COALESCE(region_id, 'GLOBAL'));
