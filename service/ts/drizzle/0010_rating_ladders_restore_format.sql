-- Restore format column participation in unique index for rating ladders

BEGIN;

-- Drop the format-less unique index if it exists
DROP INDEX IF EXISTS rating_ladders_org_discipline_idx;

-- Recreate the unique index including format so ladders differ by draw code
CREATE UNIQUE INDEX rating_ladders_org_discipline_idx
  ON rating_ladders (organization_id, sport, discipline, format, tier, COALESCE(region_id, 'GLOBAL'));

COMMIT;
