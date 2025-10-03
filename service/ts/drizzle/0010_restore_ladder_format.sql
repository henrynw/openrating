-- Restore the format column on rating_ladders and include it in the unique index.

BEGIN;

-- 1. Add the column back if it was dropped in 0009.
ALTER TABLE rating_ladders
  ADD COLUMN IF NOT EXISTS format text;

-- 2. Populate the column from historical match data where possible.
WITH latest_match_format AS (
  SELECT DISTINCT ON (ladder_id)
    ladder_id,
    format
  FROM matches
  WHERE format IS NOT NULL
  ORDER BY ladder_id, COALESCE(start_time, created_at) DESC NULLS LAST
)
UPDATE rating_ladders rl
SET format = lmf.format
FROM latest_match_format lmf
WHERE rl.ladder_id = lmf.ladder_id
  AND (rl.format IS NULL OR rl.format = '' OR rl.format = 'UNSPECIFIED');

-- 3. Default any remaining gaps to UNSPECIFIED.
UPDATE rating_ladders
SET format = 'UNSPECIFIED'
WHERE format IS NULL OR format = '';

-- 4. Ensure a default for future inserts and enforce NOT NULL.
ALTER TABLE rating_ladders
  ALTER COLUMN format SET DEFAULT 'UNSPECIFIED';

ALTER TABLE rating_ladders
  ALTER COLUMN format SET NOT NULL;

-- 5. Replace the uniqueness constraint to include format.
DROP INDEX IF EXISTS rating_ladders_org_discipline_idx;

CREATE UNIQUE INDEX rating_ladders_org_discipline_idx
  ON rating_ladders (organization_id, sport, discipline, format, tier, COALESCE(region_id, 'GLOBAL'));

COMMIT;
