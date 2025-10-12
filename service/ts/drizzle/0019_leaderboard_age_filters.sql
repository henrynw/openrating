ALTER TABLE players
  ADD COLUMN IF NOT EXISTS birth_date DATE;

UPDATE players
SET birth_date = MAKE_DATE(birth_year, 1, 1)
WHERE birth_year IS NOT NULL
  AND birth_date IS NULL;

ALTER TABLE rating_ladders
  ADD COLUMN IF NOT EXISTS default_age_cutoff DATE,
  ADD COLUMN IF NOT EXISTS age_bands JSONB;
