ALTER TABLE players ADD COLUMN IF NOT EXISTS display_name TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS short_name TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS native_name TEXT;

UPDATE players
SET display_name = trim(coalesce(given_name || ' ', '') || coalesce(family_name, ''))
WHERE (display_name IS NULL OR display_name = '')
  AND (given_name IS NOT NULL OR family_name IS NOT NULL);

UPDATE players
SET display_name = player_id
WHERE display_name IS NULL OR display_name = '';

ALTER TABLE players ALTER COLUMN display_name SET NOT NULL;
