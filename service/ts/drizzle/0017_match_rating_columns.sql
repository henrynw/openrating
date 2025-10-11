BEGIN;

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS winner_side text;

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS rating_status text;

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS rating_skip_reason text;

UPDATE matches
SET rating_status = 'RATED'
WHERE rating_status IS NULL;

ALTER TABLE matches
  ALTER COLUMN rating_status SET DEFAULT 'RATED';

ALTER TABLE matches
  ALTER COLUMN rating_status SET NOT NULL;

COMMIT;
