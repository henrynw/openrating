ALTER TABLE player_rating_history
  ADD COLUMN IF NOT EXISTS sigma_before double precision;
