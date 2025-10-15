-- Add indexes to support leaderboard and insights queries
CREATE INDEX IF NOT EXISTS idx_player_ratings_ladder_mu ON player_ratings (ladder_id, mu DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_player_ratings_ladder_matches ON player_ratings (ladder_id, matches_count DESC);
CREATE INDEX IF NOT EXISTS idx_players_org_country_sex ON players (organization_id, country_code, sex);
CREATE INDEX IF NOT EXISTS idx_player_rating_history_ladder_player_created ON player_rating_history (ladder_id, player_id, created_at DESC);

-- Optional helper for history lookups by player regardless of ladder
CREATE INDEX IF NOT EXISTS idx_player_rating_history_player_created ON player_rating_history (player_id, created_at DESC);
