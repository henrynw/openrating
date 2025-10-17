-- Improve rating replay performance with missing indexes
CREATE INDEX IF NOT EXISTS matches_ladder_start_idx ON matches (ladder_id, start_time, match_id);
CREATE INDEX IF NOT EXISTS match_sides_match_id_idx ON match_sides (match_id);
CREATE INDEX IF NOT EXISTS match_side_players_match_side_idx ON match_side_players (match_side_id);
CREATE INDEX IF NOT EXISTS match_games_match_id_idx ON match_games (match_id, game_no);
CREATE INDEX IF NOT EXISTS player_ratings_ladder_idx ON player_ratings (ladder_id);
CREATE INDEX IF NOT EXISTS player_rating_history_ladder_match_idx ON player_rating_history (ladder_id, match_id);
CREATE INDEX IF NOT EXISTS pair_synergy_history_ladder_idx ON pair_synergy_history (ladder_id, pair_key);
