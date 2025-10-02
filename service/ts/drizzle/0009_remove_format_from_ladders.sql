-- Remove format field from rating_ladders to consolidate ratings by sport+discipline only
-- This allows a player to have a single rating across all formats within a discipline
-- (e.g., MS and BS badminton singles players share the same rating)

-- Step 1: For each group of duplicate ladders (same org+sport+discipline+tier+region),
-- keep the one with the most matches and merge others into it

DO $$
DECLARE
  dup_record RECORD;
  keeper_ladder_id TEXT;
  duplicate_ladder_ids TEXT[];
BEGIN
  -- Find groups of ladders that will become duplicates after removing format
  FOR dup_record IN
    SELECT
      organization_id,
      sport,
      discipline,
      tier,
      COALESCE(region_id, 'GLOBAL') as region_key,
      array_agg(ladder_id ORDER BY
        (SELECT COUNT(*) FROM matches m WHERE m.ladder_id = rating_ladders.ladder_id) DESC,
        created_at ASC
      ) as ladder_ids
    FROM rating_ladders
    GROUP BY organization_id, sport, discipline, tier, COALESCE(region_id, 'GLOBAL')
    HAVING COUNT(*) > 1
  LOOP
    -- First ladder in array has most matches, keep it
    keeper_ladder_id := dup_record.ladder_ids[1];
    duplicate_ladder_ids := dup_record.ladder_ids[2:];

    RAISE NOTICE 'Consolidating ladders for %/%/% - keeping %, merging %',
      dup_record.sport, dup_record.discipline, dup_record.tier,
      keeper_ladder_id, duplicate_ladder_ids;

    -- Update all references to point to the keeper ladder
    UPDATE matches SET ladder_id = keeper_ladder_id
    WHERE ladder_id = ANY(duplicate_ladder_ids);

    UPDATE player_ratings SET ladder_id = keeper_ladder_id
    WHERE ladder_id = ANY(duplicate_ladder_ids)
    ON CONFLICT (player_id, ladder_id) DO UPDATE
      SET
        mu = GREATEST(player_ratings.mu, EXCLUDED.mu),
        sigma = LEAST(player_ratings.sigma, EXCLUDED.sigma),
        matches_count = player_ratings.matches_count + EXCLUDED.matches_count,
        updated_at = GREATEST(player_ratings.updated_at, EXCLUDED.updated_at);

    UPDATE player_rating_history SET ladder_id = keeper_ladder_id
    WHERE ladder_id = ANY(duplicate_ladder_ids);

    UPDATE pair_synergies SET ladder_id = keeper_ladder_id
    WHERE ladder_id = ANY(duplicate_ladder_ids)
    ON CONFLICT (ladder_id, pair_key) DO UPDATE
      SET
        gamma = GREATEST(pair_synergies.gamma, EXCLUDED.gamma),
        matches = pair_synergies.matches + EXCLUDED.matches,
        updated_at = GREATEST(pair_synergies.updated_at, EXCLUDED.updated_at);

    UPDATE pair_synergy_history SET ladder_id = keeper_ladder_id
    WHERE ladder_id = ANY(duplicate_ladder_ids);

    -- Delete the duplicate ladders
    DELETE FROM rating_ladders WHERE ladder_id = ANY(duplicate_ladder_ids);
  END LOOP;
END $$;

-- Step 2: Drop the old unique index
DROP INDEX IF EXISTS rating_ladders_org_format_idx;

-- Step 3: Drop the format column
ALTER TABLE rating_ladders DROP COLUMN IF EXISTS format;

-- Step 4: Create new unique index without format
CREATE UNIQUE INDEX IF NOT EXISTS rating_ladders_org_discipline_idx
  ON rating_ladders (organization_id, sport, discipline, tier, COALESCE(region_id, 'GLOBAL'));
