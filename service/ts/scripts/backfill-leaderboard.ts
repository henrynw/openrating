import { sql } from 'drizzle-orm';
import { getDb, getPool } from '../src/db/client.js';
import { playerRatings, playerRatingHistory } from '../src/db/schema.js';

const main = async () => {
  const db = getDb();
  const pool = getPool();

  const normalized = await db.execute(sql`
    WITH match_counts AS (
      SELECT
        ${playerRatingHistory.playerId} AS player_id,
        ${playerRatingHistory.ladderId} AS ladder_id,
        count(*)::int AS match_count
      FROM ${playerRatingHistory}
      GROUP BY ${playerRatingHistory.playerId}, ${playerRatingHistory.ladderId}
    )
    UPDATE ${playerRatings} AS pr
    SET matches_count = mc.match_count
    FROM match_counts mc
    WHERE pr.player_id = mc.player_id
      AND pr.ladder_id = mc.ladder_id
      AND pr.matches_count <> mc.match_count;
  `);

  const zeroed = await db.execute(sql`
    UPDATE ${playerRatings} AS pr
    SET matches_count = 0
    WHERE pr.matches_count <> 0
      AND NOT EXISTS (
        SELECT 1
        FROM ${playerRatingHistory} AS history
        WHERE history.player_id = pr.player_id
          AND history.ladder_id = pr.ladder_id
      );
  `);

  console.log(
    JSON.stringify(
      {
        normalized: normalized.rowCount ?? 0,
        zeroed: zeroed.rowCount ?? 0,
      },
      null,
      2
    )
  );

  await pool.end();
};

main().catch((err) => {
  console.error('leaderboard_backfill_failed', err);
  process.exitCode = 1;
});
