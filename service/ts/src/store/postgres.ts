import { randomUUID } from 'crypto';
import { and, eq, inArray } from 'drizzle-orm';
import type { PlayerState } from '../engine/types';
import { P } from '../engine/params';
import { getDb } from '../db/client';
import {
  matchGames,
  matchSidePlayers,
  matchSides,
  matches,
  playerRatings,
  playerRatingHistory,
  players,
  ratingLadders,
} from '../db/schema';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
} from './types';
import { buildLadderId } from './helpers';

const now = () => new Date();

const ensurePlayerShell = (id: string, organizationId: string) => ({
  playerId: id,
  organizationId,
  givenName: 'Unknown',
  familyName: 'Player',
});

export class PostgresStore implements RatingStore {
  constructor(private readonly db = getDb()) {}

  async createPlayer(input: PlayerCreateInput): Promise<PlayerRecord> {
    const playerId = randomUUID();
    const record = {
      playerId,
      organizationId: input.organizationId,
      externalRef: input.externalRef,
      givenName: input.givenName,
      familyName: input.familyName,
      sex: input.sex,
      birthYear: input.birthYear,
      countryCode: input.countryCode,
      regionId: input.regionId,
    } satisfies PlayerRecord;

    await this.db.insert(players).values({
      playerId,
      organizationId: input.organizationId,
      externalRef: input.externalRef,
      givenName: input.givenName,
      familyName: input.familyName,
      sex: input.sex,
      birthYear: input.birthYear,
      countryCode: input.countryCode,
      regionId: input.regionId,
      createdAt: now(),
      updatedAt: now(),
    });

    return record;
  }

  private async ensureLadder(key: LadderKey) {
    const ladderId = buildLadderId(key);

    await this.db
      .insert(ratingLadders)
      .values({
        ladderId,
        organizationId: key.organizationId,
        sport: key.sport,
        discipline: key.discipline,
        format: key.format,
        tier: key.tier,
        regionId: key.regionId,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: ratingLadders.ladderId });

    return ladderId;
  }

  async ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult> {
    const ladderId = await this.ensureLadder(ladderKey);
    if (ids.length === 0) return { ladderId, players: new Map() };

    await this.db
      .insert(players)
      .values(ids.map((id) => ({
        ...ensurePlayerShell(id, ladderKey.organizationId),
        createdAt: now(),
        updatedAt: now(),
      })))
      .onConflictDoNothing({ target: players.playerId });

    await this.db
      .insert(playerRatings)
      .values(
        ids.map((id) => ({
          playerId: id,
          ladderId,
          mu: P.baseMu,
          sigma: P.baseSigma,
          matchesCount: 0,
          updatedAt: now(),
        }))
      )
      .onConflictDoNothing({ target: [playerRatings.playerId, playerRatings.ladderId] });

    const rows = await this.db
      .select({
        playerId: playerRatings.playerId,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        matchesCount: playerRatings.matchesCount,
        regionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .where(
        and(
          eq(playerRatings.ladderId, ladderId),
          inArray(playerRatings.playerId, ids)
        )
      );

    const map = new Map<string, PlayerState>();
    for (const row of rows) {
      map.set(row.playerId, {
        playerId: row.playerId,
        mu: row.mu,
        sigma: row.sigma,
        matchesCount: row.matchesCount,
        regionId: row.regionId ?? undefined,
      });
    }

    return { ladderId, players: map };
  }

  async recordMatch(params: RecordMatchParams): Promise<{ matchId: string }> {
    const matchId = randomUUID();
    const movWeight = params.match.movWeight ?? null;

    await this.db.transaction(async (tx: any) => {
      await tx.insert(matches).values({
        matchId,
        ladderId: params.ladderId,
        providerId: params.submissionMeta.providerId,
        organizationId: params.submissionMeta.organizationId,
        sport: params.match.sport,
        discipline: params.match.discipline,
        format: params.match.format,
        tier: params.match.tier ?? 'UNSPECIFIED',
        startTime: new Date(params.submissionMeta.startTime),
        rawPayload: params.submissionMeta.rawPayload as object,
        createdAt: now(),
      });

      const sides: Array<{ key: 'A' | 'B'; players: string[] }> = [
        { key: 'A', players: params.match.sides.A.players },
        { key: 'B', players: params.match.sides.B.players },
      ];

      for (const side of sides) {
        const [sideRow] = await tx
          .insert(matchSides)
          .values({
            matchId,
            side: side.key,
            playersCount: side.players.length,
          })
          .returning({ id: matchSides.id });

        for (let idx = 0; idx < side.players.length; idx += 1) {
          await tx.insert(matchSidePlayers).values({
            matchSideId: sideRow.id,
            playerId: side.players[idx],
            position: idx,
          });
        }
      }

      for (const game of params.match.games) {
        await tx.insert(matchGames).values({
          matchId,
          gameNo: game.game_no,
          scoreA: game.a,
          scoreB: game.b,
        });
      }

      for (const entry of params.result.perPlayer) {
        const playerState = params.playerStates.get(entry.playerId);
        await tx
          .update(playerRatings)
          .set({
            mu: entry.muAfter,
            sigma: entry.sigmaAfter,
            matchesCount: playerState?.matchesCount ?? 0,
            updatedAt: now(),
          })
          .where(
            and(
              eq(playerRatings.playerId, entry.playerId),
              eq(playerRatings.ladderId, params.ladderId)
            )
          );

        await tx.insert(playerRatingHistory).values({
          playerId: entry.playerId,
          ladderId: params.ladderId,
          matchId,
          muBefore: entry.muBefore,
          muAfter: entry.muAfter,
          sigmaAfter: entry.sigmaAfter,
          delta: entry.delta,
          winProbPre: entry.winProbPre,
          movWeight,
          createdAt: now(),
        });
      }
    });

    return { matchId };
  }

  async getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null> {
    const ladderId = buildLadderId(ladderKey);
    const rows = await this.db
      .select({
        playerId: playerRatings.playerId,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        matchesCount: playerRatings.matchesCount,
        regionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .where(
        and(
          eq(playerRatings.ladderId, ladderId),
          eq(playerRatings.playerId, playerId)
        )
      );

    if (!rows.length) return null;
    const row = rows[0];
    return {
      playerId: row.playerId,
      mu: row.mu,
      sigma: row.sigma,
      matchesCount: row.matchesCount,
      regionId: row.regionId ?? undefined,
    };
  }
}
