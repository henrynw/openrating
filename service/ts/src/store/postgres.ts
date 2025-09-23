import { randomUUID } from 'crypto';
import { and, eq, inArray, or, lt, lte, sql, desc } from 'drizzle-orm';
import type { PlayerState, MatchInput } from '../engine/types.js';
import { P } from '../engine/params.js';
import { getDb } from '../db/client.js';
import {
  matchGames,
  matchSidePlayers,
  matchSides,
  matches,
  organizations,
  playerRatings,
  playerRatingHistory,
  players,
  providers,
  ratingLadders,
  regions,
  sports,
  venues,
} from '../db/schema.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerUpdateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
  MatchUpdateInput,
  OrganizationUpdateInput,
} from './types.js';
import {
  PlayerLookupError,
  PlayerListQuery,
  PlayerListResult,
  MatchListQuery,
  MatchListResult,
  MatchSummary,
  MatchGameSummary,
  MatchSideSummary,
  OrganizationCreateInput,
  OrganizationListQuery,
  OrganizationListResult,
  OrganizationRecord,
  OrganizationLookupError,
  MatchLookupError,
} from './types.js';
import { buildLadderId, isDefaultRegion, toDbRegionId } from './helpers.js';

const now = () => new Date();
const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 200;

const clampLimit = (limit?: number) => {
  if (!limit || limit < 1) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
};

const combineFilters = (filters: any[]) => {
  if (!filters.length) return undefined;
  return filters.reduce((acc: any, filter: any) => (acc ? and(acc, filter) : filter), undefined as any);
};

const buildMatchCursor = (startTime: Date, matchId: string) => `${startTime.toISOString()}|${matchId}`;

const parseMatchCursor = (cursor: string) => {
  const [ts, id] = cursor.split('|');
  if (!ts || !id) return null;
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return null;
  return { startTime: date, matchId: id };
};

const slugify = (value: string) =>
  value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '') || randomUUID();

export class PostgresStore implements RatingStore {
  constructor(private readonly db = getDb()) {}

  private async getOrganizationRowById(id: string) {
    const rows = await this.db
      .select({
        organizationId: organizations.organizationId,
        name: organizations.name,
        slug: organizations.slug,
        description: organizations.description,
        createdAt: organizations.createdAt,
      })
      .from(organizations)
      .where(eq(organizations.organizationId, id))
      .limit(1);
    return rows.at(0) ?? null;
  }

  private async getOrganizationRowBySlug(slug: string) {
    const rows = await this.db
      .select({
        organizationId: organizations.organizationId,
        name: organizations.name,
        slug: organizations.slug,
        description: organizations.description,
        createdAt: organizations.createdAt,
      })
      .from(organizations)
      .where(eq(organizations.slug, slug))
      .limit(1);
    return rows.at(0) ?? null;
  }

  private toOrganizationRecord(row: {
    organizationId: string;
    name: string;
    slug: string;
    description: string | null;
    createdAt: Date | null;
  }): OrganizationRecord {
    return {
      organizationId: row.organizationId,
      name: row.name,
      slug: row.slug,
      description: row.description,
      createdAt: row.createdAt?.toISOString(),
    };
  }

  private toPlayerRecord(row: {
    playerId: string;
    organizationId: string;
    displayName: string;
    shortName: string | null;
    nativeName: string | null;
    externalRef: string | null;
    givenName: string | null;
    familyName: string | null;
    sex: string | null;
    birthYear: number | null;
    countryCode: string | null;
    regionId: string | null;
  }): PlayerRecord {
    return {
      playerId: row.playerId,
      organizationId: row.organizationId,
      displayName: row.displayName,
      shortName: row.shortName ?? undefined,
      nativeName: row.nativeName ?? undefined,
      externalRef: row.externalRef ?? undefined,
      givenName: row.givenName ?? undefined,
      familyName: row.familyName ?? undefined,
      sex: (row.sex ?? undefined) as 'M' | 'F' | 'X' | undefined,
      birthYear: row.birthYear ?? undefined,
      countryCode: row.countryCode ?? undefined,
      regionId: row.regionId ?? undefined,
    };
  }

  private async getMatchSummaryById(matchId: string): Promise<MatchSummary | null> {
    const rows = await this.db
      .select({
        matchId: matches.matchId,
        organizationId: matches.organizationId,
        sport: matches.sport,
        discipline: matches.discipline,
        format: matches.format,
        tier: matches.tier,
        startTime: matches.startTime,
        venueId: matches.venueId,
        regionId: matches.regionId,
      })
      .from(matches)
      .where(eq(matches.matchId, matchId))
      .limit(1);

    const matchRow = rows.at(0);
    if (!matchRow) return null;

    const sideRows = (await this.db
      .select({
        side: matchSides.side,
        playerId: matchSidePlayers.playerId,
        position: matchSidePlayers.position,
      })
      .from(matchSides)
      .innerJoin(matchSidePlayers, eq(matchSidePlayers.matchSideId, matchSides.id))
      .where(eq(matchSides.matchId, matchId))
      .orderBy(matchSides.side, matchSidePlayers.position)) as Array<{
        side: string;
        playerId: string;
        position: number;
      }>;

    const gameRows = (await this.db
      .select({
        gameNo: matchGames.gameNo,
        scoreA: matchGames.scoreA,
        scoreB: matchGames.scoreB,
      })
      .from(matchGames)
      .where(eq(matchGames.matchId, matchId))
      .orderBy(matchGames.gameNo)) as Array<{
        gameNo: number;
        scoreA: number;
        scoreB: number;
      }>;

    const sideMap = new Map<string, string[]>();
    for (const row of sideRows) {
      const players = sideMap.get(row.side) ?? [];
      players[row.position] = row.playerId;
      sideMap.set(row.side, players);
    }

    const games: MatchGameSummary[] = gameRows.map((row) => ({
      gameNo: row.gameNo,
      a: row.scoreA,
      b: row.scoreB,
    }));

    const sides: MatchSideSummary[] = ['A', 'B'].map((side) => ({
      side: side as 'A' | 'B',
      players: (sideMap.get(side) ?? []).filter((player): player is string => Boolean(player)),
    }));

    return {
      matchId: matchRow.matchId,
      organizationId: matchRow.organizationId,
      sport: matchRow.sport as MatchInput['sport'],
      discipline: matchRow.discipline as MatchInput['discipline'],
      format: matchRow.format,
      tier: matchRow.tier ?? undefined,
      startTime: matchRow.startTime.toISOString(),
      venueId: matchRow.venueId ?? null,
      regionId: matchRow.regionId ?? null,
      sides,
      games,
    };
  }

  private async assertOrganizationExists(id: string) {
    const org = await this.getOrganizationRowById(id);
    if (!org) throw new OrganizationLookupError(`Organization not found: ${id}`);
    return org;
  }

  private async ensureSport(id: string, tx = this.db) {
    await tx
      .insert(sports)
      .values({
        sportId: id,
        name: id,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: sports.sportId });
  }

  private async ensureProvider(id: string, tx = this.db) {
    await tx
      .insert(providers)
      .values({
        providerId: id,
        name: id,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: providers.providerId });
  }

  private async ensureRegion(regionId: string | null | undefined, organizationId: string, tx = this.db) {
    if (!regionId || isDefaultRegion(regionId)) return null;
    await this.assertOrganizationExists(organizationId);
    await tx
      .insert(regions)
      .values({
        regionId,
        organizationId,
        parentRegionId: null,
        type: 'CUSTOM',
        name: regionId,
        countryCode: null,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: regions.regionId });
    return regionId;
  }

  private async ensureVenue(
    venueId: string | null | undefined,
    organizationId: string,
    regionId: string | null,
    tx = this.db
  ) {
    if (!venueId) return null;
    await this.assertOrganizationExists(organizationId);
    await tx
      .insert(venues)
      .values({
        venueId,
        organizationId,
        regionId,
        name: venueId,
        address: null,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: venues.venueId });
    return venueId;
  }

  async createOrganization(input: OrganizationCreateInput): Promise<OrganizationRecord> {
    const organizationId = randomUUID();
    const slug = (input.slug ?? slugify(input.name)).toLowerCase();

    try {
      const [row] = await this.db
        .insert(organizations)
        .values({
          organizationId,
          name: input.name,
          slug,
          description: input.description ?? null,
          createdAt: now(),
          updatedAt: now(),
        })
        .returning({
          organizationId: organizations.organizationId,
          name: organizations.name,
          slug: organizations.slug,
          description: organizations.description,
          createdAt: organizations.createdAt,
        });
      return this.toOrganizationRecord(row);
    } catch (err: any) {
      if (err && typeof err === 'object' && 'code' in err && err.code === '23505') {
        throw new OrganizationLookupError(`Slug already in use: ${slug}`);
      }
      throw err;
    }
  }

  async updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord> {
    const updates: Record<string, any> = {};

    if (input.name !== undefined) {
      updates.name = input.name;
    }
    if (input.description !== undefined) {
      updates.description = input.description ?? null;
    }
    if (input.slug !== undefined) {
      updates.slug = input.slug.toLowerCase();
    }

    if (!Object.keys(updates).length) {
      const existing = await this.getOrganizationRowById(organizationId);
      if (!existing) {
        throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
      }
      return this.toOrganizationRecord(existing);
    }

    updates.updatedAt = now();

    try {
      const [row] = await this.db
        .update(organizations)
        .set(updates)
        .where(eq(organizations.organizationId, organizationId))
        .returning({
          organizationId: organizations.organizationId,
          name: organizations.name,
          slug: organizations.slug,
          description: organizations.description,
          createdAt: organizations.createdAt,
        });

      if (!row) {
        throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
      }

      return this.toOrganizationRecord(row);
    } catch (err: any) {
      if (err && typeof err === 'object' && 'code' in err && err.code === '23505') {
        const slug = input.slug?.toLowerCase();
        throw new OrganizationLookupError(`Slug already in use: ${slug}`);
      }
      throw err;
    }
  }

  async listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult> {
    const limit = clampLimit(query.limit);
    const filters: any[] = [];

    if (query.cursor) {
      filters.push(sql`${organizations.slug} > ${query.cursor}`);
    }

    if (query.q) {
      filters.push(sql`${organizations.name} ILIKE ${`%${query.q}%`}`);
    }

    const condition = combineFilters(filters);

    const rows = (await this.db
      .select({
        organizationId: organizations.organizationId,
        name: organizations.name,
        slug: organizations.slug,
        description: organizations.description,
        createdAt: organizations.createdAt,
      })
      .from(organizations)
      .where(condition)
      .orderBy(organizations.slug)
      .limit(limit + 1)) as Array<{
      organizationId: string;
      name: string;
      slug: string;
      description: string | null;
      createdAt: Date | null;
    }>;

    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const nextCursor = hasMore && page.length ? page[page.length - 1].slug : undefined;

    return {
      items: page.map((row) => this.toOrganizationRecord(row)),
      nextCursor,
    };
  }

  async getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null> {
    const row = await this.getOrganizationRowBySlug(slug);
    return row ? this.toOrganizationRecord(row) : null;
  }

  async getOrganizationById(id: string): Promise<OrganizationRecord | null> {
    const row = await this.getOrganizationRowById(id);
    return row ? this.toOrganizationRecord(row) : null;
  }

  async createPlayer(input: PlayerCreateInput): Promise<PlayerRecord> {
    const playerId = randomUUID();
    await this.assertOrganizationExists(input.organizationId);
    const regionId = await this.ensureRegion(input.regionId ?? null, input.organizationId);

    await this.db.insert(players).values({
      playerId,
      organizationId: input.organizationId,
      displayName: input.displayName,
      shortName: input.shortName,
      nativeName: input.nativeName,
      externalRef: input.externalRef,
      givenName: input.givenName,
      familyName: input.familyName,
      sex: input.sex,
      birthYear: input.birthYear,
      countryCode: input.countryCode,
      regionId,
      createdAt: now(),
      updatedAt: now(),
    });

    return {
      playerId,
      organizationId: input.organizationId,
      displayName: input.displayName,
      shortName: input.shortName,
      nativeName: input.nativeName,
      givenName: input.givenName,
      familyName: input.familyName,
      sex: input.sex,
      birthYear: input.birthYear,
      countryCode: input.countryCode,
      regionId: regionId ?? undefined,
      externalRef: input.externalRef,
    } satisfies PlayerRecord;
  }

  async updatePlayer(playerId: string, organizationId: string, input: PlayerUpdateInput): Promise<PlayerRecord> {
    const selection = {
      playerId: players.playerId,
      organizationId: players.organizationId,
      displayName: players.displayName,
      shortName: players.shortName,
      nativeName: players.nativeName,
      externalRef: players.externalRef,
      givenName: players.givenName,
      familyName: players.familyName,
      sex: players.sex,
      birthYear: players.birthYear,
      countryCode: players.countryCode,
      regionId: players.regionId,
    } as const;

    const existingRows = await this.db
      .select(selection)
      .from(players)
      .where(eq(players.playerId, playerId))
      .limit(1);

    const existing = existingRows.at(0);
    if (!existing) {
      throw new PlayerLookupError(`Player not found: ${playerId}`, { missing: [playerId] });
    }
    if (existing.organizationId !== organizationId) {
      throw new PlayerLookupError(
        `Player not registered to organization ${organizationId}: ${playerId}`,
        { wrongOrganization: [playerId] }
      );
    }

    const updates: Record<string, any> = {};

    if (input.displayName !== undefined) updates.displayName = input.displayName;
    if (input.shortName !== undefined) updates.shortName = input.shortName ?? null;
    if (input.nativeName !== undefined) updates.nativeName = input.nativeName ?? null;
    if (input.externalRef !== undefined) updates.externalRef = input.externalRef ?? null;
    if (input.givenName !== undefined) updates.givenName = input.givenName ?? null;
    if (input.familyName !== undefined) updates.familyName = input.familyName ?? null;
    if (input.sex !== undefined) updates.sex = input.sex ?? null;
    if (input.birthYear !== undefined) updates.birthYear = input.birthYear ?? null;
    if (input.countryCode !== undefined) updates.countryCode = input.countryCode ?? null;

    if (input.regionId !== undefined) {
      if (!input.regionId) {
        updates.regionId = null;
      } else {
        updates.regionId = await this.ensureRegion(input.regionId, organizationId);
      }
    }

    if (!Object.keys(updates).length) {
      return this.toPlayerRecord(existing);
    }

    updates.updatedAt = now();

    const [row] = await this.db
      .update(players)
      .set(updates)
      .where(and(eq(players.playerId, playerId), eq(players.organizationId, organizationId)))
      .returning(selection);

    if (!row) {
      throw new PlayerLookupError(`Player not found: ${playerId}`, { missing: [playerId] });
    }

    return this.toPlayerRecord(row);
  }

  private async ensureLadder(key: LadderKey) {
    const ladderId = buildLadderId(key);

    await this.assertOrganizationExists(key.organizationId);
    await this.ensureSport(key.sport);
    const regionId = await this.ensureRegion(key.regionId, key.organizationId);

    await this.db
      .insert(ratingLadders)
      .values({
        ladderId,
        organizationId: key.organizationId,
        sport: key.sport,
        discipline: key.discipline,
        format: key.format,
        tier: key.tier,
        regionId: toDbRegionId(key.regionId) ?? regionId,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: ratingLadders.ladderId });

    return ladderId;
  }

  async ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult> {
    const ladderId = await this.ensureLadder(ladderKey);
    if (ids.length === 0) return { ladderId, players: new Map() };

    await this.assertOrganizationExists(ladderKey.organizationId);

    const playerRows = (await this.db
      .select({
        playerId: players.playerId,
        organizationId: players.organizationId,
      })
      .from(players)
      .where(inArray(players.playerId, ids))) as Array<{ playerId: string; organizationId: string }>;

    const found = new Set(playerRows.map((row) => row.playerId));
    const missing = ids.filter((id) => !found.has(id));
    if (missing.length) {
      throw new PlayerLookupError(`Players not found: ${missing.join(', ')}`, { missing });
    }

    const wrongOrg = playerRows
      .filter((row) => row.organizationId !== ladderKey.organizationId)
      .map((row) => row.playerId);
    if (wrongOrg.length) {
      throw new PlayerLookupError(
        `Players not registered to organization ${ladderKey.organizationId}: ${wrongOrg.join(', ')}`,
        { wrongOrganization: wrongOrg }
      );
    }

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

    await this.ensureProvider(params.submissionMeta.providerId);
    await this.assertOrganizationExists(params.submissionMeta.organizationId);
    await this.ensureSport(params.match.sport);

    const ladderRegionId = await this.ensureRegion(params.ladderKey.regionId, params.ladderKey.organizationId);
    const submissionRegionId = await this.ensureRegion(
      params.submissionMeta.regionId ?? null,
      params.submissionMeta.organizationId
    );
    const venueId = await this.ensureVenue(
      params.submissionMeta.venueId ?? null,
      params.submissionMeta.organizationId,
      submissionRegionId ?? ladderRegionId ?? null
    );

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
        venueId,
        regionId: submissionRegionId ?? ladderRegionId ?? null,
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

  async updateMatch(matchId: string, organizationId: string, input: MatchUpdateInput): Promise<MatchSummary> {
    const existingRows = await this.db
      .select({
        matchId: matches.matchId,
        organizationId: matches.organizationId,
        regionId: matches.regionId,
        venueId: matches.venueId,
      })
      .from(matches)
      .where(eq(matches.matchId, matchId))
      .limit(1);

    const existing = existingRows.at(0);
    if (!existing) {
      throw new MatchLookupError(`Match not found: ${matchId}`);
    }
    if (existing.organizationId !== organizationId) {
      throw new MatchLookupError(`Match does not belong to organization ${organizationId}`);
    }

    const updates: Record<string, any> = {};

    if (input.startTime !== undefined) {
      const date = new Date(input.startTime);
      if (Number.isNaN(date.getTime())) {
        throw new MatchLookupError('Invalid start time provided');
      }
      updates.startTime = date;
    }

    let nextRegionId = existing.regionId;
    if (input.regionId !== undefined) {
      if (!input.regionId) {
        updates.regionId = null;
        nextRegionId = null;
      } else {
        nextRegionId = await this.ensureRegion(input.regionId, organizationId);
        updates.regionId = nextRegionId;
      }
    }

    if (input.venueId !== undefined) {
      if (!input.venueId) {
        updates.venueId = null;
      } else {
        updates.venueId = await this.ensureVenue(input.venueId, organizationId, nextRegionId ?? null);
      }
    }

    if (Object.keys(updates).length) {
      const [row] = await this.db
        .update(matches)
        .set(updates)
        .where(and(eq(matches.matchId, matchId), eq(matches.organizationId, organizationId)))
        .returning({ matchId: matches.matchId });

      if (!row) {
        throw new MatchLookupError(`Match not found: ${matchId}`);
      }
    }

    const summary = await this.getMatchSummaryById(matchId);
    if (!summary) {
      throw new MatchLookupError(`Match not found: ${matchId}`);
    }
    return summary;
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

  async listPlayers(query: PlayerListQuery): Promise<PlayerListResult> {
    const limit = clampLimit(query.limit);
    await this.assertOrganizationExists(query.organizationId);
    const filters: any[] = [eq(players.organizationId, query.organizationId)];

    if (query.cursor) {
      filters.push(sql`${players.playerId} > ${query.cursor}`);
    }

    if (query.q) {
      filters.push(sql`${players.displayName} ILIKE ${`%${query.q}%`}`);
    }

    const condition = combineFilters(filters);

    let playerQuery = this.db
      .select({
        playerId: players.playerId,
        organizationId: players.organizationId,
        displayName: players.displayName,
        shortName: players.shortName,
        nativeName: players.nativeName,
        givenName: players.givenName,
        familyName: players.familyName,
        sex: players.sex,
        birthYear: players.birthYear,
        countryCode: players.countryCode,
        regionId: players.regionId,
        externalRef: players.externalRef,
      })
      .from(players)
      .orderBy(players.playerId)
      .limit(limit + 1);

    if (condition) {
      playerQuery = playerQuery.where(condition);
    }

    const rows = (await playerQuery) as Array<{
      playerId: string;
      organizationId: string;
      displayName: string;
        shortName: string | null;
        nativeName: string | null;
        givenName: string | null;
        familyName: string | null;
        sex: string | null;
        birthYear: number | null;
        countryCode: string | null;
        regionId: string | null;
        externalRef: string | null;
      }>;

    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const nextCursor = hasMore && page.length ? page[page.length - 1].playerId : undefined;

    const items: PlayerRecord[] = page.map((row) => ({
      playerId: row.playerId,
      organizationId: row.organizationId,
      displayName: row.displayName,
      shortName: row.shortName ?? undefined,
      nativeName: row.nativeName ?? undefined,
      givenName: row.givenName ?? undefined,
      familyName: row.familyName ?? undefined,
      sex: (row.sex ?? undefined) as 'M' | 'F' | 'X' | undefined,
      birthYear: row.birthYear ?? undefined,
      countryCode: row.countryCode ?? undefined,
      regionId: row.regionId ?? undefined,
      externalRef: row.externalRef ?? undefined,
    }));

    return { items, nextCursor };
  }

  async listMatches(query: MatchListQuery): Promise<MatchListResult> {
    const limit = clampLimit(query.limit);
    await this.assertOrganizationExists(query.organizationId);
    const filters: any[] = [eq(matches.organizationId, query.organizationId)];

    if (query.sport) {
      filters.push(eq(matches.sport, query.sport));
    }

    if (query.startAfter) {
      const after = new Date(query.startAfter);
      if (!Number.isNaN(after.getTime())) {
        filters.push(sql`${matches.startTime} >= ${after}`);
      }
    }

    if (query.startBefore) {
      const before = new Date(query.startBefore);
      if (!Number.isNaN(before.getTime())) {
        filters.push(sql`${matches.startTime} <= ${before}`);
      }
    }

    if (query.cursor) {
      const parsed = parseMatchCursor(query.cursor);
      if (parsed) {
        filters.push(
          or(
            lt(matches.startTime, parsed.startTime),
            and(eq(matches.startTime, parsed.startTime), lt(matches.matchId, parsed.matchId))
          )
        );
      }
    }

    if (query.playerId) {
      filters.push(
        sql`EXISTS (SELECT 1 FROM ${matchSidePlayers} msp JOIN ${matchSides} ms ON ms.id = msp.match_side_id WHERE ms.match_id = ${matches.matchId} AND msp.player_id = ${query.playerId})`
      );
    }

    const condition = combineFilters(filters);

    let matchQuery = this.db
      .select({
        matchId: matches.matchId,
        organizationId: matches.organizationId,
        sport: matches.sport,
        discipline: matches.discipline,
        format: matches.format,
        tier: matches.tier,
        startTime: matches.startTime,
        venueId: matches.venueId,
        regionId: matches.regionId,
      })
      .from(matches)
      .orderBy(desc(matches.startTime), desc(matches.matchId))
      .limit(limit + 1);

    if (condition) {
      matchQuery = matchQuery.where(condition);
    }

    const rows = (await matchQuery) as Array<{
      matchId: string;
        organizationId: string;
        sport: string;
        discipline: string;
        format: string;
        tier: string | null;
        startTime: Date;
        venueId: string | null;
        regionId: string | null;
      }>;

    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const nextCursor = hasMore && page.length ? buildMatchCursor(page[page.length - 1].startTime, page[page.length - 1].matchId) : undefined;

    if (!page.length) {
      return { items: [], nextCursor: undefined };
    }

    const matchIds = page.map((row) => row.matchId);

    const sideRows = (await this.db
      .select({
        matchId: matchSides.matchId,
        side: matchSides.side,
        playerId: matchSidePlayers.playerId,
      })
      .from(matchSides)
      .innerJoin(matchSidePlayers, eq(matchSidePlayers.matchSideId, matchSides.id))
      .where(inArray(matchSides.matchId, matchIds))) as Array<{
        matchId: string;
        side: string;
        playerId: string;
      }>;

    const gameRows = (await this.db
      .select({
        matchId: matchGames.matchId,
        gameNo: matchGames.gameNo,
        scoreA: matchGames.scoreA,
        scoreB: matchGames.scoreB,
      })
      .from(matchGames)
      .where(inArray(matchGames.matchId, matchIds))
      .orderBy(matchGames.matchId, matchGames.gameNo)) as Array<{
        matchId: string;
        gameNo: number;
        scoreA: number;
        scoreB: number;
      }>;

    const sidesMap = new Map<string, Map<string, string[]>>();
    for (const row of sideRows) {
      const sideMap = sidesMap.get(row.matchId) ?? new Map<string, string[]>();
      const playersList = sideMap.get(row.side) ?? [];
      playersList.push(row.playerId);
      sideMap.set(row.side, playersList);
      sidesMap.set(row.matchId, sideMap);
    }

    const gamesMap = new Map<string, MatchGameSummary[]>();
    for (const row of gameRows) {
      const list = gamesMap.get(row.matchId) ?? [];
      list.push({ gameNo: row.gameNo, a: row.scoreA, b: row.scoreB });
      gamesMap.set(row.matchId, list);
    }

    const items: MatchSummary[] = page.map((row) => {
      const sideMap = sidesMap.get(row.matchId) ?? new Map<string, string[]>();
      const sides: MatchSideSummary[] = ['A', 'B'].map((side) => ({
        side: side as 'A' | 'B',
        players: sideMap.get(side) ?? [],
      }));
      const gameList = gamesMap.get(row.matchId) ?? [];
      return {
        matchId: row.matchId,
        organizationId: row.organizationId,
        sport: row.sport as MatchInput['sport'],
        discipline: row.discipline as MatchInput['discipline'],
        format: row.format,
        tier: row.tier ?? undefined,
        startTime: row.startTime.toISOString(),
        venueId: row.venueId ?? null,
        regionId: row.regionId ?? null,
        sides,
        games: gameList,
      };
    });

    return { items, nextCursor };
  }
}
