import { randomUUID } from 'crypto';
import { and, eq, inArray, or, lt, lte, gt, gte, sql, desc } from 'drizzle-orm';
import type { PlayerState, MatchInput, PairState, PairUpdate } from '../engine/types.js';
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
  pairSynergies,
  pairSynergyHistory,
} from '../db/schema.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerUpdateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
  RecordMatchResult,
  MatchUpdateInput,
  OrganizationUpdateInput,
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
  RatingEventListQuery,
  RatingEventListResult,
  RatingEventRecord,
  RatingSnapshot,
  EnsurePairSynergiesParams,
  EnsurePairSynergiesResult,
  NightlyStabilizationOptions,
  LeaderboardQuery,
  LeaderboardResult,
  LeaderboardEntry,
  LeaderboardMoversQuery,
  LeaderboardMoversResult,
  LeaderboardMoverEntry,
} from './types.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError } from './types.js';
import { buildLadderId, isDefaultRegion, toDbRegionId, DEFAULT_REGION } from './helpers.js';

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

const buildRatingEventCursor = (createdAt: Date, id: number) => `${createdAt.toISOString()}|${id}`;

const parseRatingEventCursor = (cursor: string) => {
  const [ts, idRaw] = cursor.split('|');
  if (!ts || !idRaw) return null;
  const createdAt = new Date(ts);
  if (Number.isNaN(createdAt.getTime())) return null;
  const id = Number(idRaw);
  if (!Number.isFinite(id)) return null;
  return { createdAt, id };
};

const clampValue = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));
const MS_PER_WEEK = 7 * 24 * 60 * 60 * 1000;

type RatingEventRow = {
  id: number;
  playerId: string;
  ladderId: string;
  matchId: string | null;
  createdAt: Date;
  muBefore: number;
  muAfter: number;
  delta: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  winProbPre: number | null;
  movWeight: number | null;
  organizationId: string;
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

  private toRatingEventRecord(row: RatingEventRow): RatingEventRecord {
    return {
      ratingEventId: String(row.id),
      organizationId: row.organizationId,
      playerId: row.playerId,
      ladderId: row.ladderId,
      matchId: row.matchId,
      appliedAt: row.createdAt.toISOString(),
      ratingSystem: 'OPENRATING_TRUESKILL_LITE',
      muBefore: row.muBefore,
      muAfter: row.muAfter,
      delta: row.delta,
      sigmaBefore: row.sigmaBefore ?? null,
      sigmaAfter: row.sigmaAfter,
      winProbPre: row.winProbPre ?? null,
      movWeight: row.movWeight ?? null,
      metadata: null,
    };
  }

  private async assertPlayerInOrganization(playerId: string, organizationId: string) {
    const rows = await this.db
      .select({
        playerId: players.playerId,
        organizationId: players.organizationId,
      })
      .from(players)
      .where(eq(players.playerId, playerId))
      .limit(1);

    const row = rows.at(0);
    if (!row) {
      throw new PlayerLookupError(`Player not found: ${playerId}`, { missing: [playerId] });
    }
    if (row.organizationId !== organizationId) {
      throw new PlayerLookupError(
        `Player not registered to organization ${organizationId}: ${playerId}`,
        { wrongOrganization: [playerId] }
      );
    }
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

  private async applyInactivity(tx: any, asOf: Date) {
    const rows = await tx
      .select({
        playerId: playerRatings.playerId,
        ladderId: playerRatings.ladderId,
        sigma: playerRatings.sigma,
        updatedAt: playerRatings.updatedAt,
      })
      .from(playerRatings);

    for (const row of rows) {
      const updatedAt = row.updatedAt ?? asOf;
      const weeks = Math.max(0, (asOf.getTime() - updatedAt.getTime()) / MS_PER_WEEK);
      if (weeks <= 0) continue;
      const factor = Math.pow(1 + P.idle.ratePerWeek, weeks);
      let nextVar = row.sigma * row.sigma * factor;
      nextVar = Math.min(P.sigmaMax * P.sigmaMax, nextVar);
      const nextSigma = Math.max(P.sigmaMin, Math.sqrt(nextVar));
      if (Math.abs(nextSigma - row.sigma) < 1e-6) continue;

      await tx
        .update(playerRatings)
        .set({ sigma: nextSigma, updatedAt: asOf })
        .where(
          and(
            eq(playerRatings.playerId, row.playerId),
            eq(playerRatings.ladderId, row.ladderId)
          )
        );
    }
  }

  private async applySynergyDecay(tx: any, asOf: Date) {
    const rows = await tx
      .select({
        ladderId: pairSynergies.ladderId,
        pairKey: pairSynergies.pairKey,
        gamma: pairSynergies.gamma,
        updatedAt: pairSynergies.updatedAt,
      })
      .from(pairSynergies);

    for (const row of rows) {
      const updatedAt = row.updatedAt ?? asOf;
      const weeks = Math.max(0, (asOf.getTime() - updatedAt.getTime()) / MS_PER_WEEK);
      if (weeks <= 0) continue;
      const decayFactor = Math.pow(Math.max(0, 1 - P.synergy.decayRatePerWeek), weeks);
      let gamma = clampValue(row.gamma * decayFactor, P.synergy.gammaMin, P.synergy.gammaMax);
      gamma = clampValue(gamma - gamma * P.synergy.regularization, P.synergy.gammaMin, P.synergy.gammaMax);

      await tx
        .update(pairSynergies)
        .set({ gamma, updatedAt: asOf })
        .where(
          and(
            eq(pairSynergies.ladderId, row.ladderId),
            eq(pairSynergies.pairKey, row.pairKey)
          )
        );
    }
  }

  private async applyRegionBias(tx: any, asOf: Date) {
    const rows = await tx
      .select({
        playerId: playerRatings.playerId,
        ladderId: playerRatings.ladderId,
        mu: playerRatings.mu,
        regionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId));

    if (!rows.length) return;

    const globalMean = rows.reduce((acc: number, row: any) => acc + row.mu, 0) / rows.length;
    const regionStats = new Map<string, { sum: number; count: number }>();

    for (const row of rows) {
      const regionId = row.regionId ?? DEFAULT_REGION;
      if (regionId === DEFAULT_REGION) continue;
      const stat = regionStats.get(regionId) ?? { sum: 0, count: 0 };
      stat.sum += row.mu;
      stat.count += 1;
      regionStats.set(regionId, stat);
    }

    const adjustments = new Map<string, number>();
    for (const [regionId, stat] of regionStats.entries()) {
      if (!stat.count) continue;
      const mean = stat.sum / stat.count;
      const shift = clampValue(mean - globalMean, -P.region.maxShiftPerDay, P.region.maxShiftPerDay);
      if (Math.abs(shift) < 1e-6) continue;
      adjustments.set(regionId, shift);
    }

    for (const [regionId, shift] of adjustments.entries()) {
      await tx
        .execute(sql`
          UPDATE ${playerRatings}
          SET mu = mu - ${shift}, updated_at = ${asOf}
          WHERE player_id IN (
            SELECT ${players.playerId}
            FROM ${players}
            WHERE ${players.regionId} = ${regionId}
          )
        `);
    }
  }

  private async applyGraphSmoothing(tx: any, asOf: Date, horizonDays: number) {
    const cutoff = new Date(asOf.getTime() - horizonDays * 24 * 60 * 60 * 1000);
    const lambda = P.graph.smoothingLambda;
    if (lambda <= 0) return;

    const ratingRows = await tx
      .select({
        playerId: playerRatings.playerId,
        ladderId: playerRatings.ladderId,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
      })
      .from(playerRatings);

    const ratingMap = new Map<string, { mu: number; sigma: number }>();
    ratingRows.forEach((row: any) => {
      ratingMap.set(`${row.ladderId}|${row.playerId}`, { mu: row.mu, sigma: row.sigma });
    });

    if (!ratingMap.size) return;

    const matchRows = await tx
      .select({
        matchId: matches.matchId,
        ladderId: matches.ladderId,
        startTime: matches.startTime,
        side: matchSides.side,
        playerId: matchSidePlayers.playerId,
      })
      .from(matches)
      .innerJoin(matchSides, eq(matchSides.matchId, matches.matchId))
      .innerJoin(matchSidePlayers, eq(matchSidePlayers.matchSideId, matchSides.id))
      .where(gte(matches.startTime, cutoff));

    const adjacency = new Map<string, Set<string>>();
    const addEdge = (from: string, to: string) => {
      if (from === to) return;
      if (!ratingMap.has(from) || !ratingMap.has(to)) return;
      let set = adjacency.get(from);
      if (!set) {
        set = new Set<string>();
        adjacency.set(from, set);
      }
      set.add(to);
    };

    type MatchGroup = { ladderId: string; A: string[]; B: string[] };
    const matchGroups = new Map<string, MatchGroup>();
    for (const row of matchRows) {
      const group = matchGroups.get(row.matchId) ?? { ladderId: row.ladderId, A: [] as string[], B: [] as string[] };
      if (row.side === 'A') group.A.push(`${row.ladderId}|${row.playerId}`);
      else group.B.push(`${row.ladderId}|${row.playerId}`);
      matchGroups.set(row.matchId, group);
    }

    for (const group of matchGroups.values()) {
      if (!group.A.length && !group.B.length) continue;
      for (const node of group.A) {
        for (const teammate of group.A) addEdge(node, teammate);
        for (const opponent of group.B) addEdge(node, opponent);
      }
      for (const node of group.B) {
        for (const teammate of group.B) addEdge(node, teammate);
        for (const opponent of group.A) addEdge(node, opponent);
      }
    }

    const updates: Array<{ playerId: string; ladderId: string; mu: number }> = [];
    for (const [node, neighbors] of adjacency.entries()) {
      const state = ratingMap.get(node);
      if (!state || state.sigma > P.sigmaProvisional) continue;
      const neighborMus: number[] = [];
      neighbors.forEach((neighbor) => {
        const neighborState = ratingMap.get(neighbor);
        if (neighborState) neighborMus.push(neighborState.mu);
      });
      if (!neighborMus.length) continue;
      const neighborMean = neighborMus.reduce((acc, val) => acc + val, 0) / neighborMus.length;
      const delta = lambda * (state.mu - neighborMean);
      const newMu = state.mu - delta;
      ratingMap.set(node, { ...state, mu: newMu });
      const [ladderId, playerId] = node.split('|');
      updates.push({ playerId, ladderId, mu: newMu });
    }

    for (const update of updates) {
      await tx
        .update(playerRatings)
        .set({ mu: update.mu, updatedAt: asOf })
        .where(
          and(
            eq(playerRatings.playerId, update.playerId),
            eq(playerRatings.ladderId, update.ladderId)
          )
        );
    }
  }

  private async applyDriftControl(tx: any, asOf: Date) {
    const rows = await tx
      .select({
        playerId: playerRatings.playerId,
        ladderId: playerRatings.ladderId,
        mu: playerRatings.mu,
      })
      .from(playerRatings);

    if (!rows.length) return;

    const mean = rows.reduce((acc: number, row: any) => acc + row.mu, 0) / rows.length;
    const variance = rows.reduce((acc: number, row: any) => acc + (row.mu - mean) ** 2, 0) / rows.length;
    const std = Math.sqrt(variance);
    const targetMean = P.baseMu;
    const targetStd = P.drift.targetStd;

    for (const row of rows) {
      let newMu = row.mu;
      if (std > 1e-6) {
        newMu = targetMean + (row.mu - mean) * (targetStd / std);
      } else {
        newMu = targetMean;
      }
      let delta = newMu - row.mu;
      delta = clampValue(delta, -P.drift.maxDailyDelta, P.drift.maxDailyDelta);
      const mu = row.mu + delta;
      await tx
        .update(playerRatings)
        .set({ mu, updatedAt: asOf })
        .where(
          and(
            eq(playerRatings.playerId, row.playerId),
            eq(playerRatings.ladderId, row.ladderId)
          )
        );
    }
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

  async ensurePairSynergies(params: EnsurePairSynergiesParams): Promise<EnsurePairSynergiesResult> {
    if (!params.pairs.length) return new Map();

    const nowTs = now();
    await this.db
      .insert(pairSynergies)
      .values(
        params.pairs.map((pair) => ({
          ladderId: params.ladderId,
          pairKey: pair.pairId,
          players: pair.players,
          gamma: 0,
          matches: 0,
          createdAt: nowTs,
          updatedAt: nowTs,
        }))
      )
      .onConflictDoNothing({ target: [pairSynergies.ladderId, pairSynergies.pairKey] });

    const rows = await this.db
      .select({
        pairKey: pairSynergies.pairKey,
        gamma: pairSynergies.gamma,
        matches: pairSynergies.matches,
      })
      .from(pairSynergies)
      .where(
        and(
          eq(pairSynergies.ladderId, params.ladderId),
          inArray(pairSynergies.pairKey, params.pairs.map((pair) => pair.pairId))
        )
      );

    const descriptor = new Map(params.pairs.map((pair) => [pair.pairId, pair.players]));
    const map = new Map<string, PairState>();
    for (const row of rows) {
      map.set(row.pairKey, {
        pairId: row.pairKey,
        players: descriptor.get(row.pairKey) ?? [],
        gamma: row.gamma ?? 0,
        matches: row.matches ?? 0,
      });
    }

    return map;
  }

  async recordMatch(params: RecordMatchParams): Promise<RecordMatchResult> {
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

    const ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }> = [];

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

        const [historyRow] = await tx
          .insert(playerRatingHistory)
          .values({
            playerId: entry.playerId,
            ladderId: params.ladderId,
            matchId,
            muBefore: entry.muBefore,
            muAfter: entry.muAfter,
            sigmaBefore: entry.sigmaBefore,
            sigmaAfter: entry.sigmaAfter,
            delta: entry.delta,
            winProbPre: entry.winProbPre,
            movWeight,
            createdAt: now(),
          })
          .returning({
            id: playerRatingHistory.id,
            createdAt: playerRatingHistory.createdAt,
          });

        if (historyRow) {
          ratingEvents.push({
            playerId: entry.playerId,
            ratingEventId: String(historyRow.id),
            appliedAt: historyRow.createdAt.toISOString(),
          });
        }
      }

      for (const update of params.pairUpdates) {
        await tx
          .update(pairSynergies)
          .set({
            gamma: update.gammaAfter,
            matches: update.matchesAfter,
            players: update.players,
            updatedAt: now(),
          })
          .where(
            and(
              eq(pairSynergies.ladderId, params.ladderId),
              eq(pairSynergies.pairKey, update.pairId)
            )
          );

        await tx.insert(pairSynergyHistory).values({
          ladderId: params.ladderId,
          pairKey: update.pairId,
          matchId,
          gammaBefore: update.gammaBefore,
          gammaAfter: update.gammaAfter,
          delta: update.delta,
          createdAt: now(),
        });
      }
    });

    return { matchId, ratingEvents };
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

  async listRatingEvents(query: RatingEventListQuery): Promise<RatingEventListResult> {
    const ladderId = buildLadderId(query.ladderKey);
    const limit = clampLimit(query.limit);

    await this.assertOrganizationExists(query.ladderKey.organizationId);
    await this.assertPlayerInOrganization(query.playerId, query.ladderKey.organizationId);

    const filters: any[] = [
      eq(playerRatingHistory.playerId, query.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
      eq(matches.organizationId, query.ladderKey.organizationId),
    ];

    if (query.matchId) {
      filters.push(eq(playerRatingHistory.matchId, query.matchId));
    }

    if (query.since) {
      const since = new Date(query.since);
      if (!Number.isNaN(since.getTime())) {
        filters.push(gte(playerRatingHistory.createdAt, since));
      }
    }

    if (query.until) {
      const until = new Date(query.until);
      if (!Number.isNaN(until.getTime())) {
        filters.push(lt(playerRatingHistory.createdAt, until));
      }
    }

    if (query.cursor) {
      const parsed = parseRatingEventCursor(query.cursor);
      if (parsed) {
        filters.push(
          or(
            lt(playerRatingHistory.createdAt, parsed.createdAt),
            and(
              eq(playerRatingHistory.createdAt, parsed.createdAt),
              lt(playerRatingHistory.id, parsed.id)
            )
          )
        );
      }
    }

    const condition = combineFilters(filters);

    let historyQuery = this.db
      .select({
        id: playerRatingHistory.id,
        playerId: playerRatingHistory.playerId,
        ladderId: playerRatingHistory.ladderId,
        matchId: playerRatingHistory.matchId,
        createdAt: playerRatingHistory.createdAt,
        muBefore: playerRatingHistory.muBefore,
        muAfter: playerRatingHistory.muAfter,
        delta: playerRatingHistory.delta,
        sigmaBefore: playerRatingHistory.sigmaBefore,
        sigmaAfter: playerRatingHistory.sigmaAfter,
        winProbPre: playerRatingHistory.winProbPre,
        movWeight: playerRatingHistory.movWeight,
        organizationId: matches.organizationId,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
      .orderBy(desc(playerRatingHistory.createdAt), desc(playerRatingHistory.id))
      .limit(limit + 1);

    if (condition) {
      historyQuery = historyQuery.where(condition);
    }

    const rows = (await historyQuery) as RatingEventRow[];
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);

    const items: RatingEventRecord[] = page.map((row) => this.toRatingEventRecord(row));
    const last = page.at(-1);
    const nextCursor = hasMore && last
      ? buildRatingEventCursor(last.createdAt, last.id)
      : undefined;

    return { items, nextCursor };
  }

  async getRatingEvent(
    identifiers: { ladderKey: LadderKey; playerId: string; ratingEventId: string }
  ): Promise<RatingEventRecord | null> {
    const ladderId = buildLadderId(identifiers.ladderKey);
    await this.assertOrganizationExists(identifiers.ladderKey.organizationId);
    await this.assertPlayerInOrganization(identifiers.playerId, identifiers.ladderKey.organizationId);

    const numericId = Number(identifiers.ratingEventId);
    if (!Number.isFinite(numericId)) {
      return null;
    }

    const rows = (await this.db
      .select({
        id: playerRatingHistory.id,
        playerId: playerRatingHistory.playerId,
        ladderId: playerRatingHistory.ladderId,
        matchId: playerRatingHistory.matchId,
        createdAt: playerRatingHistory.createdAt,
        muBefore: playerRatingHistory.muBefore,
        muAfter: playerRatingHistory.muAfter,
        delta: playerRatingHistory.delta,
        sigmaBefore: playerRatingHistory.sigmaBefore,
        sigmaAfter: playerRatingHistory.sigmaAfter,
        winProbPre: playerRatingHistory.winProbPre,
        movWeight: playerRatingHistory.movWeight,
        organizationId: matches.organizationId,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
      .where(
        and(
          eq(playerRatingHistory.id, numericId),
          eq(playerRatingHistory.playerId, identifiers.playerId),
          eq(playerRatingHistory.ladderId, ladderId),
          eq(matches.organizationId, identifiers.ladderKey.organizationId)
        )
      )
      .limit(1)) as RatingEventRow[];

    const row = rows.at(0);
    if (!row) return null;
    return this.toRatingEventRecord(row);
  }

  async getRatingSnapshot(
    params: { playerId: string; ladderKey: LadderKey; asOf?: string }
  ): Promise<RatingSnapshot | null> {
    const ladderId = buildLadderId(params.ladderKey);
    await this.assertOrganizationExists(params.ladderKey.organizationId);
    await this.assertPlayerInOrganization(params.playerId, params.ladderKey.organizationId);

    const filters: any[] = [
      eq(playerRatingHistory.playerId, params.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
      eq(matches.organizationId, params.ladderKey.organizationId),
    ];

    let asOfDate: Date | null = null;
    if (params.asOf) {
      const parsed = new Date(params.asOf);
      if (!Number.isNaN(parsed.getTime())) {
        asOfDate = parsed;
        filters.push(lte(playerRatingHistory.createdAt, parsed));
      }
    }

    const condition = combineFilters(filters);

    let historyQuery = this.db
      .select({
        id: playerRatingHistory.id,
        playerId: playerRatingHistory.playerId,
        ladderId: playerRatingHistory.ladderId,
        matchId: playerRatingHistory.matchId,
        createdAt: playerRatingHistory.createdAt,
        muBefore: playerRatingHistory.muBefore,
        muAfter: playerRatingHistory.muAfter,
        delta: playerRatingHistory.delta,
        sigmaBefore: playerRatingHistory.sigmaBefore,
        sigmaAfter: playerRatingHistory.sigmaAfter,
        winProbPre: playerRatingHistory.winProbPre,
        movWeight: playerRatingHistory.movWeight,
        organizationId: matches.organizationId,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
      .orderBy(desc(playerRatingHistory.createdAt), desc(playerRatingHistory.id))
      .limit(1);

    if (condition) {
      historyQuery = historyQuery.where(condition);
    }

    const [historyRow] = (await historyQuery) as RatingEventRow[];
    const ratingRow = await this.db
      .select({
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        updatedAt: playerRatings.updatedAt,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .where(
        and(
          eq(playerRatings.playerId, params.playerId),
          eq(playerRatings.ladderId, ladderId),
          eq(players.organizationId, params.ladderKey.organizationId)
        )
      )
      .limit(1);

    const latestRating = ratingRow.at(0);
    if (!historyRow && !latestRating) {
      return null;
    }

    const eventRecord = historyRow ? this.toRatingEventRecord(historyRow) : null;

    const effectiveMu = historyRow ? historyRow.muAfter : latestRating?.mu ?? P.baseMu;
    const effectiveSigma = historyRow ? historyRow.sigmaAfter : latestRating?.sigma ?? P.baseSigma;

    const asOf = asOfDate
      ? asOfDate.toISOString()
      : historyRow
        ? historyRow.createdAt.toISOString()
        : latestRating?.updatedAt?.toISOString() ?? new Date().toISOString();

    return {
      organizationId: params.ladderKey.organizationId,
      playerId: params.playerId,
      ladderId,
      asOf,
      mu: effectiveMu,
      sigma: effectiveSigma,
      ratingEvent: eventRecord,
    } satisfies RatingSnapshot;
  }

  async listLeaderboard(params: LeaderboardQuery): Promise<LeaderboardResult> {
    const ladderId = buildLadderId(params.ladderKey);
    await this.assertOrganizationExists(params.ladderKey.organizationId);
    const limit = clampLimit(params.limit);

    const rows = (await this.db
      .select({
        playerId: playerRatings.playerId,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        matchesCount: playerRatings.matchesCount,
        updatedAt: playerRatings.updatedAt,
        displayName: players.displayName,
        shortName: players.shortName,
        givenName: players.givenName,
        familyName: players.familyName,
        countryCode: players.countryCode,
        regionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .where(
        and(
          eq(playerRatings.ladderId, ladderId),
          eq(players.organizationId, params.ladderKey.organizationId)
        )
      )
      .orderBy(desc(playerRatings.mu), playerRatings.playerId)
      .limit(limit)) as Array<{
        playerId: string;
        mu: number;
        sigma: number;
        matchesCount: number;
        updatedAt: Date | null;
        displayName: string;
        shortName: string | null;
        givenName: string | null;
        familyName: string | null;
        countryCode: string | null;
        regionId: string | null;
      }>;

    const playerIds = rows.map((row) => row.playerId);
    const latestEvents = new Map<string, { delta: number; matchId: string | null; createdAt: Date }>();

    if (playerIds.length) {
      const eventRows = (await this.db
        .select({
          playerId: playerRatingHistory.playerId,
          delta: playerRatingHistory.delta,
          matchId: playerRatingHistory.matchId,
          createdAt: playerRatingHistory.createdAt,
          id: playerRatingHistory.id,
        })
        .from(playerRatingHistory)
        .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
        .where(
          and(
            eq(playerRatingHistory.ladderId, ladderId),
            eq(matches.organizationId, params.ladderKey.organizationId),
            inArray(playerRatingHistory.playerId, playerIds)
          )
        )
        .orderBy(desc(playerRatingHistory.createdAt), desc(playerRatingHistory.id))) as Array<{
          playerId: string;
          delta: number;
          matchId: string | null;
          createdAt: Date;
          id: number;
        }>;

      for (const row of eventRows) {
        if (!latestEvents.has(row.playerId)) {
          latestEvents.set(row.playerId, {
            delta: row.delta,
            matchId: row.matchId,
            createdAt: row.createdAt,
          });
        }
        if (latestEvents.size === playerIds.length) {
          break;
        }
      }
    }

    const items = rows.map((row, index) => {
      const latest = latestEvents.get(row.playerId);
      return {
        rank: index + 1,
        playerId: row.playerId,
        displayName: row.displayName,
        shortName: row.shortName ?? undefined,
        givenName: row.givenName ?? undefined,
        familyName: row.familyName ?? undefined,
        countryCode: row.countryCode ?? undefined,
        regionId: row.regionId ?? undefined,
        mu: row.mu,
        sigma: row.sigma,
        matches: row.matchesCount,
        delta: latest?.delta ?? null,
        lastEventAt: latest?.createdAt ? latest.createdAt.toISOString() : null,
        lastMatchId: latest?.matchId ?? null,
      } satisfies LeaderboardEntry;
    });

    return { items } satisfies LeaderboardResult;
  }

  async listLeaderboardMovers(params: LeaderboardMoversQuery): Promise<LeaderboardMoversResult> {
    const ladderId = buildLadderId(params.ladderKey);
    await this.assertOrganizationExists(params.ladderKey.organizationId);
    const limit = clampLimit(params.limit);

    const since = new Date(params.since);
    if (Number.isNaN(since.getTime())) {
      throw new Error('Invalid since timestamp');
    }

    const aggregateRows = (await this.db
      .select({
        playerId: playerRatingHistory.playerId,
        change: sql<number>`sum(${playerRatingHistory.delta})`,
        events: sql<number>`count(*)`,
        lastEventAt: sql<Date>`max(${playerRatingHistory.createdAt})`,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
      .where(
        and(
          eq(playerRatingHistory.ladderId, ladderId),
          eq(matches.organizationId, params.ladderKey.organizationId),
          gte(playerRatingHistory.createdAt, since)
        )
      )
      .groupBy(playerRatingHistory.playerId)
      .orderBy(sql`sum(${playerRatingHistory.delta}) DESC`, sql`max(${playerRatingHistory.createdAt}) DESC`)
      .limit(limit)) as Array<{
        playerId: string;
        change: unknown;
        events: unknown;
        lastEventAt: Date | string | null;
      }>;

    const playerIds = aggregateRows.map((row) => row.playerId);
    if (!playerIds.length) {
      return { items: [] } satisfies LeaderboardMoversResult;
    }

    const detailsRows = (await this.db
      .select({
        playerId: playerRatings.playerId,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        matchesCount: playerRatings.matchesCount,
        displayName: players.displayName,
        shortName: players.shortName,
        givenName: players.givenName,
        familyName: players.familyName,
        countryCode: players.countryCode,
        regionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .where(
        and(
          eq(playerRatings.ladderId, ladderId),
          eq(players.organizationId, params.ladderKey.organizationId),
          inArray(playerRatings.playerId, playerIds)
        )
      )) as Array<{
        playerId: string;
        mu: number;
        sigma: number;
        matchesCount: number;
        displayName: string;
        shortName: string | null;
        givenName: string | null;
        familyName: string | null;
        countryCode: string | null;
        regionId: string | null;
      }>;

    const detailsByPlayer = new Map<string, (typeof detailsRows)[number]>();
    for (const row of detailsRows) {
      detailsByPlayer.set(row.playerId, row);
    }

    const items: LeaderboardMoverEntry[] = [];

    for (const row of aggregateRows) {
      const details = detailsByPlayer.get(row.playerId);
      if (!details) continue;

      const change = Number(row.change);
      if (!Number.isFinite(change) || change === 0) continue;

      const eventsRaw = Number(row.events);
      const eventsCount = Number.isFinite(eventsRaw) ? eventsRaw : 0;

      const rawLast = row.lastEventAt;
      let lastEventAt: string | null = null;
      if (rawLast instanceof Date) {
        lastEventAt = rawLast.toISOString();
      } else if (rawLast) {
        const parsed = new Date(rawLast);
        if (!Number.isNaN(parsed.getTime())) {
          lastEventAt = parsed.toISOString();
        }
      }

      items.push({
        playerId: details.playerId,
        displayName: details.displayName,
        shortName: details.shortName ?? undefined,
        givenName: details.givenName ?? undefined,
        familyName: details.familyName ?? undefined,
        countryCode: details.countryCode ?? undefined,
        regionId: details.regionId ?? undefined,
        mu: details.mu,
        sigma: details.sigma,
        matches: details.matchesCount,
        change,
        events: eventsCount,
        lastEventAt,
      });
    }

    return { items } satisfies LeaderboardMoversResult;
  }

  async runNightlyStabilization(options: NightlyStabilizationOptions = {}): Promise<void> {
    const asOf = options.asOf ?? now();
    const horizonDays = options.horizonDays ?? P.graph.horizonDays;

    await this.db.transaction(async (tx: any) => {
      await this.applyInactivity(tx, asOf);
      await this.applySynergyDecay(tx, asOf);
      await this.applyRegionBias(tx, asOf);
      await this.applyGraphSmoothing(tx, asOf, horizonDays);
      await this.applyDriftControl(tx, asOf);
    });
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
