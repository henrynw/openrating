import { randomUUID } from 'crypto';
import { and, eq, inArray, or, lt, lte, gt, gte, sql, desc } from 'drizzle-orm';
import type { PlayerState, MatchInput, PairState, PairUpdate, Sport, Discipline } from '../engine/types.js';
import { P } from '../engine/params.js';
import { getDb } from '../db/client.js';
import {
  matchGames,
  matchSidePlayers,
  matchSides,
  matches,
  organizations,
  competitions,
  playerRatings,
  playerRatingHistory,
  playerInsights,
  playerInsightJobs,
  players,
  providers,
  ratingLadders,
  regions,
  sports,
  venues,
  pairSynergies,
  pairSynergyHistory,
  events,
  competitionParticipants,
} from '../db/schema.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerUpdateInput,
  PlayerRecord,
  PlayerCompetitiveProfile,
  PlayerAttributes,
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
  MatchTiming,
  MatchSegment,
  MatchStatistics,
  MatchParticipant,
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
  EventCreateInput,
  EventUpdateInput,
  EventRecord,
  EventClassification,
  EventMediaLinks,
  EventListQuery,
  EventListResult,
  CompetitionCreateInput,
  CompetitionUpdateInput,
  CompetitionRecord,
  CompetitionListQuery,
  CompetitionListResult,
  CompetitionParticipantUpsertInput,
  CompetitionParticipantRecord,
  CompetitionParticipantListResult,
  PlayerInsightsSnapshot,
  PlayerInsightsQuery,
  PlayerInsightsEnqueueInput,
  PlayerInsightsJob,
  PlayerInsightsJobClaimOptions,
  PlayerInsightsJobCompletion,
  PlayerInsightsUpsertResult,
  PlayerInsightsBuildOptions,
} from './types.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError, EventLookupError } from './types.js';
import {
  buildLadderId,
  isDefaultRegion,
  toDbRegionId,
  DEFAULT_REGION,
  buildInsightScopeKey,
} from './helpers.js';
import {
  buildPlayerInsightsSnapshot as buildInsightsSnapshot,
  enrichSnapshotWithCache,
  type PlayerInsightSourceEvent,
  type PlayerInsightCurrentRating,
} from '../insights/builder.js';

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

type PlayerInsightEventRow = {
  id: number;
  createdAt: Date;
  sport: string;
  discipline: string;
  muBefore: number;
  muAfter: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  delta: number;
  winProbPre: number | null;
  matchId: string | null;
};

type PlayerInsightRatingRow = {
  sport: string;
  discipline: string;
  mu: number;
  sigma: number;
  matchesCount: number;
  updatedAt: Date;
};

type PlayerLeaderboardRow = {
  playerId: string;
  mu: number;
  sigma: number;
  matchesCount: number;
  displayName: string;
  shortName: string | null;
  givenName: string | null;
  familyName: string | null;
  countryCode: string | null;
  playerRegionId: string | null;
};

type EventRow = {
  eventId: string;
  organizationId: string;
  type: string;
  name: string;
  slug: string;
  description: string | null;
  startDate: Date | null;
  endDate: Date | null;
  sanctioningBody: string | null;
  season: string | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
};

type CompetitionRow = {
  competitionId: string;
  eventId: string;
  organizationId: string;
  name: string;
  slug: string;
  sport: string | null;
  discipline: string | null;
  format: string | null;
  tier: string | null;
  status: string | null;
  drawSize: number | null;
  startDate: Date | null;
  endDate: Date | null;
  classification: unknown | null;
  purse: number | null;
  purseCurrency: string | null;
  mediaLinks: unknown | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
};

type CompetitionParticipantRow = {
  competitionId: string;
  playerId: string;
  seed: number | null;
  status: string | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
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
    competitiveProfile: unknown | null;
    attributes: unknown | null;
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
      competitiveProfile: (row.competitiveProfile as PlayerCompetitiveProfile | null) ?? null,
      attributes: (row.attributes as PlayerAttributes | null) ?? null,
    };
  }

  private toEventRecord(row: EventRow): EventRecord {
    return {
      eventId: row.eventId,
      organizationId: row.organizationId,
      type: row.type as any,
      name: row.name,
      slug: row.slug,
      description: row.description,
      startDate: row.startDate ? row.startDate.toISOString() : null,
      endDate: row.endDate ? row.endDate.toISOString() : null,
      sanctioningBody: row.sanctioningBody,
      season: row.season,
      metadata: (row.metadata as Record<string, unknown> | null) ?? null,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    } satisfies EventRecord;
  }

  private toCompetitionRecord(row: CompetitionRow): CompetitionRecord {
    return {
      competitionId: row.competitionId,
      eventId: row.eventId,
      organizationId: row.organizationId,
      name: row.name,
      slug: row.slug,
      sport: (row.sport ?? null) as CompetitionRecord['sport'],
      discipline: (row.discipline ?? null) as CompetitionRecord['discipline'],
      format: row.format ?? null,
      tier: row.tier ?? null,
      status: row.status ?? null,
      drawSize: row.drawSize ?? null,
      startDate: row.startDate ? row.startDate.toISOString() : null,
      endDate: row.endDate ? row.endDate.toISOString() : null,
      classification: (row.classification as EventClassification | null) ?? null,
      purse: row.purse,
      purseCurrency: row.purseCurrency,
      mediaLinks: (row.mediaLinks as EventMediaLinks | null) ?? null,
      metadata: (row.metadata as Record<string, unknown> | null) ?? null,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    } satisfies CompetitionRecord;
  }

  private toCompetitionParticipantRecord(
    row: CompetitionParticipantRow
  ): CompetitionParticipantRecord {
    return {
      competitionId: row.competitionId,
      playerId: row.playerId,
      seed: row.seed,
      status: row.status,
      metadata: (row.metadata as Record<string, unknown> | null) ?? null,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    } satisfies CompetitionParticipantRecord;
  }

  private toPlayerInsightEvent(row: PlayerInsightEventRow): PlayerInsightSourceEvent {
    return {
      id: String(row.id),
      createdAt: row.createdAt,
      sport: row.sport as Sport,
      discipline: row.discipline as Discipline,
      muBefore: row.muBefore,
      muAfter: row.muAfter,
      sigmaBefore: row.sigmaBefore,
      sigmaAfter: row.sigmaAfter,
      delta: row.delta,
      winProbPre: row.winProbPre,
      matchId: row.matchId,
    } satisfies PlayerInsightSourceEvent;
  }

  private toPlayerInsightRating(row: PlayerInsightRatingRow): PlayerInsightCurrentRating {
    return {
      sport: row.sport as Sport,
      discipline: row.discipline as Discipline,
      mu: row.mu,
      sigma: row.sigma,
      matchesCount: row.matchesCount,
      updatedAt: row.updatedAt,
    } satisfies PlayerInsightCurrentRating;
  }

  private async fetchPlayerInsightEvents(
    query: PlayerInsightsQuery,
    client = this.db
  ): Promise<PlayerInsightSourceEvent[]> {
    const filters: any[] = [eq(playerRatingHistory.playerId, query.playerId)];
    if (query.sport) filters.push(eq(ratingLadders.sport, query.sport));
    if (query.discipline) filters.push(eq(ratingLadders.discipline, query.discipline));

    const rows = await client
      .select({
        id: playerRatingHistory.id,
        createdAt: playerRatingHistory.createdAt,
        sport: ratingLadders.sport,
        discipline: ratingLadders.discipline,
        muBefore: playerRatingHistory.muBefore,
        muAfter: playerRatingHistory.muAfter,
        sigmaBefore: playerRatingHistory.sigmaBefore,
        sigmaAfter: playerRatingHistory.sigmaAfter,
        delta: playerRatingHistory.delta,
        winProbPre: playerRatingHistory.winProbPre,
        matchId: playerRatingHistory.matchId,
      })
      .from(playerRatingHistory)
      .innerJoin(ratingLadders, eq(playerRatingHistory.ladderId, ratingLadders.ladderId))
      .where(filters.length > 1 ? and(...filters) : filters[0])
      .orderBy(playerRatingHistory.createdAt);

    return (rows as PlayerInsightEventRow[]).map((row) => this.toPlayerInsightEvent(row));
  }

  private async fetchPlayerInsightRatings(
    query: PlayerInsightsQuery,
    client = this.db
  ): Promise<PlayerInsightCurrentRating[]> {
    const filters: any[] = [eq(playerRatings.playerId, query.playerId)];
    if (query.sport) filters.push(eq(ratingLadders.sport, query.sport));
    if (query.discipline) filters.push(eq(ratingLadders.discipline, query.discipline));

    const rows = await client
      .select({
        sport: ratingLadders.sport,
        discipline: ratingLadders.discipline,
        mu: playerRatings.mu,
        sigma: playerRatings.sigma,
        matchesCount: playerRatings.matchesCount,
        updatedAt: playerRatings.updatedAt,
      })
      .from(playerRatings)
      .innerJoin(ratingLadders, eq(playerRatings.ladderId, ratingLadders.ladderId))
      .where(filters.length > 1 ? and(...filters) : filters[0])
      .orderBy(playerRatings.updatedAt);

    return (rows as PlayerInsightRatingRow[]).map((row) => this.toPlayerInsightRating(row));
  }

  private async getEventRowById(eventId: string, client = this.db): Promise<EventRow | null> {
    const rows = await client
      .select({
        eventId: events.eventId,
        organizationId: events.organizationId,
        type: events.type,
        name: events.name,
        slug: events.slug,
        description: events.description,
        startDate: events.startDate,
        endDate: events.endDate,
        sanctioningBody: events.sanctioningBody,
        season: events.season,
        metadata: events.metadata,
        createdAt: events.createdAt,
        updatedAt: events.updatedAt,
      })
      .from(events)
      .where(eq(events.eventId, eventId))
      .limit(1);
    return (rows as EventRow[]).at(0) ?? null;
  }

  private async getEventRowBySlug(organizationId: string, slug: string, client = this.db): Promise<EventRow | null> {
    const rows = await client
      .select({
        eventId: events.eventId,
        organizationId: events.organizationId,
        type: events.type,
        name: events.name,
        slug: events.slug,
        description: events.description,
        startDate: events.startDate,
        endDate: events.endDate,
        sanctioningBody: events.sanctioningBody,
        season: events.season,
        metadata: events.metadata,
        createdAt: events.createdAt,
        updatedAt: events.updatedAt,
      })
      .from(events)
      .where(
        and(eq(events.organizationId, organizationId), eq(events.slug, slug))
      )
      .limit(1);
    return (rows as EventRow[]).at(0) ?? null;
  }

  private async getCompetitionRowById(competitionId: string, client = this.db): Promise<CompetitionRow | null> {
    const rows = await client
      .select({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        name: competitions.name,
        slug: competitions.slug,
        sport: competitions.sport,
        discipline: competitions.discipline,
        format: competitions.format,
        tier: competitions.tier,
        status: competitions.status,
        drawSize: competitions.drawSize,
        startDate: competitions.startDate,
        endDate: competitions.endDate,
        classification: competitions.classification,
        purse: competitions.purse,
        purseCurrency: competitions.purseCurrency,
        mediaLinks: competitions.mediaLinks,
        metadata: competitions.metadata,
        createdAt: competitions.createdAt,
        updatedAt: competitions.updatedAt,
      })
      .from(competitions)
      .where(eq(competitions.competitionId, competitionId))
      .limit(1);
    return (rows as CompetitionRow[]).at(0) ?? null;
  }

  private async getCompetitionRowBySlug(eventId: string, slug: string, client = this.db): Promise<CompetitionRow | null> {
    const rows = await client
      .select({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        name: competitions.name,
        slug: competitions.slug,
        sport: competitions.sport,
        discipline: competitions.discipline,
        format: competitions.format,
        tier: competitions.tier,
        status: competitions.status,
        drawSize: competitions.drawSize,
        startDate: competitions.startDate,
        endDate: competitions.endDate,
        classification: competitions.classification,
        purse: competitions.purse,
        purseCurrency: competitions.purseCurrency,
        mediaLinks: competitions.mediaLinks,
        metadata: competitions.metadata,
        createdAt: competitions.createdAt,
        updatedAt: competitions.updatedAt,
      })
      .from(competitions)
      .where(and(eq(competitions.eventId, eventId), eq(competitions.slug, slug)))
      .limit(1);
    return (rows as CompetitionRow[]).at(0) ?? null;
  }

  private async assertEventBelongsToOrg(eventId: string, organizationId: string, client = this.db) {
    if (!eventId) return;
    const event = await this.getEventRowById(eventId, client);
    if (!event) {
      throw new EventLookupError(`Event not found: ${eventId}`);
    }
    if (event.organizationId !== organizationId) {
      throw new EventLookupError(`Event ${eventId} does not belong to organization ${organizationId}`);
    }
  }

  private async ensureCompetitionParticipantsTx(
    client: any,
    competitionId: string,
    playerIds: string[]
  ): Promise<void> {
    if (!playerIds.length) return;
    const uniqueIds = Array.from(new Set(playerIds));
    const rows = uniqueIds.map((playerId) => ({
      competitionId,
      playerId,
      seed: null,
      status: null,
      metadata: null,
      createdAt: now(),
      updatedAt: now(),
    }));

    await client
      .insert(competitionParticipants)
      .values(rows)
      .onConflictDoNothing();
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
        providerId: matches.providerId,
        externalRef: matches.externalRef,
        organizationId: matches.organizationId,
        sport: matches.sport,
        discipline: matches.discipline,
        format: matches.format,
        tier: matches.tier,
        startTime: matches.startTime,
        venueId: matches.venueId,
        regionId: matches.regionId,
        eventId: matches.eventId,
        competitionId: matches.competitionId,
        competitionSlug: competitions.slug,
        timing: matches.timing,
        statistics: matches.statistics,
        segments: matches.segments,
        sideParticipants: matches.sideParticipants,
      })
      .from(matches)
      .leftJoin(competitions, eq(competitions.competitionId, matches.competitionId))
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
        statistics: matchGames.statistics,
        segments: matchGames.segments,
      })
      .from(matchGames)
      .where(eq(matchGames.matchId, matchId))
      .orderBy(matchGames.gameNo)) as Array<{
        gameNo: number;
        scoreA: number;
        scoreB: number;
        statistics: unknown | null;
        segments: unknown | null;
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
      statistics: (row.statistics as MatchStatistics) ?? null,
      segments: (row.segments as MatchSegment[] | null) ?? null,
    }));

    const sideParticipants = (matchRow.sideParticipants as Record<'A' | 'B', MatchParticipant[] | null | undefined> | null) ?? null;

    const sides: MatchSideSummary[] = ['A', 'B'].map((side) => ({
      side: side as 'A' | 'B',
      players: (sideMap.get(side) ?? []).filter((player): player is string => Boolean(player)),
      participants: sideParticipants?.[side as 'A' | 'B'] ?? null,
    }));

    return {
      matchId: matchRow.matchId,
      providerId: matchRow.providerId,
      externalRef: matchRow.externalRef ?? null,
      organizationId: matchRow.organizationId,
      sport: matchRow.sport as MatchInput['sport'],
      discipline: matchRow.discipline as MatchInput['discipline'],
      format: matchRow.format,
      tier: matchRow.tier ?? undefined,
      startTime: matchRow.startTime.toISOString(),
      venueId: matchRow.venueId ?? null,
      regionId: matchRow.regionId ?? null,
      eventId: matchRow.eventId ?? null,
      competitionId: matchRow.competitionId ?? null,
      competitionSlug: (matchRow.competitionSlug as string | null) ?? null,
      timing: (matchRow.timing as MatchTiming | null) ?? null,
      statistics: (matchRow.statistics as MatchStatistics) ?? null,
      segments: (matchRow.segments as MatchSegment[] | null) ?? null,
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

  async createEvent(input: EventCreateInput): Promise<EventRecord> {
    await this.assertOrganizationExists(input.organizationId);
    const slug = slugify(input.slug ?? input.name);
    const existing = await this.getEventRowBySlug(input.organizationId, slug);
    if (existing) {
      throw new EventLookupError(`Slug already in use: ${slug}`);
    }

    const [row] = await this.db
      .insert(events)
      .values({
        eventId: randomUUID(),
        organizationId: input.organizationId,
        type: input.type,
        name: input.name,
        slug,
        description: input.description ?? null,
        startDate: input.startDate ? new Date(input.startDate) : null,
        endDate: input.endDate ? new Date(input.endDate) : null,
        sanctioningBody: input.sanctioningBody ?? null,
        season: input.season ?? null,
        metadata: input.metadata ?? null,
        createdAt: now(),
        updatedAt: now(),
      })
      .returning({
        eventId: events.eventId,
        organizationId: events.organizationId,
        type: events.type,
        name: events.name,
        slug: events.slug,
        description: events.description,
        startDate: events.startDate,
        endDate: events.endDate,
        sanctioningBody: events.sanctioningBody,
        season: events.season,
        metadata: events.metadata,
        createdAt: events.createdAt,
        updatedAt: events.updatedAt,
      });

    return this.toEventRecord(row as EventRow);
  }

  async updateEvent(eventId: string, input: EventUpdateInput): Promise<EventRecord> {
    const existing = await this.getEventRowById(eventId);
    if (!existing) {
      throw new EventLookupError(`Event not found: ${eventId}`);
    }

    let slug = existing.slug;
    if (input.slug !== undefined) {
      const candidate = slugify(input.slug);
      if (candidate !== existing.slug) {
        const dup = await this.getEventRowBySlug(existing.organizationId, candidate);
        if (dup) {
          throw new EventLookupError(`Slug already in use: ${candidate}`);
        }
        slug = candidate;
      }
    }

    const payload: Record<string, unknown> = { updatedAt: now() };
    if (input.name !== undefined) payload.name = input.name;
    if (input.type !== undefined) payload.type = input.type;
    if (input.description !== undefined) payload.description = input.description;
    if (input.startDate !== undefined) payload.startDate = input.startDate ? new Date(input.startDate) : null;
    if (input.endDate !== undefined) payload.endDate = input.endDate ? new Date(input.endDate) : null;
    if (input.sanctioningBody !== undefined) payload.sanctioningBody = input.sanctioningBody ?? null;
    if (input.season !== undefined) payload.season = input.season ?? null;
    if (input.metadata !== undefined) payload.metadata = input.metadata ?? null;
    if (input.slug !== undefined) payload.slug = slug;

    const [row] = await this.db
      .update(events)
      .set(payload)
      .where(eq(events.eventId, eventId))
      .returning({
        eventId: events.eventId,
        organizationId: events.organizationId,
        type: events.type,
        name: events.name,
        slug: events.slug,
        description: events.description,
        startDate: events.startDate,
        endDate: events.endDate,
        sanctioningBody: events.sanctioningBody,
        season: events.season,
        metadata: events.metadata,
        createdAt: events.createdAt,
        updatedAt: events.updatedAt,
      });

    return this.toEventRecord(row as EventRow);
  }

  async listEvents(query: EventListQuery): Promise<EventListResult> {
    await this.assertOrganizationExists(query.organizationId);
    const limit = clampLimit(query.limit);

    const filters: any[] = [eq(events.organizationId, query.organizationId)];

    if (query.types && query.types.length) {
      filters.push(inArray(events.type, query.types));
    }

    if (query.q) {
      filters.push(sql`${events.name} ILIKE ${`%${query.q}%`}`);
    }

    if (query.cursor) {
      filters.push(sql`${events.slug} > ${query.cursor}`);
    }

    const condition = combineFilters(filters);

    let selectQuery = this.db
      .select({
        eventId: events.eventId,
        organizationId: events.organizationId,
        type: events.type,
        name: events.name,
        slug: events.slug,
        description: events.description,
        startDate: events.startDate,
        endDate: events.endDate,
        sanctioningBody: events.sanctioningBody,
        season: events.season,
        metadata: events.metadata,
        createdAt: events.createdAt,
        updatedAt: events.updatedAt,
      })
      .from(events)
      .orderBy(events.slug)
      .limit(limit + 1);

    if (condition) {
      selectQuery = selectQuery.where(condition);
    }

    const rows = (await selectQuery) as EventRow[];
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const nextCursor = hasMore && page.length ? page[page.length - 1].slug : undefined;

    return {
      items: page.map((row) => this.toEventRecord(row)),
      nextCursor,
    };
  }

  async getEventById(eventId: string): Promise<EventRecord | null> {
    const row = await this.getEventRowById(eventId);
    return row ? this.toEventRecord(row) : null;
  }

  async getEventBySlug(organizationId: string, slug: string): Promise<EventRecord | null> {
    const row = await this.getEventRowBySlug(organizationId, slug);
    return row ? this.toEventRecord(row) : null;
  }

  async createCompetition(input: CompetitionCreateInput): Promise<CompetitionRecord> {
    await this.assertOrganizationExists(input.organizationId);
    const event = await this.getEventRowById(input.eventId);
    if (!event) {
      throw new EventLookupError(`Event not found: ${input.eventId}`);
    }
    if (event.organizationId !== input.organizationId) {
      throw new EventLookupError(
        `Event ${input.eventId} does not belong to organization ${input.organizationId}`
      );
    }

    const slug = slugify(input.slug ?? input.name);
    const existing = await this.getCompetitionRowBySlug(input.eventId, slug);
    if (existing) {
      throw new EventLookupError(`Competition slug already in use: ${slug}`);
    }

    const [row] = await this.db
      .insert(competitions)
      .values({
        competitionId: randomUUID(),
        eventId: input.eventId,
        organizationId: input.organizationId,
        name: input.name,
        slug,
        sport: input.sport ?? null,
        discipline: input.discipline ?? null,
        format: input.format ?? null,
        tier: input.tier ?? null,
        status: input.status ?? null,
        drawSize: input.drawSize ?? null,
        startDate: input.startDate ? new Date(input.startDate) : null,
        endDate: input.endDate ? new Date(input.endDate) : null,
        classification: input.classification ?? null,
        purse: input.purse ?? null,
        purseCurrency: input.purseCurrency ?? null,
        mediaLinks: input.mediaLinks ?? null,
        metadata: input.metadata ?? null,
        createdAt: now(),
        updatedAt: now(),
      })
      .returning({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        name: competitions.name,
        slug: competitions.slug,
        sport: competitions.sport,
        discipline: competitions.discipline,
        format: competitions.format,
        tier: competitions.tier,
        status: competitions.status,
        drawSize: competitions.drawSize,
        startDate: competitions.startDate,
        endDate: competitions.endDate,
        classification: competitions.classification,
        purse: competitions.purse,
        purseCurrency: competitions.purseCurrency,
        mediaLinks: competitions.mediaLinks,
        metadata: competitions.metadata,
        createdAt: competitions.createdAt,
        updatedAt: competitions.updatedAt,
      });

    return this.toCompetitionRecord(row as CompetitionRow);
  }

  async updateCompetition(competitionId: string, input: CompetitionUpdateInput): Promise<CompetitionRecord> {
    const existing = await this.getCompetitionRowById(competitionId);
    if (!existing) {
      throw new EventLookupError(`Competition not found: ${competitionId}`);
    }

    let slug = existing.slug;
    if (input.slug !== undefined || input.name !== undefined) {
      const candidate = slugify(input.slug ?? input.name ?? existing.name);
      if (candidate !== existing.slug) {
        const dup = await this.getCompetitionRowBySlug(existing.eventId, candidate);
        if (dup) {
          throw new EventLookupError(`Competition slug already in use: ${candidate}`);
        }
        slug = candidate;
      }
    }

    const payload: Record<string, unknown> = { updatedAt: now() };
    if (input.name !== undefined) payload.name = input.name;
    if (input.sport !== undefined) payload.sport = input.sport ?? null;
    if (input.discipline !== undefined) payload.discipline = input.discipline ?? null;
    if (input.format !== undefined) payload.format = input.format ?? null;
    if (input.tier !== undefined) payload.tier = input.tier ?? null;
    if (input.status !== undefined) payload.status = input.status ?? null;
    if (input.drawSize !== undefined) payload.drawSize = input.drawSize ?? null;
    if (input.startDate !== undefined) {
      payload.startDate = input.startDate ? new Date(input.startDate) : null;
    }
    if (input.endDate !== undefined) {
      payload.endDate = input.endDate ? new Date(input.endDate) : null;
    }
    if (input.classification !== undefined) payload.classification = input.classification ?? null;
    if (input.purse !== undefined) payload.purse = input.purse ?? null;
    if (input.purseCurrency !== undefined) payload.purseCurrency = input.purseCurrency ?? null;
    if (input.mediaLinks !== undefined) payload.mediaLinks = input.mediaLinks ?? null;
    if (input.metadata !== undefined) payload.metadata = input.metadata ?? null;
    if (slug !== existing.slug) payload.slug = slug;

    const [row] = await this.db
      .update(competitions)
      .set(payload)
      .where(eq(competitions.competitionId, competitionId))
      .returning({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        name: competitions.name,
        slug: competitions.slug,
        sport: competitions.sport,
        discipline: competitions.discipline,
        format: competitions.format,
        tier: competitions.tier,
        status: competitions.status,
        drawSize: competitions.drawSize,
        startDate: competitions.startDate,
        endDate: competitions.endDate,
        classification: competitions.classification,
        purse: competitions.purse,
        purseCurrency: competitions.purseCurrency,
        mediaLinks: competitions.mediaLinks,
        metadata: competitions.metadata,
        createdAt: competitions.createdAt,
        updatedAt: competitions.updatedAt,
      });

    return this.toCompetitionRecord(row as CompetitionRow);
  }

  async listCompetitions(query: CompetitionListQuery): Promise<CompetitionListResult> {
    const rows = await this.db
      .select({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        name: competitions.name,
        slug: competitions.slug,
        sport: competitions.sport,
        discipline: competitions.discipline,
        format: competitions.format,
        tier: competitions.tier,
        status: competitions.status,
        drawSize: competitions.drawSize,
        startDate: competitions.startDate,
        endDate: competitions.endDate,
        classification: competitions.classification,
        purse: competitions.purse,
        purseCurrency: competitions.purseCurrency,
        mediaLinks: competitions.mediaLinks,
        metadata: competitions.metadata,
        createdAt: competitions.createdAt,
        updatedAt: competitions.updatedAt,
      })
      .from(competitions)
      .where(eq(competitions.eventId, query.eventId))
      .orderBy(competitions.slug);

    return {
      items: (rows as CompetitionRow[]).map((row) => this.toCompetitionRecord(row)),
    } satisfies CompetitionListResult;
  }

  async getCompetitionById(competitionId: string): Promise<CompetitionRecord | null> {
    const row = await this.getCompetitionRowById(competitionId);
    return row ? this.toCompetitionRecord(row) : null;
  }

  async getCompetitionBySlug(eventId: string, slug: string): Promise<CompetitionRecord | null> {
    const row = await this.getCompetitionRowBySlug(eventId, slug);
    return row ? this.toCompetitionRecord(row) : null;
  }

  async upsertCompetitionParticipant(
    input: CompetitionParticipantUpsertInput
  ): Promise<CompetitionParticipantRecord> {
    const competition = await this.getCompetitionRowById(input.competitionId);
    if (!competition) {
      throw new EventLookupError(`Competition not found: ${input.competitionId}`);
    }

    await this.assertPlayerInOrganization(input.playerId, competition.organizationId);

    const nowTs = now();
    const [row] = await this.db
      .insert(competitionParticipants)
      .values({
        competitionId: input.competitionId,
        playerId: input.playerId,
        seed: input.seed ?? null,
        status: input.status ?? null,
        metadata: input.metadata ?? null,
        createdAt: nowTs,
        updatedAt: nowTs,
      })
      .onConflictDoUpdate({
        target: [competitionParticipants.competitionId, competitionParticipants.playerId],
        set: {
          seed: input.seed ?? null,
          status: input.status ?? null,
          metadata: input.metadata ?? null,
          updatedAt: nowTs,
        },
      })
      .returning({
        competitionId: competitionParticipants.competitionId,
        playerId: competitionParticipants.playerId,
        seed: competitionParticipants.seed,
        status: competitionParticipants.status,
        metadata: competitionParticipants.metadata,
        createdAt: competitionParticipants.createdAt,
        updatedAt: competitionParticipants.updatedAt,
      });

    return this.toCompetitionParticipantRecord(row as CompetitionParticipantRow);
  }

  async listCompetitionParticipants(
    competitionId: string
  ): Promise<CompetitionParticipantListResult> {
    const competition = await this.getCompetitionRowById(competitionId);
    if (!competition) {
      throw new EventLookupError(`Competition not found: ${competitionId}`);
    }

    const rows = (await this.db
      .select({
        competitionId: competitionParticipants.competitionId,
        playerId: competitionParticipants.playerId,
        seed: competitionParticipants.seed,
        status: competitionParticipants.status,
        metadata: competitionParticipants.metadata,
        createdAt: competitionParticipants.createdAt,
        updatedAt: competitionParticipants.updatedAt,
      })
      .from(competitionParticipants)
      .where(eq(competitionParticipants.competitionId, competitionId))
      .orderBy(competitionParticipants.playerId)) as CompetitionParticipantRow[];

    return { items: rows.map((row) => this.toCompetitionParticipantRecord(row)) };
  }

  async ensureCompetitionParticipants(competitionId: string, playerIds: string[]): Promise<void> {
    if (!playerIds.length) return;
    const competition = await this.getCompetitionRowById(competitionId);
    if (!competition) {
      throw new EventLookupError(`Competition not found: ${competitionId}`);
    }

    const uniqueIds = Array.from(new Set(playerIds));
    const validRows = (await this.db
      .select({ playerId: players.playerId })
      .from(players)
      .where(
        and(eq(players.organizationId, competition.organizationId), inArray(players.playerId, uniqueIds))
      )) as Array<{ playerId: string }>;

    const validIds = validRows.map((row) => row.playerId);
    if (!validIds.length) return;

    await this.ensureCompetitionParticipantsTx(this.db, competitionId, validIds);
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
      competitiveProfile: input.competitiveProfile ?? null,
      attributes: input.attributes ?? null,
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
      competitiveProfile: input.competitiveProfile ?? null,
      attributes: input.attributes ?? null,
    } satisfies PlayerRecord;
  }

  async getPlayer(playerId: string, organizationId: string): Promise<PlayerRecord | null> {
    const rows = await this.db
      .select({
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
        competitiveProfile: players.competitiveProfile,
        attributes: players.attributes,
      })
      .from(players)
      .where(and(eq(players.playerId, playerId), eq(players.organizationId, organizationId)))
      .limit(1);

    const row = rows.at(0);
    if (!row) return null;
    return this.toPlayerRecord(row as any);
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
      competitiveProfile: players.competitiveProfile,
      attributes: players.attributes,
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
    if (input.competitiveProfile !== undefined) {
      updates.competitiveProfile = input.competitiveProfile ?? null;
    }
    if (input.attributes !== undefined) {
      updates.attributes = input.attributes ?? null;
    }

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

    await this.ensureSport(key.sport);

    await this.db
      .insert(ratingLadders)
      .values({
        ladderId,
        sport: key.sport,
        discipline: key.discipline,
        createdAt: now(),
        updatedAt: now(),
      })
      .onConflictDoNothing({ target: ratingLadders.ladderId });

    return ladderId;
  }

  async ensurePlayers(
    ids: string[],
    ladderKey: LadderKey,
    options: { organizationId: string }
  ): Promise<EnsurePlayersResult> {
    const ladderId = await this.ensureLadder(ladderKey);
    if (ids.length === 0) return { ladderId, players: new Map() };

    await this.assertOrganizationExists(options.organizationId);

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
      .filter((row) => row.organizationId !== options.organizationId)
      .map((row) => row.playerId);
    if (wrongOrg.length) {
      throw new PlayerLookupError(
        `Players not registered to organization ${options.organizationId}: ${wrongOrg.join(', ')}`,
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
    // Check for existing match with same provider + external_ref for idempotency
    if (params.submissionMeta.externalRef) {
      const existing = await this.db
        .select({ matchId: matches.matchId })
        .from(matches)
        .where(
          and(
            eq(matches.providerId, params.submissionMeta.providerId),
            eq(matches.externalRef, params.submissionMeta.externalRef)
          )
        )
        .limit(1);

      if (existing.length > 0) {
        // Match already exists, return existing match info
        const existingMatchId = existing[0].matchId;
        const ratingEvents = (await this.db
          .select({
            playerId: playerRatingHistory.playerId,
            ratingEventId: sql<string>`CAST(${playerRatingHistory.id} AS TEXT)`,
            appliedAt: playerRatingHistory.createdAt,
          })
          .from(playerRatingHistory)
          .where(eq(playerRatingHistory.matchId, existingMatchId))) as Array<{
          playerId: string;
          ratingEventId: string;
          appliedAt: Date;
        }>;

        return {
          matchId: existingMatchId,
          ratingEvents: ratingEvents.map((e) => ({
            playerId: e.playerId,
            ratingEventId: e.ratingEventId,
            appliedAt: e.appliedAt.toISOString(),
          })),
        };
      }
    }

    const matchId = randomUUID();
    const movWeight = params.match.movWeight ?? null;

    await this.ensureProvider(params.submissionMeta.providerId);
    await this.assertOrganizationExists(params.submissionMeta.organizationId);
    await this.ensureSport(params.match.sport);

    const playerIds = new Set<string>();
    params.match.sides.A.players.forEach((id) => playerIds.add(id));
    params.match.sides.B.players.forEach((id) => playerIds.add(id));

    let eventId = params.eventId ?? null;
    let competitionId = params.competitionId ?? null;

    if (competitionId) {
      const competition = await this.getCompetitionRowById(competitionId);
      if (!competition) {
        throw new EventLookupError(`Competition not found: ${competitionId}`);
      }
      if (competition.organizationId !== params.submissionMeta.organizationId) {
        throw new EventLookupError(
          `Competition ${competitionId} does not belong to organization ${params.submissionMeta.organizationId}`
        );
      }
      if (eventId && competition.eventId !== eventId) {
        throw new EventLookupError('Competition does not belong to provided event');
      }
      if (competition.sport && competition.sport !== params.match.sport) {
        throw new EventLookupError('Competition sport does not match submitted match sport');
      }
      if (competition.discipline && competition.discipline !== params.match.discipline) {
        throw new EventLookupError('Competition discipline does not match submitted match discipline');
      }
      if (competition.format && competition.format !== params.match.format) {
        throw new EventLookupError('Competition format does not match submitted match format');
      }
      eventId = competition.eventId;
    }

    if (eventId) {
      await this.assertEventBelongsToOrg(eventId, params.submissionMeta.organizationId);
    }

    const submissionRegionId = await this.ensureRegion(
      params.submissionMeta.regionId ?? null,
      params.submissionMeta.organizationId
    );
    const venueId = await this.ensureVenue(
      params.submissionMeta.venueId ?? null,
      params.submissionMeta.organizationId,
      submissionRegionId ?? null
    );

    const gameExtras = new Map<number, { segments?: MatchSegment[] | null; statistics?: MatchStatistics }>();
    for (const detail of params.gameDetails ?? []) {
      gameExtras.set(detail.gameNo, {
        segments: detail.segments ?? null,
        statistics: detail.statistics ?? null,
      });
    }

    const ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }> = [];

    await this.db.transaction(async (tx: any) => {
      await tx.insert(matches).values({
        matchId,
        ladderId: params.ladderId,
        providerId: params.submissionMeta.providerId,
        externalRef: params.submissionMeta.externalRef ?? null,
        organizationId: params.submissionMeta.organizationId,
        sport: params.match.sport,
        discipline: params.match.discipline,
        format: params.match.format,
        tier: params.match.tier ?? 'UNSPECIFIED',
        venueId,
        regionId: submissionRegionId ?? null,
        eventId,
        competitionId,
        startTime: new Date(params.submissionMeta.startTime),
        timing: params.timing ?? null,
        statistics: params.statistics ?? null,
        segments: params.segments ?? null,
        sideParticipants: params.sideParticipants ?? null,
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
        const extras = gameExtras.get(game.game_no);
        await tx.insert(matchGames).values({
          matchId,
          gameNo: game.game_no,
          scoreA: game.a,
          scoreB: game.b,
          statistics: extras?.statistics ?? null,
          segments: extras?.segments ?? null,
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

      if (competitionId) {
        await this.ensureCompetitionParticipantsTx(tx, competitionId, Array.from(playerIds));
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
        eventId: matches.eventId,
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
    let ensureCompetitionId: string | null = null;

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

    if (input.eventId !== undefined) {
      if (!input.eventId) {
        updates.eventId = null;
        updates.competitionId = null;
      } else {
        await this.assertEventBelongsToOrg(input.eventId, organizationId);
        updates.eventId = input.eventId;
      }
    }

    if (input.competitionId !== undefined) {
      if (!input.competitionId) {
        updates.competitionId = null;
      } else {
        const competition = await this.getCompetitionRowById(input.competitionId);
        if (!competition || competition.organizationId !== organizationId) {
          throw new EventLookupError(`Competition not found for organization ${organizationId}`);
        }
        updates.competitionId = input.competitionId;
        updates.eventId = competition.eventId;
        ensureCompetitionId = input.competitionId;
      }
    }

    if (input.timing !== undefined) {
      updates.timing = input.timing ?? null;
    }
    if (input.statistics !== undefined) {
      updates.statistics = input.statistics ?? null;
    }
    if (input.segments !== undefined) {
      updates.segments = input.segments ?? null;
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

    if (ensureCompetitionId) {
      const playerRows = (await this.db
        .select({ playerId: matchSidePlayers.playerId })
        .from(matchSidePlayers)
        .innerJoin(matchSides, eq(matchSides.id, matchSidePlayers.matchSideId))
        .where(eq(matchSides.matchId, matchId))) as Array<{ playerId: string }>;
      const playerIds = playerRows.map((row) => row.playerId);
      await this.ensureCompetitionParticipants(ensureCompetitionId, playerIds);
    }

    const summary = await this.getMatchSummaryById(matchId);
    if (!summary) {
      throw new MatchLookupError(`Match not found: ${matchId}`);
    }
    return summary;
  }

  async getMatch(matchId: string, organizationId: string): Promise<MatchSummary | null> {
    const summary = await this.getMatchSummaryById(matchId);
    if (!summary) return null;
    if (summary.organizationId !== organizationId) {
      return null;
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

    const filters: any[] = [
      eq(playerRatingHistory.playerId, query.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
    ];

    if (query.organizationId) {
      await this.assertOrganizationExists(query.organizationId);
      filters.push(eq(matches.organizationId, query.organizationId));
    }

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
    identifiers: { ladderKey: LadderKey; playerId: string; ratingEventId: string; organizationId?: string | null }
  ): Promise<RatingEventRecord | null> {
    const ladderId = buildLadderId(identifiers.ladderKey);
    const numericId = Number(identifiers.ratingEventId);
    if (!Number.isFinite(numericId)) {
      return null;
    }

    const filters: any[] = [
      eq(playerRatingHistory.id, numericId),
      eq(playerRatingHistory.playerId, identifiers.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
    ];

    if (identifiers.organizationId) {
      await this.assertOrganizationExists(identifiers.organizationId);
      filters.push(eq(matches.organizationId, identifiers.organizationId));
    }

    const condition = combineFilters(filters);

    let eventQuery = this.db
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
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId));

    if (condition) {
      eventQuery = eventQuery.where(condition);
    }

    const rows = (await eventQuery.limit(1)) as RatingEventRow[];

    const row = rows.at(0);
    if (!row) return null;
    return this.toRatingEventRecord(row);
  }

  async getRatingSnapshot(
    params: { playerId: string; ladderKey: LadderKey; asOf?: string; organizationId?: string | null }
  ): Promise<RatingSnapshot | null> {
    const ladderId = buildLadderId(params.ladderKey);
    if (params.organizationId) {
      await this.assertOrganizationExists(params.organizationId);
    }

    const filters: any[] = [
      eq(playerRatingHistory.playerId, params.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
    ];

    if (params.organizationId) {
      filters.push(eq(matches.organizationId, params.organizationId));
    }

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
    let ratingQuery = this.db
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
          eq(playerRatings.ladderId, ladderId)
        )
      );

    if (params.organizationId) {
      ratingQuery = ratingQuery.where(eq(players.organizationId, params.organizationId));
    }

    const ratingRow = await ratingQuery.limit(1);

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
      sport: params.ladderKey.sport,
      discipline: params.ladderKey.discipline,
      scope: params.ladderKey.tier ?? null,
      organizationId: params.organizationId ?? null,
      playerId: params.playerId,
      ladderId,
      asOf,
      mu: effectiveMu,
      sigma: effectiveSigma,
      ratingEvent: eventRecord,
    } satisfies RatingSnapshot;
  }

  async listLeaderboard(params: LeaderboardQuery): Promise<LeaderboardResult> {
    const ladderKey: LadderKey = {
      sport: params.sport,
      discipline: params.discipline,
    };
    const ladderId = await this.ensureLadder(ladderKey);
    const limit = clampLimit(params.limit);

    const playerFilters: any[] = [eq(playerRatings.ladderId, ladderId)];
    if (params.organizationId) {
      await this.assertOrganizationExists(params.organizationId);
      playerFilters.push(eq(players.organizationId, params.organizationId));
    }

    const playerCondition = combineFilters(playerFilters);

    let playerQuery = this.db
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
        playerRegionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .orderBy(desc(playerRatings.mu), playerRatings.playerId)
      .limit(limit);

    if (playerCondition) {
      playerQuery = playerQuery.where(playerCondition);
    }

    const playerRows = (await playerQuery) as PlayerLeaderboardRow[];
    if (!playerRows.length) {
      return { items: [] } satisfies LeaderboardResult;
    }

    const playerIds = playerRows.map((row) => row.playerId);

    const historyFilters: any[] = [
      eq(playerRatingHistory.ladderId, ladderId),
      inArray(playerRatingHistory.playerId, playerIds),
    ];
    if (params.organizationId) {
      historyFilters.push(eq(matches.organizationId, params.organizationId));
    }

    const historyCondition = combineFilters(historyFilters);

    let historyQuery = this.db
      .select({
        playerId: playerRatingHistory.playerId,
        delta: playerRatingHistory.delta,
        matchId: playerRatingHistory.matchId,
        createdAt: playerRatingHistory.createdAt,
        id: playerRatingHistory.id,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId))
      .orderBy(desc(playerRatingHistory.createdAt), desc(playerRatingHistory.id));

    if (historyCondition) {
      historyQuery = historyQuery.where(historyCondition);
    }

    const historyRows = (await historyQuery) as Array<{
      playerId: string;
      delta: number;
      matchId: string | null;
      createdAt: Date;
      id: number;
    }>;

    const latestByPlayer = new Map<string, { delta: number; matchId: string | null; createdAt: Date }>();
    for (const row of historyRows) {
      if (latestByPlayer.has(row.playerId)) continue;
      latestByPlayer.set(row.playerId, {
        delta: row.delta,
        matchId: row.matchId,
        createdAt: row.createdAt,
      });
      if (latestByPlayer.size === playerIds.length) {
        break;
      }
    }

    const items: LeaderboardEntry[] = playerRows.map((row, index) => {
      const latest = latestByPlayer.get(row.playerId);
      return {
        rank: index + 1,
        playerId: row.playerId,
        displayName: row.displayName,
        shortName: row.shortName ?? undefined,
        givenName: row.givenName ?? undefined,
        familyName: row.familyName ?? undefined,
        countryCode: row.countryCode ?? undefined,
        regionId: row.playerRegionId ?? undefined,
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
    const ladderKey: LadderKey = {
      sport: params.sport,
      discipline: params.discipline,
    };
    const ladderId = await this.ensureLadder(ladderKey);
    const limit = clampLimit(params.limit);

    const since = new Date(params.since);
    if (Number.isNaN(since.getTime())) {
      throw new Error('Invalid since timestamp');
    }

    const historyFilters: any[] = [
      eq(playerRatingHistory.ladderId, ladderId),
      gte(playerRatingHistory.createdAt, since),
    ];
    if (params.organizationId) {
      await this.assertOrganizationExists(params.organizationId);
      historyFilters.push(eq(matches.organizationId, params.organizationId));
    }

    const historyCondition = combineFilters(historyFilters);

    let aggregateQuery = this.db
      .select({
        playerId: playerRatingHistory.playerId,
        change: sql<number>`sum(${playerRatingHistory.delta})`,
        events: sql<number>`count(*)`,
        lastEventAt: sql<Date>`max(${playerRatingHistory.createdAt})`,
      })
      .from(playerRatingHistory)
      .innerJoin(matches, eq(matches.matchId, playerRatingHistory.matchId));

    if (historyCondition) {
      aggregateQuery = aggregateQuery.where(historyCondition);
    }

    const aggregateRows = (await aggregateQuery
      .groupBy(playerRatingHistory.playerId)
      .orderBy(sql`sum(${playerRatingHistory.delta}) DESC`, sql`max(${playerRatingHistory.createdAt}) DESC`)
      .limit(limit * 2)) as Array<{
        playerId: string;
        change: unknown;
        events: unknown;
        lastEventAt: Date | string | null;
      }>;

    if (!aggregateRows.length) {
      return { items: [] } satisfies LeaderboardMoversResult;
    }

    const candidateIds = aggregateRows.map((row) => row.playerId);

    const ratingFilters: any[] = [
      eq(playerRatings.ladderId, ladderId),
      inArray(playerRatings.playerId, candidateIds),
    ];
    if (params.organizationId) {
      ratingFilters.push(eq(players.organizationId, params.organizationId));
    }

    const ratingCondition = combineFilters(ratingFilters);

    let ratingQuery = this.db
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
        playerRegionId: players.regionId,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId));

    if (ratingCondition) {
      ratingQuery = ratingQuery.where(ratingCondition);
    }

    const ratingRows = (await ratingQuery) as PlayerLeaderboardRow[];
    const ratingByPlayer = new Map<string, PlayerLeaderboardRow>(
      ratingRows.map((row) => [row.playerId, row])
    );

    const items: LeaderboardMoverEntry[] = [];

    for (const row of aggregateRows) {
      const rating = ratingByPlayer.get(row.playerId);
      if (!rating) continue;

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
        playerId: rating.playerId,
        displayName: rating.displayName,
        shortName: rating.shortName ?? undefined,
        givenName: rating.givenName ?? undefined,
        familyName: rating.familyName ?? undefined,
        countryCode: rating.countryCode ?? undefined,
        regionId: rating.playerRegionId ?? undefined,
        mu: rating.mu,
        sigma: rating.sigma,
        matches: rating.matchesCount,
        change,
        events: eventsCount,
        lastEventAt,
      });

      if (items.length === limit) {
        break;
      }
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

  async getPlayerInsights(query: PlayerInsightsQuery): Promise<PlayerInsightsSnapshot | null> {
    await this.assertPlayerInOrganization(query.playerId, query.organizationId);
    const scopeKey = buildInsightScopeKey(query.sport ?? null, query.discipline ?? null);

    const rows = await this.db
      .select({ snapshot: playerInsights.snapshot })
      .from(playerInsights)
      .where(
        and(
          eq(playerInsights.playerId, query.playerId),
          eq(playerInsights.organizationId, query.organizationId),
          eq(playerInsights.scopeKey, scopeKey)
        )
      )
      .limit(1);

    const row = rows.at(0);
    if (!row) return null;
    return JSON.parse(JSON.stringify(row.snapshot)) as PlayerInsightsSnapshot;
  }

  async buildPlayerInsightsSnapshot(
    query: PlayerInsightsQuery,
    options?: PlayerInsightsBuildOptions
  ): Promise<PlayerInsightsSnapshot> {
    await this.assertPlayerInOrganization(query.playerId, query.organizationId);
    const [events, ratings] = await Promise.all([
      this.fetchPlayerInsightEvents(query),
      this.fetchPlayerInsightRatings(query),
    ]);

    return buildInsightsSnapshot({
      playerId: query.playerId,
      sport: query.sport ?? null,
      discipline: query.discipline ?? null,
      events,
      ratings,
      options,
    });
  }

  async upsertPlayerInsightsSnapshot(
    query: PlayerInsightsQuery,
    snapshot: PlayerInsightsSnapshot
  ): Promise<PlayerInsightsUpsertResult> {
    await this.assertPlayerInOrganization(query.playerId, query.organizationId);
    const scopeKey = buildInsightScopeKey(query.sport ?? null, query.discipline ?? null);
    const { snapshot: enriched, etag, digest } = enrichSnapshotWithCache(snapshot);
    const generatedAt = new Date(enriched.meta.generatedAt);

    await this.db
      .insert(playerInsights)
      .values({
        playerId: query.playerId,
        organizationId: query.organizationId,
        sport: query.sport ?? null,
        discipline: query.discipline ?? null,
        scopeKey,
        schemaVersion: enriched.meta.schemaVersion,
        snapshot: enriched as unknown as PlayerInsightsSnapshot,
        generatedAt,
        etag,
        digest,
      })
      .onConflictDoUpdate({
        target: [playerInsights.playerId, playerInsights.organizationId, playerInsights.scopeKey],
        set: {
          schemaVersion: enriched.meta.schemaVersion,
          snapshot: enriched as unknown as PlayerInsightsSnapshot,
          generatedAt,
          etag,
          digest,
          updatedAt: sql`now()`,
        },
      });

    return { snapshot: JSON.parse(JSON.stringify(enriched)) as PlayerInsightsSnapshot, etag, digest };
  }

  async enqueuePlayerInsightsRefresh(
    input: PlayerInsightsEnqueueInput
  ): Promise<{ jobId: string; enqueued: boolean }> {
    await this.assertPlayerInOrganization(input.playerId, input.organizationId);
    const scopeKey = buildInsightScopeKey(input.sport ?? null, input.discipline ?? null);
    const runAt = input.runAt ? new Date(input.runAt) : now();

    const existingRows = await this.db
      .select({
        jobId: playerInsightJobs.jobId,
        status: playerInsightJobs.status,
      })
      .from(playerInsightJobs)
      .where(
        and(
          eq(playerInsightJobs.playerId, input.playerId),
          eq(playerInsightJobs.organizationId, input.organizationId),
          eq(playerInsightJobs.scopeKey, scopeKey),
          sql`${playerInsightJobs.status} IN ('PENDING', 'IN_PROGRESS')`
        )
      )
      .limit(1);

    const existing = existingRows.at(0);

    if (existing) {
      if (input.dedupe === false) {
        const updateData: Record<string, unknown> = {
          status: 'PENDING',
          runAt,
          lockedAt: null,
          lockedBy: null,
          updatedAt: sql`now()`,
          lastError: null,
        };
        if (input.payload !== undefined) {
          updateData.payload = input.payload;
        }
        await this.db
          .update(playerInsightJobs)
          .set(updateData)
          .where(eq(playerInsightJobs.jobId, existing.jobId));
        return { jobId: existing.jobId, enqueued: true };
      }

      return { jobId: existing.jobId, enqueued: false };
    }

    const jobId = randomUUID();
    await this.db.insert(playerInsightJobs).values({
      jobId,
      playerId: input.playerId,
      organizationId: input.organizationId,
      sport: input.sport ?? null,
      discipline: input.discipline ?? null,
      scopeKey,
      status: 'PENDING',
      runAt,
      attempts: 0,
      lockedAt: null,
      lockedBy: null,
      payload: input.payload ?? null,
      lastError: null,
    });

    return { jobId, enqueued: true };
  }

  async claimPlayerInsightsJob(
    options: PlayerInsightsJobClaimOptions
  ): Promise<PlayerInsightsJob | null> {
    const result = await this.db.execute(sql`
      WITH claimed AS (
        SELECT job_id
        FROM player_insight_jobs
        WHERE status = 'PENDING'
          AND run_at <= now()
        ORDER BY run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE player_insight_jobs
      SET status = 'IN_PROGRESS',
          locked_at = now(),
          locked_by = ${options.workerId},
          attempts = attempts + 1,
          updated_at = now()
      WHERE job_id IN (SELECT job_id FROM claimed)
      RETURNING job_id AS "jobId",
                player_id AS "playerId",
                organization_id AS "organizationId",
                sport,
                discipline,
                run_at AS "runAt",
                status,
                attempts,
                locked_at AS "lockedAt",
                locked_by AS "lockedBy",
                payload,
                last_error AS "lastError";
    `);

    const rows = result.rows as Array<{
      jobId: string;
      playerId: string;
      organizationId: string;
      sport: string | null;
      discipline: string | null;
      runAt: Date;
      status: string;
      attempts: number;
      lockedAt: Date | null;
      lockedBy: string | null;
      payload: unknown;
      lastError: string | null;
    }>;
    const row = rows?.[0];
    if (!row) return null;
    return {
      jobId: row.jobId,
      playerId: row.playerId,
      organizationId: row.organizationId,
      sport: (row.sport ?? null) as Sport | null,
      discipline: (row.discipline ?? null) as Discipline | null,
      runAt: row.runAt.toISOString(),
      status: row.status as PlayerInsightsJob['status'],
      attempts: row.attempts,
      lockedAt: row.lockedAt ? row.lockedAt.toISOString() : null,
      lockedBy: row.lockedBy ?? null,
      payload: (row.payload ?? null) as Record<string, unknown> | null,
      lastError: row.lastError ?? null,
    } satisfies PlayerInsightsJob;
  }

  async completePlayerInsightsJob(result: PlayerInsightsJobCompletion): Promise<void> {
    const condition = and(
      eq(playerInsightJobs.jobId, result.jobId),
      or(
        sql`${playerInsightJobs.lockedBy} IS NULL`,
        eq(playerInsightJobs.lockedBy, result.workerId)
      )
    );

    if (result.success) {
      await this.db
        .update(playerInsightJobs)
        .set({
          status: 'COMPLETED',
          lockedAt: null,
          lockedBy: null,
          updatedAt: sql`now()`,
          lastError: null,
        })
        .where(condition);
      return;
    }

    const runAt = result.rescheduleAt === undefined
      ? new Date(Date.now() + 30_000)
      : result.rescheduleAt === null
        ? null
        : new Date(result.rescheduleAt);

    const updateData: Record<string, unknown> = {
      status: runAt ? 'PENDING' : 'FAILED',
      lockedAt: null,
      lockedBy: null,
      updatedAt: sql`now()`,
      lastError: result.error ?? null,
    };
    if (runAt) {
      updateData.runAt = runAt;
    }

    await this.db.update(playerInsightJobs).set(updateData).where(condition);
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
        competitiveProfile: players.competitiveProfile,
        attributes: players.attributes,
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
        competitiveProfile: unknown | null;
        attributes: unknown | null;
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
      competitiveProfile: (row.competitiveProfile as PlayerCompetitiveProfile | null) ?? null,
      attributes: (row.attributes as PlayerAttributes | null) ?? null,
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

    if (query.eventId) {
      filters.push(eq(matches.eventId, query.eventId));
    }

    if (query.competitionId) {
      filters.push(eq(matches.competitionId, query.competitionId));
    }

    const condition = combineFilters(filters);

    let matchQuery = this.db
      .select({
        matchId: matches.matchId,
        providerId: matches.providerId,
        externalRef: matches.externalRef,
        organizationId: matches.organizationId,
        sport: matches.sport,
        discipline: matches.discipline,
        format: matches.format,
        tier: matches.tier,
        startTime: matches.startTime,
        venueId: matches.venueId,
        regionId: matches.regionId,
        eventId: matches.eventId,
        competitionId: matches.competitionId,
        competitionSlug: competitions.slug,
        timing: matches.timing,
        statistics: matches.statistics,
        segments: matches.segments,
        sideParticipants: matches.sideParticipants,
      })
      .from(matches)
      .leftJoin(competitions, eq(competitions.competitionId, matches.competitionId))
      .orderBy(desc(matches.startTime), desc(matches.matchId))
      .limit(limit + 1);

    if (condition) {
      matchQuery = matchQuery.where(condition);
    }

    const rows = (await matchQuery) as Array<{
      matchId: string;
      providerId: string;
      externalRef: string | null;
      organizationId: string;
      sport: string;
      discipline: string;
      format: string;
      tier: string | null;
      startTime: Date;
      venueId: string | null;
      regionId: string | null;
      eventId: string | null;
      competitionId: string | null;
      competitionSlug: string | null;
      timing: unknown | null;
      statistics: unknown | null;
      segments: unknown | null;
      sideParticipants: unknown | null;
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
        statistics: matchGames.statistics,
        segments: matchGames.segments,
      })
      .from(matchGames)
      .where(inArray(matchGames.matchId, matchIds))
      .orderBy(matchGames.matchId, matchGames.gameNo)) as Array<{
        matchId: string;
        gameNo: number;
        scoreA: number;
        scoreB: number;
        statistics: unknown | null;
        segments: unknown | null;
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
      list.push({
        gameNo: row.gameNo,
        a: row.scoreA,
        b: row.scoreB,
        statistics: (row.statistics as MatchStatistics) ?? null,
        segments: (row.segments as MatchSegment[] | null) ?? null,
      });
      gamesMap.set(row.matchId, list);
    }

    const items: MatchSummary[] = page.map((row) => {
      const sideMap = sidesMap.get(row.matchId) ?? new Map<string, string[]>();
      const participants = (row.sideParticipants as Record<'A' | 'B', MatchParticipant[] | null | undefined> | null) ?? null;
      const sides: MatchSideSummary[] = ['A', 'B'].map((side) => ({
        side: side as 'A' | 'B',
        players: sideMap.get(side) ?? [],
        participants: participants?.[side as 'A' | 'B'] ?? null,
      }));
      const gameList = gamesMap.get(row.matchId) ?? [];
      return {
        matchId: row.matchId,
        providerId: row.providerId,
        externalRef: row.externalRef ?? null,
        organizationId: row.organizationId,
        sport: row.sport as MatchInput['sport'],
        discipline: row.discipline as MatchInput['discipline'],
        format: row.format,
        tier: row.tier ?? undefined,
        startTime: row.startTime.toISOString(),
        venueId: row.venueId ?? null,
        regionId: row.regionId ?? null,
        eventId: row.eventId ?? null,
        competitionId: row.competitionId ?? null,
        competitionSlug: row.competitionSlug ?? null,
        timing: (row.timing as MatchTiming | null) ?? null,
        statistics: (row.statistics as MatchStatistics) ?? null,
        segments: (row.segments as MatchSegment[] | null) ?? null,
        sides,
        games: gameList,
      };
    });

    return { items, nextCursor };
  }
}
