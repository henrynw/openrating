import { randomUUID } from 'crypto';
import { and, eq, inArray, or, lt, lte, gt, gte, sql, desc } from 'drizzle-orm';
import type {
  PlayerState,
  MatchInput,
  PairState,
  PairUpdate,
  PlayerSex,
  SexOffsetSignal,
  Sport,
  Discipline,
  WinnerSide,
} from '../engine/types.js';
import { updateMatch as runMatchUpdate } from '../engine/rating.js';
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
  playerInsightAi,
  playerInsightAiJobs,
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
  ratingReplayQueue,
  ladderSexOffsets,
} from '../db/schema.js';
import {
  buildMatchCursor,
  parseMatchCursor,
  buildRatingEventCursor,
  parseNumericRatingEventCursor,
} from './util/cursors.js';
import { clampLimit } from './util/pagination.js';
import { encodeLeaderboardCursor, decodeLeaderboardCursor } from './util/leaderboard-cursor.js';
import { combineFilters, type SqlFilter } from './postgres/sql-helpers.js';
import { createPostgresContext, type PostgresStoreContext } from './postgres/context.js';
import { createOrganizationsModule, type OrganizationsModule } from './postgres/modules/organizations.js';
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
  PlayerSportTotalsQuery,
  PlayerSportTotalsResult,
  MatchListQuery,
  MatchListResult,
  MatchRatingStatus,
  MatchRatingSkipReason,
  MatchSportTotalsQuery,
  MatchSportTotalsResult,
  MatchSummary,
  MatchGameSummary,
  MatchTiming,
  MatchSegment,
  MatchStatistics,
  MatchParticipant,
  MatchSideSummary,
  MatchPlayerSummaryEmbed,
  MatchEventSummaryEmbed,
  MatchStage,
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
  RatingReplayOptions,
  RatingReplayReport,
  RatingReplayReportItem,
  RatingReplayQueueOptions,
  LeaderboardQuery,
  LeaderboardResult,
  LeaderboardEntry,
  LeaderboardMoversQuery,
  LeaderboardMoversResult,
  LeaderboardMoverEntry,
  PlayerRatingsSummaryQuery,
  PlayerRatingsSummaryResult,
  PlayerRatingsSummaryItem,
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
  CompetitionsByEventMap,
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
  PlayerInsightAiEnsureInput,
  PlayerInsightAiEnsureResult,
  PlayerInsightAiEnqueueInput,
  PlayerInsightAiJob,
  PlayerInsightAiJobClaimOptions,
  PlayerInsightAiJobCompletion,
  PlayerInsightAiData,
  PlayerInsightAiResultInput,
} from './types.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError, EventLookupError } from './types.js';
import {
  buildLadderId,
  isDefaultRegion,
  toDbRegionId,
  DEFAULT_REGION,
  buildInsightScopeKey,
  buildPairKey,
  sortPairPlayers,
} from './helpers.js';
import { slugify } from './util/slug.js';
import { reconcileBirthDetails } from './birth.js';
import {
  resolveAgeFilter,
  type LadderAgePolicy,
  type ResolvedAgeFilter,
  type AgeBandDefinition,
} from './age.js';
import {
  buildPlayerInsightsSnapshot as buildInsightsSnapshot,
  enrichSnapshotWithCache,
  type PlayerInsightSourceEvent,
  type PlayerInsightCurrentRating,
} from '../insights/builder.js';

const now = () => new Date();

const clampValue = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));

type SexKey = PlayerSex | 'U';

interface SexOffsetRecord {
  sex: SexKey;
  bias: number;
  matches: number;
  updatedAt: Date;
}
const MS_PER_WEEK = 7 * 24 * 60 * 60 * 1000;

const MATCH_STAGE_TYPES: ReadonlySet<MatchStage['type']> = new Set([
  'ROUND_OF',
  'GROUP',
  'QUARTERFINAL',
  'SEMIFINAL',
  'FINAL',
  'PLAYOFF',
  'OTHER',
]);

const serializeStageForStorage = (stage: MatchStage | null | undefined) => {
  if (stage === undefined) return undefined;
  if (stage === null) return null;
  const payload: Record<string, unknown> = { type: stage.type };
  if (stage.value !== undefined) payload.value = stage.value;
  if (stage.label !== undefined) payload.label = stage.label;
  return payload;
};

const extractMatchStageFromRaw = (raw: unknown): MatchStage | null => {
  if (!raw || typeof raw !== 'object') return null;
  const stage = (raw as Record<string, unknown>).stage;
  if (!stage || typeof stage !== 'object') return null;
  const record = stage as Record<string, unknown>;
  const type = record.type;
  if (typeof type !== 'string' || !MATCH_STAGE_TYPES.has(type as MatchStage['type'])) {
    return null;
  }

  const normalized: MatchStage = { type: type as MatchStage['type'] };

  if (record.value === undefined || record.value === null) {
    normalized.value = null;
  } else if (typeof record.value === 'number' && Number.isInteger(record.value) && record.value >= 1) {
    normalized.value = record.value;
  } else if (typeof record.value === 'string') {
    const parsed = Number.parseInt(record.value, 10);
    if (Number.isInteger(parsed) && parsed >= 1) {
      normalized.value = parsed;
    } else {
      normalized.value = null;
    }
  }

  if (record.label === undefined || record.label === null) {
    normalized.label = null;
  } else if (typeof record.label === 'string' && record.label.length) {
    normalized.label = record.label;
  }

  return normalized;
};

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

type ReplayLadderResult = {
  report: RatingReplayReportItem;
  refreshTargets: Array<{
    playerId: string;
    organizationId: string;
    sport: Sport;
    discipline: Discipline;
  }>;
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
  providerId: string | null;
  externalRef: string | null;
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
  providerId: string | null;
  externalRef: string | null;
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

export class PostgresStore implements RatingStore {
  private readonly ctx: PostgresStoreContext;
  private readonly organizations: OrganizationsModule;
  private readonly sexOffsets = new Map<string, Map<SexKey, SexOffsetRecord>>();
  private readonly sexOffsetEligibility = new Map<string, { expiresAt: number; allowed: boolean }>();

  constructor(private readonly db = getDb()) {
    this.ctx = createPostgresContext(this.db);
    this.organizations = createOrganizationsModule(this.ctx);
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
    birthDate: Date | null;
    countryCode: string | null;
    regionId: string | null;
    competitiveProfile: unknown | null;
    attributes: unknown | null;
    profilePhotoId: string | null;
    profilePhotoUploadedAt: Date | null;
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
      birthDate: row.birthDate ? row.birthDate.toISOString().slice(0, 10) : undefined,
      countryCode: row.countryCode ?? undefined,
      regionId: row.regionId ?? undefined,
      competitiveProfile: (row.competitiveProfile as PlayerCompetitiveProfile | null) ?? null,
      attributes: (row.attributes as PlayerAttributes | null) ?? null,
      profilePhotoId: row.profilePhotoId ?? undefined,
      profilePhotoUploadedAt: row.profilePhotoUploadedAt?.toISOString(),
    };
  }

  private createDefaultSexOffset(sex: SexKey): SexOffsetRecord {
    return { sex, bias: 0, matches: 0, updatedAt: new Date(0) };
  }

  private async ensureSexOffsetMap(
    ladderId: string,
    tx?: any
  ): Promise<Map<SexKey, SexOffsetRecord>> {
    let map = this.sexOffsets.get(ladderId);
    if (map) return map;

    const rows = (await (tx ?? this.db)
      .select({
        sex: ladderSexOffsets.sex,
        bias: ladderSexOffsets.bias,
        matches: ladderSexOffsets.matches,
        updatedAt: ladderSexOffsets.updatedAt,
      })
      .from(ladderSexOffsets)
      .where(eq(ladderSexOffsets.ladderId, ladderId))) as Array<{
        sex: string;
        bias: number;
        matches: number;
        updatedAt: Date;
      }>;

    map = new Map<SexKey, SexOffsetRecord>();
    for (const row of rows) {
      const sex = (row.sex as SexKey) ?? 'U';
      map.set(sex, {
        sex,
        bias: row.bias,
        matches: row.matches,
        updatedAt: row.updatedAt,
      });
    }

    const ensure = (sex: SexKey) => {
      if (!map!.has(sex)) {
        map!.set(sex, this.createDefaultSexOffset(sex));
      }
    };

    ensure('M');
    ensure('F');
    ensure('X');
    ensure('U');

    this.sexOffsets.set(ladderId, map);
    return map;
  }

  private async resetSexOffsets(tx: any, ladderId: string) {
    await (tx ?? this.db)
      .delete(ladderSexOffsets)
      .where(eq(ladderSexOffsets.ladderId, ladderId));
    this.sexOffsets.delete(ladderId);
    this.sexOffsetEligibility.delete(ladderId);
    await this.ensureSexOffsetMap(ladderId, tx);
  }

  private async getSexBias(
    ladderId: string,
    sex?: PlayerSex | null,
    tx?: any
  ): Promise<number> {
    const map = await this.ensureSexOffsetMap(ladderId, tx);
    const key: SexKey = sex ?? 'U';
    return map.get(key)?.bias ?? 0;
  }

  private async countRecentInterSexMatches(
    tx: any,
    ladderId: string,
    asOf: Date,
    windowDays: number
  ): Promise<number> {
    const cutoff = new Date(asOf.getTime() - windowDays * 24 * 60 * 60 * 1000);
    const rows = (await (tx ?? this.db)
      .select({
        matchId: matches.matchId,
        sex: players.sex,
      })
      .from(matches)
      .innerJoin(matchSides, eq(matchSides.matchId, matches.matchId))
      .innerJoin(matchSidePlayers, eq(matchSidePlayers.matchSideId, matchSides.id))
      .innerJoin(players, eq(matchSidePlayers.playerId, players.playerId))
      .where(and(eq(matches.ladderId, ladderId), gte(matches.startTime, cutoff)))) as Array<{
        matchId: string;
        sex: string | null;
      }>;

    const perMatch = new Map<string, Set<string>>();
    for (const row of rows) {
      if (!row.sex) continue;
      const set = perMatch.get(row.matchId) ?? new Set<string>();
      set.add(row.sex);
      perMatch.set(row.matchId, set);
    }
    let mixed = 0;
    for (const set of perMatch.values()) {
      if (set.size > 1) mixed += 1;
    }
    return mixed;
  }

  private async shouldAllowSexOffsetUpdate(
    tx: any,
    ladderId: string,
    asOf: Date
  ): Promise<boolean> {
    if (!P.sexOffsets?.enabled) return false;

    const params = P.sexOffsets;
    const cache = this.sexOffsetEligibility.get(ladderId);
    const nowMs = asOf.getTime();
    if (cache && cache.expiresAt > nowMs) {
      return cache.allowed;
    }

    const offsets = await this.ensureSexOffsetMap(ladderId, tx);
    const relevantKeys: SexKey[] = ['M', 'F', 'X'];
    const biases = relevantKeys.map((key) => offsets.get(key)?.bias ?? 0);
    const width = biases.length ? Math.max(...biases) - Math.min(...biases) : 0;

    let allow = true;
    if (allow && params.minEdges90d && params.minEdges90d > 0) {
      const mixed = await this.countRecentInterSexMatches(tx, ladderId, asOf, 90);
      if (mixed < params.minEdges90d) {
        allow = false;
      }
    }

    if (allow && params.maxCiWidth !== undefined && params.maxCiWidth >= 0) {
      if (width > params.maxCiWidth) {
        allow = false;
      }
    }

    this.sexOffsetEligibility.set(ladderId, {
      allowed: allow,
      expiresAt: nowMs + 6 * 60 * 60 * 1000,
    });

    return allow;
  }

  private async applySexOffsetSignal(
    tx: any,
    ladderId: string,
    signal?: SexOffsetSignal | null
  ): Promise<void> {
    if (!signal || !P.sexOffsets?.enabled) return;
    const nowTs = new Date();
    if (!(await this.shouldAllowSexOffsetUpdate(tx, ladderId, nowTs))) {
      return;
    }
    const map = await this.ensureSexOffsetMap(ladderId, tx);
    const params = P.sexOffsets;
    const baseline = params.baseline as SexKey;
    const keys: SexKey[] = ['M', 'F', 'X', 'U'];

    for (const key of keys) {
      if (key === 'U') continue;
      const diff = (signal.countsA[key] ?? 0) - (signal.countsB[key] ?? 0);
      if (diff === 0) continue;
      const record = map.get(key);
      if (!record) continue;
      const delta = clampValue(
        params.kFactor * signal.surprise * diff,
        -params.deltaMax,
        params.deltaMax
      );
      const raw = record.bias + delta;
      record.bias = clampValue(raw, -params.maxAbs, params.maxAbs);
      record.matches += Math.abs(diff);
      record.updatedAt = nowTs;
    }

    const baselineRecord = map.get(baseline);
    const shift = baselineRecord?.bias ?? 0;
    if (shift !== 0) {
      for (const key of keys) {
        const record = map.get(key);
        if (!record) continue;
        record.bias -= shift;
      }
    }
    const persistKeys: SexKey[] = ['M', 'F', 'X'];
    for (const key of persistKeys) {
      const record = map.get(key) ?? this.createDefaultSexOffset(key);
      record.updatedAt = nowTs;
      await (tx ?? this.db)
        .insert(ladderSexOffsets)
        .values({
          ladderId,
          sex: key,
          bias: record.bias,
          matches: record.matches,
          updatedAt: nowTs,
        })
        .onConflictDoUpdate({
          target: [ladderSexOffsets.ladderId, ladderSexOffsets.sex],
          set: {
            bias: record.bias,
            matches: record.matches,
            updatedAt: nowTs,
          },
        });
    }

    this.sexOffsetEligibility.delete(ladderId);
  }

  private async applySexOffsetMaintenance(tx: any, asOf: Date) {
    if (!P.sexOffsets?.enabled) return;
    const shrink = P.sexOffsets.regularization ?? 0;
    if (shrink <= 0) return;

    const ladderRows = (await (tx ?? this.db)
      .select({ ladderId: ladderSexOffsets.ladderId })
      .from(ladderSexOffsets)) as Array<{ ladderId: string }>;

    const ladderIds = new Set<string>(ladderRows.map((row) => row.ladderId));
    for (const ladderId of this.sexOffsets.keys()) {
      ladderIds.add(ladderId);
    }

    for (const ladderId of ladderIds) {
      const map = await this.ensureSexOffsetMap(ladderId, tx);
      const params = P.sexOffsets;
      const baseline = params.baseline as SexKey;
      const persistKeys: SexKey[] = ['M', 'F', 'X'];

      for (const record of map.values()) {
        record.bias = clampValue(record.bias * (1 - shrink), -params.maxAbs, params.maxAbs);
        record.updatedAt = asOf;
      }

      const baselineBias = map.get(baseline)?.bias ?? 0;
      if (baselineBias !== 0) {
        for (const record of map.values()) {
          record.bias = clampValue(record.bias - baselineBias, -params.maxAbs, params.maxAbs);
        }
      }

      for (const key of persistKeys) {
        const record = map.get(key) ?? this.createDefaultSexOffset(key);
        await (tx ?? this.db)
          .insert(ladderSexOffsets)
          .values({
            ladderId,
            sex: key,
            bias: record.bias,
            matches: record.matches,
            updatedAt: asOf,
          })
          .onConflictDoUpdate({
            target: [ladderSexOffsets.ladderId, ladderSexOffsets.sex],
            set: {
              bias: record.bias,
              matches: record.matches,
              updatedAt: asOf,
            },
          });
      }

      this.sexOffsetEligibility.delete(ladderId);
    }
  }

  private toEventRecord(row: EventRow): EventRecord {
    return {
      eventId: row.eventId,
      organizationId: row.organizationId,
      providerId: row.providerId ?? null,
      externalRef: row.externalRef ?? null,
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
      providerId: row.providerId ?? null,
      externalRef: row.externalRef ?? null,
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
    const filters: SqlFilter[] = [eq(playerRatingHistory.playerId, query.playerId)];
    if (query.sport) filters.push(eq(ratingLadders.sport, query.sport));
    if (query.discipline) filters.push(eq(ratingLadders.discipline, query.discipline));

    const condition = combineFilters(filters);
    if (!condition) {
      throw new Error('Invalid insight event filter state');
    }

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
      .where(condition)
      .orderBy(playerRatingHistory.createdAt);

    return (rows as PlayerInsightEventRow[]).map((row) => this.toPlayerInsightEvent(row));
  }

  private async fetchPlayerInsightRatings(
    query: PlayerInsightsQuery,
    client = this.db
  ): Promise<PlayerInsightCurrentRating[]> {
    const filters: SqlFilter[] = [eq(playerRatings.playerId, query.playerId)];
    if (query.sport) filters.push(eq(ratingLadders.sport, query.sport));
    if (query.discipline) filters.push(eq(ratingLadders.discipline, query.discipline));

    const condition = combineFilters(filters);
    if (!condition) {
      throw new Error('Invalid insight rating filter state');
    }

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
      .where(condition)
      .orderBy(playerRatings.updatedAt);

    return (rows as PlayerInsightRatingRow[]).map((row) => this.toPlayerInsightRating(row));
  }

  private async getEventRowById(eventId: string, client = this.db): Promise<EventRow | null> {
    const rows = await client
      .select({
        eventId: events.eventId,
        organizationId: events.organizationId,
        providerId: events.providerId,
        externalRef: events.externalRef,
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
        providerId: events.providerId,
        externalRef: events.externalRef,
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

  private async getEventRowByProviderRef(
    providerId: string,
    externalRef: string,
    client = this.db
  ): Promise<EventRow | null> {
    const rows = await client
      .select({
        eventId: events.eventId,
        organizationId: events.organizationId,
        providerId: events.providerId,
        externalRef: events.externalRef,
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
        and(eq(events.providerId, providerId), eq(events.externalRef, externalRef))
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
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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

  private async getCompetitionRowByProviderRef(
    providerId: string,
    externalRef: string,
    client = this.db
  ): Promise<CompetitionRow | null> {
    const rows = await client
      .select({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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
      .where(and(eq(competitions.providerId, providerId), eq(competitions.externalRef, externalRef)))
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

  private async getMatchSummaryById(
    matchId: string,
    options?: { includeRatingEvents?: boolean }
  ): Promise<MatchSummary | null> {
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
        winnerSide: matches.winnerSide,
        ratingStatus: matches.ratingStatus,
        ratingSkipReason: matches.ratingSkipReason,
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
        rawPayload: matches.rawPayload,
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
    const stage = extractMatchStageFromRaw(matchRow.rawPayload);

    const sides: MatchSideSummary[] = ['A', 'B'].map((side) => ({
      side: side as 'A' | 'B',
      players: (sideMap.get(side) ?? []).filter((player): player is string => Boolean(player)),
      participants: sideParticipants?.[side as 'A' | 'B'] ?? null,
    }));

    const summary: MatchSummary = {
      matchId: matchRow.matchId,
      providerId: matchRow.providerId,
      externalRef: matchRow.externalRef ?? null,
      organizationId: matchRow.organizationId,
      sport: matchRow.sport as MatchInput['sport'],
      discipline: matchRow.discipline as MatchInput['discipline'],
      format: matchRow.format,
      tier: matchRow.tier ?? undefined,
      stage,
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
      ratingStatus: (matchRow.ratingStatus as MatchRatingStatus) ?? 'RATED',
      ratingSkipReason:
        (matchRow.ratingSkipReason as MatchRatingSkipReason | null) ?? null,
      winnerSide: (matchRow.winnerSide as WinnerSide | null) ?? null,
    };

    if (options?.includeRatingEvents) {
      const eventsMap = await this.getRatingEventsForMatches([matchRow.matchId]);
      summary.ratingEvents = eventsMap.get(matchRow.matchId) ?? [];
    }

    return summary;
  }

  private async getRatingEventsForMatches(
    matchIds: string[]
  ): Promise<Map<string, RatingEventRecord[]>> {
    if (!matchIds.length) {
      return new Map();
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
      .where(inArray(playerRatingHistory.matchId, matchIds))
      .orderBy(playerRatingHistory.matchId, playerRatingHistory.createdAt, playerRatingHistory.id)) as Array<
      RatingEventRow & { matchId: string | null }
    >;

    const results = new Map<string, RatingEventRecord[]>();
    for (const row of rows) {
      if (!row.matchId) continue;
      const record = this.toRatingEventRecord(row);
      const existing = results.get(row.matchId) ?? [];
      existing.push(record);
      results.set(row.matchId, existing);
    }

    return results;
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
      const activationWeeks = P.idle.activationDays ? P.idle.activationDays / 7 : 0;
      if (activationWeeks && weeks <= activationWeeks) continue;
      const effectiveWeeks = activationWeeks ? weeks - activationWeeks : weeks;
      if (effectiveWeeks <= 0) continue;
      const factor = Math.pow(1 + P.idle.ratePerWeek, effectiveWeeks);
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
    await this.ctx.assertOrganizationExists(organizationId);
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
    await this.ctx.assertOrganizationExists(organizationId);
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
    return this.organizations.createOrganization(input);
  }

  async updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord> {
    return this.organizations.updateOrganization(organizationId, input);
  }

  async listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult> {
    return this.organizations.listOrganizations(query);
  }

  async getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null> {
    return this.organizations.getOrganizationBySlug(slug);
  }

  async getOrganizationById(id: string): Promise<OrganizationRecord | null> {
    return this.organizations.getOrganizationById(id);
  }

  async createEvent(input: EventCreateInput): Promise<EventRecord> {
    await this.ctx.assertOrganizationExists(input.organizationId);
    await this.ensureProvider(input.providerId);

    if (input.externalRef) {
      const existingByRef = await this.getEventRowByProviderRef(input.providerId, input.externalRef);
      if (existingByRef) {
        return this.toEventRecord(existingByRef);
      }
    }

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
        providerId: input.providerId,
        externalRef: input.externalRef ?? null,
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
        providerId: events.providerId,
        externalRef: events.externalRef,
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
        providerId: events.providerId,
        externalRef: events.externalRef,
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
    await this.ctx.assertOrganizationExists(query.organizationId);
    const limit = clampLimit(query.limit);

    const sortField = query.sortField ?? 'slug';
    const sortDirection = query.sortDirection
      ?? (sortField === 'slug' || sortField === 'name' ? 'asc' : 'desc');
    const useLegacySlugCursor = !query.sortField && !query.sortDirection && sortField === 'slug' && sortDirection === 'asc';

    const decodeCursor = (raw: string): {
      sortField: typeof sortField;
      sortDirection: typeof sortDirection;
      value: string | null;
      eventId: string;
    } | null => {
      try {
        const normalized = raw.replace(/-/g, '+').replace(/_/g, '/');
        const padding = (4 - (normalized.length % 4)) % 4;
        const payload = Buffer.from(normalized + '='.repeat(padding), 'base64').toString('utf8');
        const parsed = JSON.parse(payload) as {
          sortField?: typeof sortField;
          sortDirection?: typeof sortDirection;
          value?: string | null;
          eventId?: string;
        };
        if (!parsed || !parsed.sortField || !parsed.sortDirection || !parsed.eventId) {
          return null;
        }
        return {
          sortField: parsed.sortField,
          sortDirection: parsed.sortDirection,
          value: parsed.value ?? null,
          eventId: parsed.eventId,
        };
      } catch {
        return null;
      }
    };

    const encodeCursor = (payload: {
      sortField: typeof sortField;
      sortDirection: typeof sortDirection;
      value: string | null;
      eventId: string;
    }): string => {
      const json = JSON.stringify(payload);
      return Buffer.from(json, 'utf8')
        .toString('base64')
        .replace(/=+$/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');
    };

    const filters: SqlFilter[] = [eq(events.organizationId, query.organizationId)];

    if (query.types && query.types.length) {
      filters.push(inArray(events.type, query.types));
    }

    if (query.season) {
      filters.push(eq(events.season, query.season));
    }

    if (query.sanctioningBody) {
      filters.push(eq(events.sanctioningBody, query.sanctioningBody));
    }

    if (query.startDateFrom) {
      filters.push(gte(events.startDate, new Date(query.startDateFrom)));
    }

    if (query.startDateTo) {
      filters.push(lte(events.startDate, new Date(query.startDateTo)));
    }

    if (query.endDateFrom) {
      filters.push(gte(events.endDate, new Date(query.endDateFrom)));
    }

    if (query.endDateTo) {
      filters.push(lte(events.endDate, new Date(query.endDateTo)));
    }

    if (query.q) {
      const like = `%${query.q}%`;
      filters.push(
        or(sql`${events.name} ILIKE ${like}`, sql`${events.slug} ILIKE ${like}`)
      );
    }

    if (query.sport || query.discipline) {
      const competitionFilters: SqlFilter[] = [eq(competitions.eventId, events.eventId)];
      if (query.sport) {
        competitionFilters.push(eq(competitions.sport, query.sport));
      }
      if (query.discipline) {
        competitionFilters.push(eq(competitions.discipline, query.discipline));
      }
      const competitionCondition = combineFilters(competitionFilters);
      if (competitionCondition) {
        filters.push(sql`EXISTS (SELECT 1 FROM ${competitions} WHERE ${competitionCondition})`);
      }
    }

    if (query.statuses && query.statuses.length) {
      const statusConditions = query.statuses
        .map((status) => {
          switch (status) {
            case 'UPCOMING':
              return sql`(( ${events.startDate} IS NOT NULL AND ${events.startDate} > now() ) OR ( ${events.startDate} IS NULL AND ${events.endDate} IS NOT NULL AND ${events.endDate} > now() ))`;
            case 'IN_PROGRESS':
              return sql`(( ${events.startDate} IS NOT NULL AND ${events.startDate} <= now() AND ( ${events.endDate} IS NULL OR ${events.endDate} >= now() )) OR ( ${events.startDate} IS NULL AND ${events.endDate} IS NOT NULL AND ${events.endDate} >= now() ))`;
            case 'COMPLETED':
              return sql`(( ${events.endDate} IS NOT NULL AND ${events.endDate} < now() ) OR ( ${events.endDate} IS NULL AND ${events.startDate} IS NOT NULL AND ${events.startDate} < now() ))`;
            default:
              return null;
          }
        })
        .filter((clause): clause is NonNullable<typeof clause> => Boolean(clause));
      if (statusConditions.length) {
        const statusCondition = statusConditions
          .slice(1)
          .reduce((acc, clause) => or(acc, clause), statusConditions[0]);
        filters.push(statusCondition);
      }
    }

    if (query.cursor) {
      const parsedCursor = decodeCursor(query.cursor);
      const buildCursorCondition = (payload: {
        sortField: typeof sortField;
        sortDirection: typeof sortDirection;
        value: string | null;
        eventId: string;
      }): SqlFilter => {
        const tieBreakerColumn = events.eventId;
        const tieBreakerValue = payload.eventId;
        switch (payload.sortField) {
          case 'start_date': {
            if (!payload.value) {
              return gt(tieBreakerColumn, tieBreakerValue);
            }
            const cursorDate = new Date(payload.value);
            if (Number.isNaN(cursorDate.getTime())) return null;
            if (payload.sortDirection === 'asc') {
              return or(
                gt(events.startDate, cursorDate),
                and(eq(events.startDate, cursorDate), gt(tieBreakerColumn, tieBreakerValue))
              );
            }
            return or(
              lt(events.startDate, cursorDate),
              and(eq(events.startDate, cursorDate), gt(tieBreakerColumn, tieBreakerValue))
            );
          }
          case 'created_at': {
            if (!payload.value) {
              return gt(tieBreakerColumn, tieBreakerValue);
            }
            const cursorDate = new Date(payload.value);
            if (Number.isNaN(cursorDate.getTime())) return null;
            if (payload.sortDirection === 'asc') {
              return or(
                gt(events.createdAt, cursorDate),
                and(eq(events.createdAt, cursorDate), gt(tieBreakerColumn, tieBreakerValue))
              );
            }
            return or(
              lt(events.createdAt, cursorDate),
              and(eq(events.createdAt, cursorDate), gt(tieBreakerColumn, tieBreakerValue))
            );
          }
          case 'name': {
            if (!payload.value) {
              return gt(tieBreakerColumn, tieBreakerValue);
            }
            if (payload.sortDirection === 'asc') {
              return or(
                gt(events.name, payload.value),
                and(eq(events.name, payload.value), gt(tieBreakerColumn, tieBreakerValue))
              );
            }
            return or(
              lt(events.name, payload.value),
              and(eq(events.name, payload.value), gt(tieBreakerColumn, tieBreakerValue))
            );
          }
          case 'slug':
          default: {
            if (!payload.value) {
              return gt(tieBreakerColumn, tieBreakerValue);
            }
            if (payload.sortDirection === 'asc') {
              return or(
                gt(events.slug, payload.value),
                and(eq(events.slug, payload.value), gt(tieBreakerColumn, tieBreakerValue))
              );
            }
            return or(
              lt(events.slug, payload.value),
              and(eq(events.slug, payload.value), gt(tieBreakerColumn, tieBreakerValue))
            );
          }
        }
      };

      if (
        parsedCursor &&
        parsedCursor.sortField === sortField &&
        parsedCursor.sortDirection === sortDirection
      ) {
        const cursorCondition = buildCursorCondition(parsedCursor);
        if (cursorCondition) {
          filters.push(cursorCondition);
        }
      } else if (useLegacySlugCursor) {
        filters.push(sql`${events.slug} > ${query.cursor}`);
      }
    }

    const condition = combineFilters(filters);

    const selectColumns = {
      eventId: events.eventId,
      organizationId: events.organizationId,
      providerId: events.providerId,
      externalRef: events.externalRef,
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
    } as const;

    let selectQuery = this.db.select(selectColumns).from(events).limit(limit + 1);

    if (condition) {
      selectQuery = selectQuery.where(condition);
    }

    const directionSql = sortDirection === 'desc' ? sql`DESC` : sql`ASC`;
    const orderExpressions = (() => {
      switch (sortField) {
        case 'start_date':
          return [sql`${events.startDate} ${directionSql} NULLS LAST`, sql`${events.eventId} ASC`];
        case 'created_at':
          return [sql`${events.createdAt} ${directionSql} NULLS LAST`, sql`${events.eventId} ASC`];
        case 'name':
          return [sql`${events.name} ${directionSql}`, sql`${events.eventId} ASC`];
        case 'slug':
        default:
          return [sql`${events.slug} ${directionSql}`, sql`${events.eventId} ASC`];
      }
    })();

    selectQuery = selectQuery.orderBy(...orderExpressions);

    const rows = (await selectQuery) as EventRow[];
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const lastRow = page.length ? page[page.length - 1] : null;

    let nextCursor: string | undefined;
    if (hasMore && lastRow) {
      if (useLegacySlugCursor) {
        nextCursor = lastRow.slug;
      } else {
        let cursorValue: string | null = null;
        switch (sortField) {
          case 'start_date':
            cursorValue = lastRow.startDate ? lastRow.startDate.toISOString() : null;
            break;
          case 'created_at':
            cursorValue = lastRow.createdAt ? lastRow.createdAt.toISOString() : null;
            break;
          case 'name':
            cursorValue = lastRow.name;
            break;
          case 'slug':
          default:
            cursorValue = lastRow.slug;
            break;
        }
        nextCursor = encodeCursor({
          sortField,
          sortDirection,
          value: cursorValue,
          eventId: lastRow.eventId,
        });
      }
    }

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
    await this.ctx.assertOrganizationExists(input.organizationId);
    const event = await this.getEventRowById(input.eventId);
    if (!event) {
      throw new EventLookupError(`Event not found: ${input.eventId}`);
    }
    if (event.organizationId !== input.organizationId) {
      throw new EventLookupError(
        `Event ${input.eventId} does not belong to organization ${input.organizationId}`
      );
    }

    await this.ensureProvider(input.providerId);

    if (input.externalRef) {
      const existingByRef = await this.getCompetitionRowByProviderRef(input.providerId, input.externalRef);
      if (existingByRef) {
        if (existingByRef.eventId !== input.eventId) {
          throw new EventLookupError(
            `Competition external_ref already exists for a different event (${existingByRef.eventId})`
          );
        }
        return this.toCompetitionRecord(existingByRef);
      }
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
        providerId: input.providerId,
        externalRef: input.externalRef ?? null,
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
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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

  async listCompetitionsByEventIds(eventIds: string[]): Promise<CompetitionsByEventMap> {
    if (!eventIds.length) {
      return {};
    }

    const rows = await this.db
      .select({
        competitionId: competitions.competitionId,
        eventId: competitions.eventId,
        organizationId: competitions.organizationId,
        providerId: competitions.providerId,
        externalRef: competitions.externalRef,
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
      .where(inArray(competitions.eventId, eventIds))
      .orderBy(competitions.eventId, competitions.slug);

    const result: CompetitionsByEventMap = {};
    for (const row of rows as CompetitionRow[]) {
      const record = this.toCompetitionRecord(row);
      if (!result[record.eventId]) {
        result[record.eventId] = [];
      }
      result[record.eventId].push(record);
    }

    return result;
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
    await this.ctx.assertOrganizationExists(input.organizationId);
    const regionId = await this.ensureRegion(input.regionId ?? null, input.organizationId);

    const birthPatch: { birthYear?: number | null; birthDate?: string | null } = {};
    if (input.birthYear !== undefined) birthPatch.birthYear = input.birthYear;
    if (input.birthDate !== undefined) birthPatch.birthDate = input.birthDate;
    const birth = reconcileBirthDetails({}, birthPatch);

    const birthDateValue = birth.birthDate ? new Date(`${birth.birthDate}T00:00:00.000Z`) : null;

    const profilePhotoUploadedAt = input.profilePhotoUploadedAt
      ? new Date(input.profilePhotoUploadedAt)
      : input.profilePhotoId
        ? now()
        : null;

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
      birthYear: birth.birthYear,
      birthDate: birthDateValue,
      countryCode: input.countryCode,
      regionId,
      competitiveProfile: input.competitiveProfile ?? null,
      attributes: input.attributes ?? null,
      profilePhotoId: input.profilePhotoId ?? null,
      profilePhotoUploadedAt,
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
      birthYear: birth.birthYear ?? undefined,
      birthDate: birth.birthDate ?? undefined,
      countryCode: input.countryCode,
      regionId: regionId ?? undefined,
      externalRef: input.externalRef,
      competitiveProfile: input.competitiveProfile ?? null,
      attributes: input.attributes ?? null,
      profilePhotoId: input.profilePhotoId ?? undefined,
      profilePhotoUploadedAt: profilePhotoUploadedAt?.toISOString(),
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
      profilePhotoId: players.profilePhotoId,
      profilePhotoUploadedAt: players.profilePhotoUploadedAt,
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
      birthDate: players.birthDate,
      countryCode: players.countryCode,
      regionId: players.regionId,
      competitiveProfile: players.competitiveProfile,
      attributes: players.attributes,
      profilePhotoId: players.profilePhotoId,
      profilePhotoUploadedAt: players.profilePhotoUploadedAt,
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
    const birthPatch: { birthYear?: number | null; birthDate?: string | null } = {};
    if (input.birthYear !== undefined) birthPatch.birthYear = input.birthYear;
    if (input.birthDate !== undefined) birthPatch.birthDate = input.birthDate;
    if (Object.keys(birthPatch).length) {
      const currentBirth = {
        birthYear: existing.birthYear,
        birthDate: existing.birthDate ? existing.birthDate.toISOString().slice(0, 10) : null,
      };
      const birth = reconcileBirthDetails(currentBirth, birthPatch);
      updates.birthYear = birth.birthYear;
      updates.birthDate = birth.birthDate ? new Date(`${birth.birthDate}T00:00:00.000Z`) : null;
    }
    if (input.countryCode !== undefined) updates.countryCode = input.countryCode ?? null;
    if (input.competitiveProfile !== undefined) {
      updates.competitiveProfile = input.competitiveProfile ?? null;
    }
    if (input.attributes !== undefined) {
      updates.attributes = input.attributes ?? null;
    }
    if (input.profilePhotoId !== undefined) {
      updates.profilePhotoId = input.profilePhotoId ?? null;
      if (input.profilePhotoUploadedAt === undefined && input.profilePhotoId) {
        updates.profilePhotoUploadedAt = now();
      }
    }
    if (input.profilePhotoUploadedAt !== undefined) {
      updates.profilePhotoUploadedAt = input.profilePhotoUploadedAt
        ? new Date(input.profilePhotoUploadedAt)
        : null;
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

  private async getLadderAgePolicy(ladderId: string): Promise<LadderAgePolicy | null> {
    const rows = await this.db
      .select({
        cutoff: ratingLadders.defaultAgeCutoff,
        groups: ratingLadders.ageBands,
      })
      .from(ratingLadders)
      .where(eq(ratingLadders.ladderId, ladderId))
      .limit(1);

    const row = rows.at(0);
    if (!row) return null;
    const groups = row.groups as Record<string, AgeBandDefinition> | null;
    return {
      cutoff: row.cutoff ? row.cutoff.toISOString().slice(0, 10) : null,
      groups,
    };
  }

  async ensurePlayers(
    ids: string[],
    ladderKey: LadderKey,
    options: { organizationId: string }
  ): Promise<EnsurePlayersResult> {
    const ladderId = await this.ensureLadder(ladderKey);
    if (ids.length === 0) return { ladderId, players: new Map() };

    await this.ctx.assertOrganizationExists(options.organizationId);

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
        sex: players.sex,
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
      const sex = (row.sex ?? undefined) as PlayerSex | undefined;
      const sexBias = await this.getSexBias(ladderId, sex);
      map.set(row.playerId, {
        playerId: row.playerId,
        mu: row.mu,
        sigma: row.sigma,
        matchesCount: row.matchesCount,
        regionId: row.regionId ?? undefined,
        sex,
        sexBias,
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

  private async enqueueReplay(tx: any, ladderId: string, startTime: Date) {
    const timestamp = now();
    await tx
      .insert(ratingReplayQueue)
      .values({
        ladderId,
        earliestStartTime: startTime,
        createdAt: timestamp,
        updatedAt: timestamp,
      })
      .onConflictDoUpdate({
        target: ratingReplayQueue.ladderId,
        set: {
          earliestStartTime: sql`LEAST(${ratingReplayQueue.earliestStartTime}, ${startTime})`,
          updatedAt: timestamp,
        },
      });
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
    const result = params.result ?? null;
    const pairUpdates = params.pairUpdates ?? [];
    const ratingStatus: MatchRatingStatus = params.ratingStatus ?? (result ? 'RATED' : 'UNRATED');
    const ratingSkipReason: MatchRatingSkipReason | null = params.ratingSkipReason ?? null;

    await this.ensureProvider(params.submissionMeta.providerId);
    await this.ctx.assertOrganizationExists(params.submissionMeta.organizationId);
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

    let ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }> = [];

    await this.db.transaction(async (tx: any) => {
      const parseTimestamp = (value?: string | null) => {
        if (!value) return null;
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? null : date;
      };

      const matchStartTime = parseTimestamp(params.submissionMeta.startTime) ?? now();
      const matchCompletedAt = parseTimestamp(params.timing?.completedAt ?? null);
      const appliedAt = matchCompletedAt ?? matchStartTime;

      const latestRows = await tx
        .select({ latestStart: sql<Date | null>`MAX(${matches.startTime})` })
        .from(matches)
        .where(eq(matches.ladderId, params.ladderId));
      const latestStartTime = latestRows.at(0)?.latestStart ?? null;
      const replayRequired = result && latestStartTime ? matchStartTime < latestStartTime : false;

      const rawPayloadSource =
        params.submissionMeta.rawPayload && typeof params.submissionMeta.rawPayload === 'object'
          ? { ...(params.submissionMeta.rawPayload as Record<string, unknown>) }
          : {};
      if (params.stage !== undefined) {
        rawPayloadSource.stage = serializeStageForStorage(params.stage ?? null);
      }

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
        winnerSide: params.match.winner ?? null,
        ratingStatus,
        ratingSkipReason,
        venueId,
        regionId: submissionRegionId ?? null,
        eventId,
        competitionId,
        startTime: matchStartTime,
        timing: params.timing ?? null,
        statistics: params.statistics ?? null,
        segments: params.segments ?? null,
        sideParticipants: params.sideParticipants ?? null,
        rawPayload: rawPayloadSource,
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

      if (result) {
        if (result.sexOffset) {
          await this.applySexOffsetSignal(tx, params.ladderId, result.sexOffset);
          const offsets = await this.ensureSexOffsetMap(params.ladderId, tx);
          for (const state of params.playerStates.values()) {
            const key = (state.sex ?? 'U') as SexKey;
            state.sexBias = offsets.get(key)?.bias ?? 0;
          }
        }
        for (const entry of result.perPlayer) {
          const playerState = params.playerStates.get(entry.playerId);
          await tx
            .update(playerRatings)
            .set({
            mu: entry.muAfter,
            sigma: entry.sigmaAfter,
            matchesCount: playerState?.matchesCount ?? 0,
            updatedAt: appliedAt,
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
            createdAt: appliedAt,
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

        for (const update of pairUpdates) {
          await tx
            .update(pairSynergies)
            .set({
              gamma: update.gammaAfter,
              matches: update.matchesAfter,
              players: update.players,
              updatedAt: appliedAt,
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
            createdAt: appliedAt,
          });
        }
      }

      if (competitionId) {
        await this.ensureCompetitionParticipantsTx(tx, competitionId, Array.from(playerIds));
      }

      if (replayRequired) {
        await this.enqueueReplay(tx, params.ladderId, matchStartTime);
      }
    });

    if (!result) {
      return { matchId, ratingEvents: [] };
    }

    const eventByPlayer = new Map(ratingEvents.map((event) => [event.playerId, event]));
    const missingForPlayers = result.perPlayer
      .map((entry) => entry.playerId)
      .filter((playerId) => !eventByPlayer.has(playerId));

    if (missingForPlayers.length) {
      console.warn('record_match_rating_events_missing', {
        matchId,
        missingPlayers: missingForPlayers,
      });

      const fetched = (await this.db
        .select({
          playerId: playerRatingHistory.playerId,
          ratingEventId: sql<string>`CAST(${playerRatingHistory.id} AS TEXT)`,
          appliedAt: playerRatingHistory.createdAt,
        })
        .from(playerRatingHistory)
        .where(
          and(
            eq(playerRatingHistory.matchId, matchId),
            inArray(playerRatingHistory.playerId, missingForPlayers)
          )
        )) as Array<{ playerId: string; ratingEventId: string; appliedAt: Date }>;

      for (const row of fetched) {
        eventByPlayer.set(row.playerId, {
          playerId: row.playerId,
          ratingEventId: row.ratingEventId,
          appliedAt: row.appliedAt.toISOString(),
        });
      }
    }

    const orderedRatingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }> = [];

    for (const entry of result.perPlayer) {
      const event = eventByPlayer.get(entry.playerId);
      if (!event) {
        throw new Error(`missing rating event for player ${entry.playerId} in match ${matchId}`);
      }
      orderedRatingEvents.push(event);
    }

    ratingEvents = orderedRatingEvents;

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
        startTime: matches.startTime,
        ladderId: matches.ladderId,
        rawPayload: matches.rawPayload,
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
    const rawPayloadBase =
      existing.rawPayload && typeof existing.rawPayload === 'object'
        ? { ...(existing.rawPayload as Record<string, unknown>) }
        : {};
    let rawPayloadUpdated = false;
    let ensureCompetitionId: string | null = null;
    let replayStartTime: Date | null = null;

    if (input.startTime !== undefined) {
      const date = new Date(input.startTime);
      if (Number.isNaN(date.getTime())) {
        throw new MatchLookupError('Invalid start time provided');
      }
      updates.startTime = date;
      const previous = existing.startTime;
      if (previous && date.getTime() !== previous.getTime()) {
        replayStartTime = date < previous ? date : previous;
      }
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

    if (input.stage !== undefined) {
      rawPayloadBase.stage = serializeStageForStorage(input.stage ?? null);
      rawPayloadUpdated = true;
    }

    if (rawPayloadUpdated) {
      updates.rawPayload = rawPayloadBase;
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

    if (replayStartTime) {
      await this.enqueueReplay(this.db, existing.ladderId, replayStartTime);
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

  async getMatch(
    matchId: string,
    organizationId: string,
    options?: { includeRatingEvents?: boolean }
  ): Promise<MatchSummary | null> {
    const summary = await this.getMatchSummaryById(matchId, options);
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
        sex: players.sex,
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
    const sex = (row.sex ?? undefined) as PlayerSex | undefined;
    const sexBias = await this.getSexBias(ladderId, sex);
    return {
      playerId: row.playerId,
      mu: row.mu,
      sigma: row.sigma,
      matchesCount: row.matchesCount,
      regionId: row.regionId ?? undefined,
      sex,
      sexBias,
    };
  }

  async listRatingEvents(query: RatingEventListQuery): Promise<RatingEventListResult> {
    const ladderId = buildLadderId(query.ladderKey);
    const limit = clampLimit(query.limit);

    const filters: SqlFilter[] = [
      eq(playerRatingHistory.playerId, query.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
    ];

    if (query.organizationId) {
      await this.ctx.assertOrganizationExists(query.organizationId);
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
      const parsed = parseNumericRatingEventCursor(query.cursor);
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
      ? buildRatingEventCursor({ createdAt: last.createdAt, id: last.id })
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

    const filters: SqlFilter[] = [
      eq(playerRatingHistory.id, numericId),
      eq(playerRatingHistory.playerId, identifiers.playerId),
      eq(playerRatingHistory.ladderId, ladderId),
    ];

    if (identifiers.organizationId) {
      await this.ctx.assertOrganizationExists(identifiers.organizationId);
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
      await this.ctx.assertOrganizationExists(params.organizationId);
    }

    const filters: SqlFilter[] = [
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
        sex: players.sex,
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
    const sex = (latestRating?.sex ?? null) as PlayerSex | null;
    const bias = await this.getSexBias(ladderId, sex);
    const muRaw = effectiveMu;
    const muAdjusted = muRaw + bias;
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
      mu: muAdjusted,
      muRaw,
      sigma: effectiveSigma,
      ratingEvent: eventRecord,
    } satisfies RatingSnapshot;
  }

  async listLeaderboard(params: LeaderboardQuery): Promise<LeaderboardResult> {
    const timingStart = Date.now();

    const ladderKey: LadderKey = {
      sport: params.sport,
      discipline: params.discipline,
    };
    if (params.segment) {
      ladderKey.segment = params.segment as LadderKey['segment'];
    }
    if (params.classCodes && params.classCodes.length) {
      ladderKey.classCodes = params.classCodes;
    }
    const ladderId = await this.ensureLadder(ladderKey);
    const limit = clampLimit(params.limit);

    const organizationFilter = params.organizationId ? eq(players.organizationId, params.organizationId) : null;

    const baseFilters: any[] = [eq(playerRatings.ladderId, ladderId), gt(playerRatings.matchesCount, 0)];
    if (params.organizationId) {
      await this.ctx.assertOrganizationExists(params.organizationId);
      baseFilters.push(organizationFilter);
    }

    if (params.sex) {
      baseFilters.push(eq(players.sex, params.sex));
    }
    if (params.countryCode) {
      baseFilters.push(eq(players.countryCode, params.countryCode));
    }
    if (params.regionId) {
      baseFilters.push(eq(players.regionId, params.regionId));
    }

    let ageBounds: ResolvedAgeFilter | null = null;
    const wantsAgeFilter =
      params.ageGroup != null ||
      params.ageFrom !== undefined ||
      params.ageTo !== undefined ||
      params.ageCutoff != null;

    if (wantsAgeFilter) {
      const agePolicy = await this.getLadderAgePolicy(ladderId);
      ageBounds = resolveAgeFilter(ladderKey, agePolicy, {
        ageGroup: params.ageGroup ?? null,
        ageFrom: params.ageFrom ?? null,
        ageTo: params.ageTo ?? null,
        ageCutoff: params.ageCutoff ?? null,
      });
    }

    if (ageBounds) {
      let birthDateClause: any | null = null;
      if (ageBounds.minBirthDate || ageBounds.maxBirthDate) {
        birthDateClause = sql`${players.birthDate} IS NOT NULL`;
        if (ageBounds.minBirthDate) {
          birthDateClause = and(birthDateClause, gte(players.birthDate, ageBounds.minBirthDate));
        }
        if (ageBounds.maxBirthDate) {
          birthDateClause = and(birthDateClause, lte(players.birthDate, ageBounds.maxBirthDate));
        }
      }

      let birthYearClause: any | null = null;
      if (ageBounds.minBirthYear != null || ageBounds.maxBirthYear != null) {
        birthYearClause = sql`${players.birthDate} IS NULL`;
        if (ageBounds.minBirthYear != null) {
          birthYearClause = and(birthYearClause, gte(players.birthYear, ageBounds.minBirthYear));
        }
        if (ageBounds.maxBirthYear != null) {
          birthYearClause = and(birthYearClause, lte(players.birthYear, ageBounds.maxBirthYear));
        }
      }

      if (birthDateClause && birthYearClause) {
        baseFilters.push(or(birthDateClause, birthYearClause));
      } else if (birthDateClause) {
        baseFilters.push(birthDateClause);
      } else if (birthYearClause) {
        baseFilters.push(birthYearClause);
      }
    }

    const totalCondition = combineFilters(baseFilters);
    let totalQuery = this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId));

    if (totalCondition) {
      totalQuery = totalQuery.where(totalCondition);
    }

    const totalRows = (await totalQuery) as Array<{ count: number }>;
    const totalCount = totalRows[0]?.count ?? 0;
    if (!totalCount) {
      return { items: [], totalCount: 0, pageSize: limit } satisfies LeaderboardResult;
    }

    const cursor = params.cursor ? decodeLeaderboardCursor(params.cursor) : null;

    const biasExpr = sql<number>`COALESCE(${ladderSexOffsets.bias}, 0)`;
    const effectiveMuExpr = sql<number>`(${playerRatings.mu} + ${biasExpr})`;

    const dataFilters = [...baseFilters];
    if (cursor) {
      dataFilters.push(
        sql`(${effectiveMuExpr} < ${cursor.mu}) OR (${effectiveMuExpr} = ${cursor.mu} AND ${playerRatings.playerId} > ${cursor.playerId})`
      );
    }

    const effectiveCondition = combineFilters(dataFilters);

    const playerRowsQuery = this.db
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
        sex: players.sex,
        effectiveMu: effectiveMuExpr,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .leftJoin(
        ladderSexOffsets,
        and(
          eq(ladderSexOffsets.ladderId, playerRatings.ladderId),
          sql`${ladderSexOffsets.sex} = COALESCE(${players.sex}, 'U')`
        )
      )
      .orderBy(desc(effectiveMuExpr), playerRatings.playerId)
      .limit(limit + 1);

    const conditionedPlayerQuery = effectiveCondition ? playerRowsQuery.where(effectiveCondition) : playerRowsQuery;
    const playerRows = (await conditionedPlayerQuery) as Array<
      PlayerLeaderboardRow & { sex: string | null; effectiveMu: number }
    >;

    if (!playerRows.length) {
      return { items: [], totalCount, pageSize: limit } satisfies LeaderboardResult;
    }

    const hasMore = playerRows.length > limit;
    const limitedRows = hasMore ? playerRows.slice(0, limit) : playerRows;
    const playerIds = limitedRows.map((row) => row.playerId);

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

    const rankOffset = cursor ? cursor.rank : 0;

    const items: LeaderboardEntry[] = limitedRows.map((row, index) => {
      const latest = latestByPlayer.get(row.playerId);
      return {
        rank: rankOffset + index + 1,
        playerId: row.playerId,
        displayName: row.displayName,
        shortName: row.shortName ?? undefined,
        givenName: row.givenName ?? undefined,
        familyName: row.familyName ?? undefined,
        countryCode: row.countryCode ?? undefined,
        regionId: row.playerRegionId ?? undefined,
        mu: row.effectiveMu,
        muRaw: row.mu,
        sigma: row.sigma,
        matches: row.matchesCount,
        delta: latest?.delta ?? null,
        lastEventAt: latest?.createdAt ? latest.createdAt.toISOString() : null,
        lastMatchId: latest?.matchId ?? null,
      } satisfies LeaderboardEntry;
    });

    const nextCursor = hasMore && limitedRows.length
      ? encodeLeaderboardCursor({
          mu: limitedRows[limitedRows.length - 1].effectiveMu,
          playerId: limitedRows[limitedRows.length - 1].playerId,
          rank: rankOffset + limitedRows.length,
        })
      : undefined;

    const response = { items, nextCursor, totalCount, pageSize: limit } satisfies LeaderboardResult;
    const durationMs = Date.now() - timingStart;
    console.debug('leaderboard_query_timing', {
      ladderId,
      totalCount,
      fetched: items.length,
      hasMore,
      durationMs,
    });

    return response;
  }

  async listLeaderboardMovers(params: LeaderboardMoversQuery): Promise<LeaderboardMoversResult> {
    const ladderKey: LadderKey = {
      sport: params.sport,
      discipline: params.discipline,
    };
    if (params.segment) {
      ladderKey.segment = params.segment as LadderKey['segment'];
    }
    if (params.classCodes && params.classCodes.length) {
      ladderKey.classCodes = params.classCodes;
    }
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
      await this.ctx.assertOrganizationExists(params.organizationId);
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
        sex: players.sex,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId));

    if (ratingCondition) {
      ratingQuery = ratingQuery.where(ratingCondition);
    }

    const ratingRows = (await ratingQuery) as Array<PlayerLeaderboardRow & { sex: string | null }>;
    const ratingByPlayer = new Map<string, PlayerLeaderboardRow & { sex: string | null }>(
      ratingRows.map((row) => [row.playerId, row])
    );

    const offsets = await this.ensureSexOffsetMap(ladderId);
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

      const sex = (rating.sex ?? undefined) as PlayerSex | undefined;
      const key = (sex ?? 'U') as SexKey;
      const bias = offsets.get(key)?.bias ?? 0;

      items.push({
        playerId: rating.playerId,
        displayName: rating.displayName,
        shortName: rating.shortName ?? undefined,
        givenName: rating.givenName ?? undefined,
        familyName: rating.familyName ?? undefined,
        countryCode: rating.countryCode ?? undefined,
        regionId: rating.playerRegionId ?? undefined,
        mu: rating.mu + bias,
        muRaw: rating.mu,
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

  async getPlayerRatingsSummary(params: PlayerRatingsSummaryQuery): Promise<PlayerRatingsSummaryResult> {
    const uniqueIds = Array.from(new Set(params.playerIds));
    if (!uniqueIds.length) {
      return { items: [] } satisfies PlayerRatingsSummaryResult;
    }

    const ladderKey: LadderKey = {
      sport: params.sport,
      discipline: params.discipline,
    };
    if (params.segment) {
      ladderKey.segment = params.segment as LadderKey['segment'];
    }
    if (params.classCodes && params.classCodes.length) {
      ladderKey.classCodes = params.classCodes;
    }
    const ladderId = await this.ensureLadder(ladderKey);

    if (params.organizationId) {
      await this.ctx.assertOrganizationExists(params.organizationId);
    }

    const baseFilters: SqlFilter[] = [
      eq(playerRatings.ladderId, ladderId),
      inArray(playerRatings.playerId, uniqueIds),
      gt(playerRatings.matchesCount, 0),
    ];
    if (params.organizationId) {
      baseFilters.push(eq(players.organizationId, params.organizationId));
    }

    const condition = combineFilters(baseFilters);
    if (!condition) {
      return { items: [] } satisfies PlayerRatingsSummaryResult;
    }

    const biasExpr = sql<number>`COALESCE(${ladderSexOffsets.bias}, 0)`;
    const effectiveMuExpr = sql<number>`(${playerRatings.mu} + ${biasExpr})`;

    const playerRows = (await this.db
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
        sex: players.sex,
        effectiveMu: effectiveMuExpr,
      })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .leftJoin(
        ladderSexOffsets,
        and(
          eq(ladderSexOffsets.ladderId, playerRatings.ladderId),
          sql`${ladderSexOffsets.sex} = COALESCE(${players.sex}, 'U')`
        )
      )
      .where(condition)
      .orderBy(desc(effectiveMuExpr), playerRatings.playerId)) as Array<
        PlayerLeaderboardRow & { sex: string | null; effectiveMu: number }
      >;

    if (!playerRows.length) {
      return { items: [] } satisfies PlayerRatingsSummaryResult;
    }

    const historyFilters: SqlFilter[] = [
      eq(playerRatingHistory.ladderId, ladderId),
      inArray(playerRatingHistory.playerId, playerRows.map((row) => row.playerId)),
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
      if (latestByPlayer.size === playerRows.length) {
        break;
      }
    }

    const items: PlayerRatingsSummaryItem[] = [];
    for (const row of playerRows) {
      const latest = latestByPlayer.get(row.playerId);
      const rank = await this.computeLeaderboardRank(
        ladderId,
        row.effectiveMu,
        row.playerId,
        params.organizationId ?? null
      );

      items.push({
        playerId: row.playerId,
        displayName: row.displayName,
        shortName: row.shortName ?? undefined,
        givenName: row.givenName ?? undefined,
        familyName: row.familyName ?? undefined,
        countryCode: row.countryCode ?? undefined,
        regionId: row.playerRegionId ?? undefined,
        mu: row.effectiveMu,
        muRaw: row.mu,
        sigma: row.sigma,
        matches: row.matchesCount,
        delta: latest?.delta ?? null,
        lastEventAt: latest?.createdAt ? latest.createdAt.toISOString() : null,
        lastMatchId: latest?.matchId ?? null,
        rank,
      });
    }

    return { items } satisfies PlayerRatingsSummaryResult;
  }

  private async computeLeaderboardRank(
    ladderId: string,
    effectiveMu: number,
    playerId: string,
    organizationId: string | null
  ): Promise<number> {
    const biasExpr = sql<number>`COALESCE(${ladderSexOffsets.bias}, 0)`;
    const comparisonClause = sql`(${playerRatings.mu} + ${biasExpr} > ${effectiveMu}) OR ((
      ${playerRatings.mu} + ${biasExpr} = ${effectiveMu}
    ) AND ${playerRatings.playerId} < ${playerId})`;

    const filters: SqlFilter[] = [
      eq(playerRatings.ladderId, ladderId),
      gt(playerRatings.matchesCount, 0),
    ];
    if (organizationId) {
      filters.push(eq(players.organizationId, organizationId));
    }

    const baseCondition = combineFilters(filters);
    const whereCondition = baseCondition ? and(baseCondition, comparisonClause) : comparisonClause;

    const rows = (await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(playerRatings)
      .innerJoin(players, eq(playerRatings.playerId, players.playerId))
      .leftJoin(
        ladderSexOffsets,
        and(
          eq(ladderSexOffsets.ladderId, playerRatings.ladderId),
          sql`${ladderSexOffsets.sex} = COALESCE(${players.sex}, 'U')`
        )
      )
      .where(whereCondition)) as Array<{ count: number }>;

    const count = rows[0]?.count ?? 0;
    return count + 1;
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
      await this.applySexOffsetMaintenance(tx, asOf);
    });
  }

  async processRatingReplayQueue(options: RatingReplayQueueOptions = {}): Promise<RatingReplayReport> {
    const dryRun = options.dryRun ?? false;
    const limit = options.limit && options.limit > 0 ? options.limit : 10;

    const queueEntries = await this.db
      .select({
        ladderId: ratingReplayQueue.ladderId,
        earliestStart: ratingReplayQueue.earliestStartTime,
      })
      .from(ratingReplayQueue)
      .orderBy(ratingReplayQueue.earliestStartTime)
      .limit(limit);

    const items: RatingReplayReportItem[] = [];
    const refreshTargets = new Map<string, { playerId: string; organizationId: string; sport: Sport; discipline: Discipline }>();

    for (const entry of queueEntries) {
      const detail = await this.replayLadder(entry.ladderId, {
        from: entry.earliestStart ?? null,
        dryRun,
      });

      if (detail) {
        items.push(detail.report);
        if (!dryRun) {
          await this.db.delete(ratingReplayQueue).where(eq(ratingReplayQueue.ladderId, entry.ladderId));
        }
        if (!dryRun) {
          for (const target of detail.refreshTargets) {
            const key = `${target.playerId}:${target.organizationId}:${target.sport}:${target.discipline}`;
            refreshTargets.set(key, target);
          }
        }
      }
    }

    if (!dryRun && refreshTargets.size) {
      for (const target of refreshTargets.values()) {
        await this.enqueuePlayerInsightsRefresh({
          playerId: target.playerId,
          organizationId: target.organizationId,
          sport: target.sport,
          discipline: target.discipline,
          dedupe: false,
        });
      }
    }

    return this.buildReplayReport(items, dryRun);
  }

  async replayRatings(options: RatingReplayOptions): Promise<RatingReplayReport> {
    if (!options.ladderId) {
      throw new Error('ladderId is required for replayRatings');
    }

    const from = options.from ? new Date(options.from) : null;
    const dryRun = options.dryRun ?? false;

    const detail = await this.replayLadder(options.ladderId, { from, dryRun });
    const items = detail ? [detail.report] : [];
    if (!dryRun) {
      await this.db.delete(ratingReplayQueue).where(eq(ratingReplayQueue.ladderId, options.ladderId));
      if (detail) {
        for (const target of detail.refreshTargets) {
          await this.enqueuePlayerInsightsRefresh({
            playerId: target.playerId,
            organizationId: target.organizationId,
            sport: target.sport,
            discipline: target.discipline,
            dedupe: false,
          });
        }
      }
    }
    return this.buildReplayReport(items, dryRun);
  }

  private async replayLadder(
    ladderId: string,
    options: { from?: Date | null; dryRun: boolean }
  ): Promise<ReplayLadderResult | null> {
    const from = options.from ?? null;
    const dryRun = options.dryRun;
    const replayTimestamp = now();

    return this.db.transaction(async (tx: any) => {
      const ladderRows = await tx
        .select({
          sport: ratingLadders.sport,
          discipline: ratingLadders.discipline,
        })
        .from(ratingLadders)
        .where(eq(ratingLadders.ladderId, ladderId))
        .limit(1);

      const ladder = ladderRows.at(0);
      if (!ladder) {
        throw new Error(`Ladder not found: ${ladderId}`);
      }

      await this.resetSexOffsets(tx, ladderId);

      const offsets = await this.ensureSexOffsetMap(ladderId, tx);

      const ladderKey: LadderKey = {
        sport: ladder.sport as LadderKey['sport'],
        discipline: ladder.discipline as LadderKey['discipline'],
      };

      const existingRatingRows = (await tx
        .select({
          playerId: playerRatings.playerId,
          updatedAt: playerRatings.updatedAt,
        })
        .from(playerRatings)
        .where(eq(playerRatings.ladderId, ladderId))) as Array<{
        playerId: string;
        updatedAt: Date | null;
      }>;

      const playerRegionMap = new Map<string, string | undefined>();
      const playerOrganizationMap = new Map<string, string>();
      const playerSexMap = new Map<string, PlayerSex | undefined>();
      const chunk = <T,>(items: T[], size: number): T[][] => {
        if (items.length <= size) return items.length ? [items] : [];
        const batches: T[][] = [];
        for (let i = 0; i < items.length; i += size) {
          batches.push(items.slice(i, i + size));
        }
        return batches;
      };
      const loadPlayerMeta = async (ids: string[]) => {
        if (!ids.length) return;
        for (const batch of chunk(ids, 500)) {
          const rows = (await tx
            .select({
              playerId: players.playerId,
              regionId: players.regionId,
              organizationId: players.organizationId,
              sex: players.sex,
            })
            .from(players)
            .where(inArray(players.playerId, batch))) as Array<{
            playerId: string;
            regionId: string | null;
            organizationId: string;
            sex: string | null;
          }>;
          for (const row of rows) {
            if (!playerOrganizationMap.has(row.playerId)) {
              playerRegionMap.set(row.playerId, row.regionId ?? undefined);
              playerOrganizationMap.set(row.playerId, row.organizationId);
              playerSexMap.set(row.playerId, (row.sex ?? undefined) as PlayerSex | undefined);
            }
          }
        }
      };

      const existingUpdatedAt = new Map<string, Date>();
      const playerIds = new Set<string>();
      if (existingRatingRows.length) {
        const existingIds: string[] = [];
        for (const row of existingRatingRows) {
          playerIds.add(row.playerId);
          existingIds.push(row.playerId);
          if (row.updatedAt) existingUpdatedAt.set(row.playerId, row.updatedAt);
        }
        await loadPlayerMeta(existingIds);
      }

      const ensurePlayerState = (map: Map<string, PlayerState>, playerId: string) => {
        let state = map.get(playerId);
        if (!state) {
          const sex = playerSexMap.get(playerId);
          const key = (sex ?? 'U') as SexKey;
          const sexBias = offsets.get(key)?.bias ?? 0;
          state = {
            playerId,
            mu: P.baseMu,
            sigma: P.baseSigma,
            matchesCount: 0,
            regionId: playerRegionMap.get(playerId),
            sex,
            sexBias,
          };
          map.set(playerId, state);
        } else {
          const sex = playerSexMap.get(playerId) ?? state.sex;
          state.sex = sex;
          const key = (sex ?? 'U') as SexKey;
          state.sexBias = offsets.get(key)?.bias ?? 0;
        }
        return state;
      };

      const ensurePairState = (map: Map<string, PairState>, playersList: string[]) => {
        const sorted = sortPairPlayers(playersList);
        const pairId = buildPairKey(sorted);
        let state = map.get(pairId);
        if (!state) {
          state = { pairId, players: sorted, gamma: 0, matches: 0 };
          map.set(pairId, state);
        }
        return state;
      };

      const parseTimestamp = (value?: string | null) => {
        if (!value) return null;
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? null : date;
      };

      const playerStates = new Map<string, PlayerState>();
      const pairStates = new Map<string, PairState>();
      const playerLastAppliedAt = new Map<string, Date>();
      const uniquePlayers = new Set<string>();

      let matchesProcessed = 0;
      let pairUpdatesCount = 0;
      let firstApplied: Date | null = null;
      let lastApplied: Date | null = null;

      const baseFilters: SqlFilter[] = [eq(matches.ladderId, ladderId)];
      if (from) {
        baseFilters.push(sql`${matches.startTime} >= ${from}`);
      }
      const baseCondition = combineFilters(baseFilters);

      const chunkSize = 500;
      let cursor: { startTime: Date; matchId: string } | null = null;
      let processedAny = false;
      let clearedExisting = false;
      const historyBatchInsert = async (
        historyRows: Array<{
          playerId: string;
          matchId: string;
          muBefore: number;
          muAfter: number;
          sigmaBefore: number;
          sigmaAfter: number;
          delta: number;
          winProbPre: number;
          movWeight: number | null;
          createdAt: Date;
        }>,
      ) => {
        if (!historyRows.length) return;
        for (const batch of chunk(historyRows, 500)) {
          await tx.insert(playerRatingHistory).values(
            batch.map((row) => ({
              playerId: row.playerId,
              ladderId,
              matchId: row.matchId,
              muBefore: row.muBefore,
              muAfter: row.muAfter,
              sigmaBefore: row.sigmaBefore,
              sigmaAfter: row.sigmaAfter,
              delta: row.delta,
              winProbPre: row.winProbPre,
              movWeight: row.movWeight,
              createdAt: row.createdAt,
            }))
          );
        }
      };

      const pairHistoryBatchInsert = async (
        pairHistoryRows: Array<{
          pairKey: string;
          matchId: string;
          gammaBefore: number;
          gammaAfter: number;
          delta: number;
          createdAt: Date;
        }>,
      ) => {
        if (!pairHistoryRows.length) return;
        for (const batch of chunk(pairHistoryRows, 500)) {
          await tx.insert(pairSynergyHistory).values(
            batch.map((row) => ({
              ladderId,
              pairKey: row.pairKey,
              matchId: row.matchId,
              gammaBefore: row.gammaBefore,
              gammaAfter: row.gammaAfter,
              delta: row.delta,
              createdAt: row.createdAt,
            }))
          );
        }
      };

      while (true) {
        let matchQuery = tx
          .select({
            matchId: matches.matchId,
            organizationId: matches.organizationId,
            startTime: matches.startTime,
            sport: matches.sport,
            discipline: matches.discipline,
            format: matches.format,
            tier: matches.tier,
            timing: matches.timing,
            rawPayload: matches.rawPayload,
          })
          .from(matches)
          .orderBy(matches.startTime, matches.matchId)
          .limit(chunkSize);
        if (baseCondition) {
          matchQuery = matchQuery.where(baseCondition);
        }
        if (cursor) {
          matchQuery = matchQuery.where(
            sql`(${matches.startTime} > ${cursor.startTime}) OR (${matches.startTime} = ${cursor.startTime} AND ${matches.matchId} > ${cursor.matchId})`
          );
        }

        const matchRows = (await matchQuery) as Array<{
          matchId: string;
          organizationId: string;
          startTime: Date;
          sport: string;
          discipline: string;
          format: string;
          tier: string | null;
          timing: unknown;
          rawPayload: unknown;
        }>;

        if (!matchRows.length) {
          if (!processedAny) {
            if (!dryRun) {
              await tx.delete(playerRatingHistory).where(eq(playerRatingHistory.ladderId, ladderId));
              await tx.delete(pairSynergyHistory).where(eq(pairSynergyHistory.ladderId, ladderId));
              await tx.delete(pairSynergies).where(eq(pairSynergies.ladderId, ladderId));
              await tx.delete(playerRatings).where(eq(playerRatings.ladderId, ladderId));
            }

            return {
              report: {
                ladderId,
                ladderKey,
                replayFrom: from ? from.toISOString() : null,
                replayTo: null,
                matchesProcessed: 0,
                playersTouched: 0,
                pairUpdates: 0,
                dryRun,
              },
              refreshTargets: [],
            } satisfies ReplayLadderResult;
          }
          break;
        }

        processedAny = true;

        const matchIds = matchRows.map((row) => row.matchId);
        const sideRows = (await tx
          .select({
            matchId: matchSides.matchId,
            side: matchSides.side,
            playerId: matchSidePlayers.playerId,
            position: matchSidePlayers.position,
          })
          .from(matchSides)
          .innerJoin(matchSidePlayers, eq(matchSidePlayers.matchSideId, matchSides.id))
          .where(inArray(matchSides.matchId, matchIds))
          .orderBy(matchSides.matchId, matchSides.side, matchSidePlayers.position)) as Array<{
          matchId: string;
          side: string;
          playerId: string;
          position: number;
        }>;

        const gameRows = (await tx
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

        const movWeightRows = (await tx
          .select({
            matchId: playerRatingHistory.matchId,
            movWeight: playerRatingHistory.movWeight,
          })
          .from(playerRatingHistory)
          .where(
            and(
              eq(playerRatingHistory.ladderId, ladderId),
              inArray(playerRatingHistory.matchId, matchIds)
            )
          )) as Array<{
          matchId: string;
          movWeight: number | null;
        }>;

        const sidesByMatch = new Map<string, { A: string[]; B: string[] }>();
        const gamesByMatch = new Map<string, Array<{ game_no: number; a: number; b: number }>>();
        const movWeightByMatch = new Map<string, number>();
        const chunkPlayerIds = new Set<string>();

        for (const row of sideRows) {
          const entry = sidesByMatch.get(row.matchId) ?? { A: [] as string[], B: [] as string[] };
          if (row.side === 'A' || row.side === 'B') {
            entry[row.side as 'A' | 'B'][row.position] = row.playerId;
          }
          sidesByMatch.set(row.matchId, entry);
          chunkPlayerIds.add(row.playerId);
        }

        for (const row of gameRows) {
          const list = gamesByMatch.get(row.matchId) ?? [];
          list.push({ game_no: row.gameNo, a: row.scoreA, b: row.scoreB });
          gamesByMatch.set(row.matchId, list);
        }

        for (const row of movWeightRows) {
          if (row.movWeight !== null && !movWeightByMatch.has(row.matchId)) {
            movWeightByMatch.set(row.matchId, row.movWeight);
          }
        }

        const newMetaIds = Array.from(chunkPlayerIds).filter((id) => !playerOrganizationMap.has(id));
        if (newMetaIds.length) {
          await loadPlayerMeta(newMetaIds);
        }
        for (const id of chunkPlayerIds) {
          playerIds.add(id);
        }

        const chunkHistoryRows: Array<{
          playerId: string;
          matchId: string;
          muBefore: number;
          muAfter: number;
          sigmaBefore: number;
          sigmaAfter: number;
          delta: number;
          winProbPre: number;
          movWeight: number | null;
          createdAt: Date;
        }> = [];

        const chunkPairHistoryRows: Array<{
          pairKey: string;
          matchId: string;
          gammaBefore: number;
          gammaAfter: number;
          delta: number;
          createdAt: Date;
        }> = [];

        for (const matchRow of matchRows) {
          const sides = sidesByMatch.get(matchRow.matchId) ?? { A: [] as string[], B: [] as string[] };
          const games = gamesByMatch.get(matchRow.matchId) ?? [];

          const matchInput: MatchInput = {
          sport: matchRow.sport as Sport,
          discipline: matchRow.discipline as Discipline,
          format: matchRow.format,
          tier: (matchRow.tier ?? undefined) as MatchInput['tier'],
          sides: {
            A: { players: (sides.A ?? []).filter((player): player is string => Boolean(player)) },
            B: { players: (sides.B ?? []).filter((player): player is string => Boolean(player)) },
          },
          games: games.map((game) => ({ game_no: game.game_no, a: game.a, b: game.b })),
        };

          const raw = (matchRow.rawPayload ?? null) as { winner?: unknown; mov_weight?: unknown } | null;
          const rawWinner = raw && typeof raw.winner === 'string' ? (raw.winner as string) : null;
          if (rawWinner === 'A' || rawWinner === 'B') {
            matchInput.winner = rawWinner;
          } else {
            let winsA = 0;
            let winsB = 0;
            for (const game of matchInput.games) {
              if (game.a > game.b) winsA += 1;
              else if (game.b > game.a) winsB += 1;
            }
            if (winsA > winsB) matchInput.winner = 'A';
            else if (winsB > winsA) matchInput.winner = 'B';
          }

          let movWeight: number | undefined;
          const rawMov = raw && typeof raw.mov_weight === 'number' ? (raw.mov_weight as number) : null;
          if (typeof rawMov === 'number') {
            movWeight = rawMov;
          } else {
            const historyWeight = movWeightByMatch.get(matchRow.matchId);
            if (typeof historyWeight === 'number') {
              movWeight = historyWeight;
            }
          }
          if (movWeight !== undefined) {
            matchInput.movWeight = movWeight;
          }

          matchInput.sides.A.players.forEach((playerId) => ensurePlayerState(playerStates, playerId));
          matchInput.sides.B.players.forEach((playerId) => ensurePlayerState(playerStates, playerId));

          const pairDescriptors: Array<{ pairId: string; players: string[] }> = [];
          if (matchInput.sides.A.players.length > 1) {
            const pair = ensurePairState(pairStates, matchInput.sides.A.players);
            pairDescriptors.push({ pairId: pair.pairId, players: [...pair.players] });
          }
          if (matchInput.sides.B.players.length > 1) {
            const pair = ensurePairState(pairStates, matchInput.sides.B.players);
            pairDescriptors.push({ pairId: pair.pairId, players: [...pair.players] });
          }

          const result = runMatchUpdate(matchInput, {
            getPlayer: (id) => ensurePlayerState(playerStates, id),
            getPair: pairDescriptors.length
              ? (playersList) => ensurePairState(pairStates, playersList)
              : undefined,
          });

          if (result.sexOffset) {
            await this.applySexOffsetSignal(tx, ladderId, result.sexOffset);
            for (const state of playerStates.values()) {
              const key = (state.sex ?? 'U') as SexKey;
              state.sexBias = offsets.get(key)?.bias ?? 0;
            }
          }

          const timing = (matchRow.timing as MatchTiming | null) ?? null;
          const completedAt = timing ? parseTimestamp(timing.completedAt ?? null) : null;
          const appliedAt = completedAt ?? matchRow.startTime;

          matchesProcessed += 1;
          if (!firstApplied || appliedAt < firstApplied) {
            firstApplied = appliedAt;
          }
          if (!lastApplied || appliedAt > lastApplied) {
            lastApplied = appliedAt;
          }

          for (const entry of result.perPlayer) {
            chunkHistoryRows.push({
              playerId: entry.playerId,
              matchId: matchRow.matchId,
              muBefore: entry.muBefore,
              muAfter: entry.muAfter,
              sigmaBefore: entry.sigmaBefore,
              sigmaAfter: entry.sigmaAfter,
              delta: entry.delta,
              winProbPre: entry.winProbPre,
              movWeight: movWeight ?? null,
              createdAt: appliedAt,
            });
            uniquePlayers.add(entry.playerId);
            playerLastAppliedAt.set(entry.playerId, appliedAt);
          }

          for (const update of result.pairUpdates) {
            pairUpdatesCount += 1;
            chunkPairHistoryRows.push({
              pairKey: update.pairId,
              matchId: matchRow.matchId,
              gammaBefore: update.gammaBefore,
              gammaAfter: update.gammaAfter,
              delta: update.delta,
              createdAt: appliedAt,
            });
          }
        }

        if (!dryRun) {
          if (!clearedExisting) {
            await tx.delete(playerRatingHistory).where(eq(playerRatingHistory.ladderId, ladderId));
            await tx.delete(pairSynergyHistory).where(eq(pairSynergyHistory.ladderId, ladderId));
            await tx.delete(pairSynergies).where(eq(pairSynergies.ladderId, ladderId));
            await tx.delete(playerRatings).where(eq(playerRatings.ladderId, ladderId));
            clearedExisting = true;
          }
          await historyBatchInsert(chunkHistoryRows);
          await pairHistoryBatchInsert(chunkPairHistoryRows);
        }

        const lastMatch = matchRows[matchRows.length - 1];
        cursor = { startTime: lastMatch.startTime, matchId: lastMatch.matchId };
      }

      for (const playerId of playerIds) {
        ensurePlayerState(playerStates, playerId);
      }

      const ratingRows = Array.from(playerStates.values()).map((state) => ({
        playerId: state.playerId,
        ladderId,
        mu: state.mu,
        sigma: state.sigma,
        matchesCount: state.matchesCount,
        updatedAt: playerLastAppliedAt.get(state.playerId)
          ?? existingUpdatedAt.get(state.playerId)
          ?? replayTimestamp,
      }));

      if (!dryRun) {
        for (const batch of chunk(ratingRows, 500)) {
          if (batch.length) {
            await tx.insert(playerRatings).values(batch);
          }
        }

        const pairRows = Array.from(pairStates.values()).map((pairState) => ({
          ladderId,
          pairKey: pairState.pairId,
          players: pairState.players,
          gamma: pairState.gamma,
          matches: pairState.matches,
          createdAt: replayTimestamp,
          updatedAt: replayTimestamp,
        }));

        for (const batch of chunk(pairRows, 500)) {
          if (batch.length) {
            await tx.insert(pairSynergies).values(batch);
          }
        }
      }

      const refreshTargets: ReplayLadderResult['refreshTargets'] = [];
      if (!dryRun) {
        for (const playerId of uniquePlayers) {
          const organizationId = playerOrganizationMap.get(playerId);
          if (!organizationId) continue;
          refreshTargets.push({
            playerId,
            organizationId,
            sport: ladderKey.sport as Sport,
            discipline: ladderKey.discipline as Discipline,
          });
        }
      }

      return {
        report: {
          ladderId,
          ladderKey,
          replayFrom: (from ?? firstApplied)?.toISOString() ?? null,
          replayTo: lastApplied ? lastApplied.toISOString() : null,
          matchesProcessed,
          playersTouched: uniquePlayers.size,
          pairUpdates: pairUpdatesCount,
          dryRun,
        },
        refreshTargets,
      } satisfies ReplayLadderResult;
    });
  }

  private buildReplayReport(items: RatingReplayReportItem[], dryRun: boolean): RatingReplayReport {
    const matchesProcessed = items.reduce((sum, item) => sum + item.matchesProcessed, 0);
    const playersTouched = items.reduce((sum, item) => sum + item.playersTouched, 0);
    return {
      dryRun,
      laddersProcessed: items.length,
      matchesProcessed,
      playersTouched,
      entries: items,
    };
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
    const timingStart = Date.now();

    await this.assertPlayerInOrganization(query.playerId, query.organizationId);
    const [events, ratings] = await Promise.all([
      this.fetchPlayerInsightEvents(query),
      this.fetchPlayerInsightRatings(query),
    ]);

    const snapshot = buildInsightsSnapshot({
      playerId: query.playerId,
      sport: query.sport ?? null,
      discipline: query.discipline ?? null,
      events,
      ratings,
      options,
    });

    const durationMs = Date.now() - timingStart;
    console.debug('player_insights_build_timing', {
      playerId: query.playerId,
      organizationId: query.organizationId,
      sport: query.sport ?? null,
      discipline: query.discipline ?? null,
      events: events.length,
      ratings: ratings.length,
      durationMs,
    });

    return snapshot;
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
  ): Promise<PlayerInsightsJob[]> {
    const requestedBatchSize = Number(options.batchSize ?? 1);
    const batchSize = Number.isFinite(requestedBatchSize)
      ? Math.max(1, Math.min(requestedBatchSize, 100))
      : 1;

    const result = await this.db.execute(sql`
      WITH claimed AS (
        SELECT job_id
        FROM player_insight_jobs
        WHERE status = 'PENDING'
          AND run_at <= now()
        ORDER BY run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT ${batchSize}
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

    if (!rows?.length) return [];

    return rows.map((row) => ({
      jobId: row.jobId,
      playerId: row.playerId,
      organizationId: row.organizationId,
      sport: (row.sport ?? null) as Sport | null,
      discipline: (row.discipline ?? null) as Discipline | null,
      runAt: row.runAt instanceof Date
        ? row.runAt.toISOString()
        : new Date(row.runAt).toISOString(),
      status: row.status as PlayerInsightsJob['status'],
      attempts: row.attempts,
      lockedAt: row.lockedAt
        ? (row.lockedAt instanceof Date ? row.lockedAt.toISOString() : new Date(row.lockedAt).toISOString())
        : null,
      lockedBy: row.lockedBy ?? null,
      payload: (row.payload ?? null) as Record<string, unknown> | null,
      lastError: row.lastError ?? null,
    } satisfies PlayerInsightsJob));
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

  async getPlayerInsightAiState(input: PlayerInsightAiEnsureInput): Promise<PlayerInsightAiData | null> {
    await this.assertPlayerInOrganization(input.playerId, input.organizationId);
    const scopeKey = buildInsightScopeKey(input.sport ?? null, input.discipline ?? null);

    const rows = await this.db
      .select({
        snapshotDigest: playerInsightAi.snapshotDigest,
        promptVersion: playerInsightAi.promptVersion,
        status: playerInsightAi.status,
        narrative: playerInsightAi.narrative,
        model: playerInsightAi.model,
        tokensPrompt: playerInsightAi.tokensPrompt,
        tokensCompletion: playerInsightAi.tokensCompletion,
        tokensTotal: playerInsightAi.tokensTotal,
        generatedAt: playerInsightAi.generatedAt,
        lastRequestedAt: playerInsightAi.lastRequestedAt,
        expiresAt: playerInsightAi.expiresAt,
        pollAfterMs: playerInsightAi.pollAfterMs,
        errorCode: playerInsightAi.errorCode,
        errorMessage: playerInsightAi.errorMessage,
      })
      .from(playerInsightAi)
      .where(
        and(
          eq(playerInsightAi.playerId, input.playerId),
          eq(playerInsightAi.organizationId, input.organizationId),
          eq(playerInsightAi.scopeKey, scopeKey),
          eq(playerInsightAi.promptVersion, input.promptVersion)
        )
      )
      .limit(1);

    const row = rows.at(0);
    if (!row) return null;
    return this.mapAiRow(row);
  }

  async ensurePlayerInsightAiState(
    input: PlayerInsightAiEnsureInput
  ): Promise<PlayerInsightAiEnsureResult> {
    await this.assertPlayerInOrganization(input.playerId, input.organizationId);
    const scopeKey = buildInsightScopeKey(input.sport ?? null, input.discipline ?? null);
    const requestedAt = input.requestedAt ? new Date(input.requestedAt) : now();

    const existingRows = await this.db
      .select({
        snapshotDigest: playerInsightAi.snapshotDigest,
        promptVersion: playerInsightAi.promptVersion,
        status: playerInsightAi.status,
        narrative: playerInsightAi.narrative,
        model: playerInsightAi.model,
        tokensPrompt: playerInsightAi.tokensPrompt,
        tokensCompletion: playerInsightAi.tokensCompletion,
        tokensTotal: playerInsightAi.tokensTotal,
        generatedAt: playerInsightAi.generatedAt,
        lastRequestedAt: playerInsightAi.lastRequestedAt,
        expiresAt: playerInsightAi.expiresAt,
        pollAfterMs: playerInsightAi.pollAfterMs,
        errorCode: playerInsightAi.errorCode,
        errorMessage: playerInsightAi.errorMessage,
      })
      .from(playerInsightAi)
      .where(
        and(
          eq(playerInsightAi.playerId, input.playerId),
          eq(playerInsightAi.organizationId, input.organizationId),
          eq(playerInsightAi.scopeKey, scopeKey),
          eq(playerInsightAi.promptVersion, input.promptVersion)
        )
      )
      .limit(1);

    const existing = existingRows.at(0);
    const pollAfterMs = input.pollAfterMs ?? existing?.pollAfterMs ?? (existing ? existing.pollAfterMs ?? null : 1500);

    const shouldReset =
      !existing ||
      existing.snapshotDigest !== input.snapshotDigest ||
      (existing.status !== 'READY' && existing.status !== 'PENDING');

    if (shouldReset) {
      await this.db
        .insert(playerInsightAi)
        .values({
          playerId: input.playerId,
          organizationId: input.organizationId,
          sport: input.sport ?? null,
          discipline: input.discipline ?? null,
          scopeKey,
          promptVersion: input.promptVersion,
          snapshotDigest: input.snapshotDigest,
          status: 'PENDING',
          narrative: null,
          model: null,
          tokensPrompt: null,
          tokensCompletion: null,
          tokensTotal: null,
          generatedAt: null,
          lastRequestedAt: requestedAt,
          expiresAt: null,
          pollAfterMs,
          errorCode: null,
          errorMessage: null,
        })
        .onConflictDoUpdate({
          target: [
            playerInsightAi.playerId,
            playerInsightAi.organizationId,
            playerInsightAi.scopeKey,
            playerInsightAi.promptVersion,
          ],
          set: {
            sport: input.sport ?? null,
            discipline: input.discipline ?? null,
            snapshotDigest: input.snapshotDigest,
            status: 'PENDING',
            narrative: null,
            model: null,
            tokensPrompt: null,
            tokensCompletion: null,
            tokensTotal: null,
            generatedAt: null,
            lastRequestedAt: requestedAt,
            expiresAt: null,
            pollAfterMs,
            errorCode: null,
            errorMessage: null,
            updatedAt: sql`now()`,
          },
        });
    } else {
      await this.db
        .update(playerInsightAi)
        .set({
          lastRequestedAt: requestedAt,
          pollAfterMs,
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(playerInsightAi.playerId, input.playerId),
            eq(playerInsightAi.organizationId, input.organizationId),
            eq(playerInsightAi.scopeKey, scopeKey),
            eq(playerInsightAi.promptVersion, input.promptVersion)
          )
        );
    }

    const refreshed = await this.db
      .select({
        snapshotDigest: playerInsightAi.snapshotDigest,
        promptVersion: playerInsightAi.promptVersion,
        status: playerInsightAi.status,
        narrative: playerInsightAi.narrative,
        model: playerInsightAi.model,
        tokensPrompt: playerInsightAi.tokensPrompt,
        tokensCompletion: playerInsightAi.tokensCompletion,
        tokensTotal: playerInsightAi.tokensTotal,
        generatedAt: playerInsightAi.generatedAt,
        lastRequestedAt: playerInsightAi.lastRequestedAt,
        expiresAt: playerInsightAi.expiresAt,
        pollAfterMs: playerInsightAi.pollAfterMs,
        errorCode: playerInsightAi.errorCode,
        errorMessage: playerInsightAi.errorMessage,
      })
      .from(playerInsightAi)
      .where(
        and(
          eq(playerInsightAi.playerId, input.playerId),
          eq(playerInsightAi.organizationId, input.organizationId),
          eq(playerInsightAi.scopeKey, scopeKey),
          eq(playerInsightAi.promptVersion, input.promptVersion)
        )
      )
      .limit(1);

    const stateRow = refreshed.at(0);
    if (!stateRow) {
      throw new Error('Failed to load AI insight state after upsert');
    }

    const state = this.mapAiRow(stateRow);

    let jobId: string | null = null;
    let enqueued = false;
    if (state.status === 'PENDING' && input.enqueue !== false) {
      const result = await this.enqueuePlayerInsightAiJob({
        organizationId: input.organizationId,
        playerId: input.playerId,
        sport: input.sport ?? null,
        discipline: input.discipline ?? null,
        snapshotDigest: input.snapshotDigest,
        promptVersion: input.promptVersion,
        runAt: requestedAt,
        payload: input.payload ?? null,
        dedupe: true,
      });
      jobId = result.jobId;
      enqueued = result.enqueued;
    }

    return { state, jobId, enqueued };
  }

  async savePlayerInsightAiResult(input: PlayerInsightAiResultInput): Promise<PlayerInsightAiData> {
    await this.assertPlayerInOrganization(input.playerId, input.organizationId);
    const scopeKey = buildInsightScopeKey(input.sport ?? null, input.discipline ?? null);

    const generatedAt = input.generatedAt ? new Date(input.generatedAt) : null;
    const expiresAt = input.expiresAt ? new Date(input.expiresAt) : null;

    const existingRows = await this.db
      .select({
        lastRequestedAt: playerInsightAi.lastRequestedAt,
        pollAfterMs: playerInsightAi.pollAfterMs,
      })
      .from(playerInsightAi)
      .where(
        and(
          eq(playerInsightAi.playerId, input.playerId),
          eq(playerInsightAi.organizationId, input.organizationId),
          eq(playerInsightAi.scopeKey, scopeKey),
          eq(playerInsightAi.promptVersion, input.promptVersion)
        )
      )
      .limit(1);

    const existing = existingRows.at(0);

    await this.db
      .insert(playerInsightAi)
      .values({
        playerId: input.playerId,
        organizationId: input.organizationId,
        sport: input.sport ?? null,
        discipline: input.discipline ?? null,
        scopeKey,
        promptVersion: input.promptVersion,
        snapshotDigest: input.snapshotDigest,
        status: input.status,
        narrative: input.narrative ?? null,
        model: input.model ?? null,
        tokensPrompt: input.tokens?.prompt ?? null,
        tokensCompletion: input.tokens?.completion ?? null,
        tokensTotal: input.tokens?.total ?? null,
        generatedAt,
        lastRequestedAt: existing?.lastRequestedAt ?? null,
        expiresAt,
        pollAfterMs: existing?.pollAfterMs ?? null,
        errorCode: input.errorCode ?? null,
        errorMessage: input.errorMessage ?? null,
      })
      .onConflictDoUpdate({
        target: [
          playerInsightAi.playerId,
          playerInsightAi.organizationId,
          playerInsightAi.scopeKey,
          playerInsightAi.promptVersion,
        ],
        set: {
          sport: input.sport ?? null,
          discipline: input.discipline ?? null,
          snapshotDigest: input.snapshotDigest,
          status: input.status,
          narrative: input.narrative ?? null,
          model: input.model ?? null,
          tokensPrompt: input.tokens?.prompt ?? null,
          tokensCompletion: input.tokens?.completion ?? null,
          tokensTotal: input.tokens?.total ?? null,
          generatedAt,
          expiresAt,
          errorCode: input.errorCode ?? null,
          errorMessage: input.errorMessage ?? null,
          updatedAt: sql`now()`,
        },
      });

    const rows = await this.db
      .select({
        snapshotDigest: playerInsightAi.snapshotDigest,
        promptVersion: playerInsightAi.promptVersion,
        status: playerInsightAi.status,
        narrative: playerInsightAi.narrative,
        model: playerInsightAi.model,
        tokensPrompt: playerInsightAi.tokensPrompt,
        tokensCompletion: playerInsightAi.tokensCompletion,
        tokensTotal: playerInsightAi.tokensTotal,
        generatedAt: playerInsightAi.generatedAt,
        lastRequestedAt: playerInsightAi.lastRequestedAt,
        expiresAt: playerInsightAi.expiresAt,
        pollAfterMs: playerInsightAi.pollAfterMs,
        errorCode: playerInsightAi.errorCode,
        errorMessage: playerInsightAi.errorMessage,
      })
      .from(playerInsightAi)
      .where(
        and(
          eq(playerInsightAi.playerId, input.playerId),
          eq(playerInsightAi.organizationId, input.organizationId),
          eq(playerInsightAi.scopeKey, scopeKey),
          eq(playerInsightAi.promptVersion, input.promptVersion)
        )
      )
      .limit(1);

    const row = rows.at(0);
    if (!row) {
      throw new Error('Failed to load AI insight state after save');
    }

    return this.mapAiRow(row);
  }

  async enqueuePlayerInsightAiJob(
    input: PlayerInsightAiEnqueueInput
  ): Promise<{ jobId: string; enqueued: boolean }> {
    await this.assertPlayerInOrganization(input.playerId, input.organizationId);
    const scopeKey = buildInsightScopeKey(input.sport ?? null, input.discipline ?? null);
    const runAt = input.runAt ? new Date(input.runAt) : now();

    const existingRows = await this.db
      .select({
        jobId: playerInsightAiJobs.jobId,
        status: playerInsightAiJobs.status,
        snapshotDigest: playerInsightAiJobs.snapshotDigest,
      })
      .from(playerInsightAiJobs)
      .where(
        and(
          eq(playerInsightAiJobs.playerId, input.playerId),
          eq(playerInsightAiJobs.organizationId, input.organizationId),
          eq(playerInsightAiJobs.scopeKey, scopeKey),
          eq(playerInsightAiJobs.promptVersion, input.promptVersion),
          sql`${playerInsightAiJobs.status} IN ('PENDING', 'IN_PROGRESS')`
        )
      )
      .limit(1);

    const existing = existingRows.at(0);

    if (existing) {
      const digestChanged = existing.snapshotDigest !== input.snapshotDigest;
      if (input.dedupe === false || digestChanged) {
        const updateData: Record<string, unknown> = {
          status: 'PENDING',
          runAt,
          lockedAt: null,
          lockedBy: null,
          updatedAt: sql`now()`,
          lastError: null,
          snapshotDigest: input.snapshotDigest,
        };
        if (input.payload !== undefined) {
          updateData.payload = input.payload;
        }
        await this.db
          .update(playerInsightAiJobs)
          .set(updateData)
          .where(eq(playerInsightAiJobs.jobId, existing.jobId));
        return { jobId: existing.jobId, enqueued: true };
      }

      if (input.payload !== undefined) {
        await this.db
          .update(playerInsightAiJobs)
          .set({ payload: input.payload, updatedAt: sql`now()` })
          .where(eq(playerInsightAiJobs.jobId, existing.jobId));
      }

      return { jobId: existing.jobId, enqueued: false };
    }

    const jobId = randomUUID();
    await this.db.insert(playerInsightAiJobs).values({
      jobId,
      playerId: input.playerId,
      organizationId: input.organizationId,
      sport: input.sport ?? null,
      discipline: input.discipline ?? null,
      scopeKey,
      promptVersion: input.promptVersion,
      snapshotDigest: input.snapshotDigest,
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

  async claimPlayerInsightAiJob(
    options: PlayerInsightAiJobClaimOptions
  ): Promise<PlayerInsightAiJob | null> {
    const result = await this.db.execute(sql`
      WITH claimed AS (
        SELECT job_id
        FROM player_insight_ai_jobs
        WHERE status = 'PENDING'
          AND run_at <= now()
        ORDER BY run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE player_insight_ai_jobs
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
                scope_key AS "scopeKey",
                prompt_version AS "promptVersion",
                snapshot_digest AS "snapshotDigest",
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
      scopeKey: string;
      promptVersion: string;
      snapshotDigest: string;
      runAt: Date | string;
      status: string;
      attempts: number;
      lockedAt: Date | string | null;
      lockedBy: string | null;
      payload: unknown;
      lastError: string | null;
    }>;
    const row = rows?.[0];
    if (!row) return null;
    const toIso = (value: Date | string | null | undefined) => {
      if (!value) return null;
      if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? null : value.toISOString();
      }
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
    };
    return {
      jobId: row.jobId,
      playerId: row.playerId,
      organizationId: row.organizationId,
      sport: (row.sport ?? null) as Sport | null,
      discipline: (row.discipline ?? null) as Discipline | null,
      scopeKey: row.scopeKey,
      promptVersion: row.promptVersion,
      snapshotDigest: row.snapshotDigest,
      runAt: toIso(row.runAt) ?? new Date().toISOString(),
      status: row.status as PlayerInsightAiJob['status'],
      attempts: row.attempts,
      lockedAt: toIso(row.lockedAt),
      lockedBy: row.lockedBy ?? null,
      payload: (row.payload ?? null) as Record<string, unknown> | null,
      lastError: row.lastError ?? null,
    } satisfies PlayerInsightAiJob;
  }

  async completePlayerInsightAiJob(result: PlayerInsightAiJobCompletion): Promise<void> {
    const condition = and(
      eq(playerInsightAiJobs.jobId, result.jobId),
      or(
        sql`${playerInsightAiJobs.lockedBy} IS NULL`,
        eq(playerInsightAiJobs.lockedBy, result.workerId)
      )
    );

    if (result.success) {
      await this.db
        .update(playerInsightAiJobs)
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

    await this.db.update(playerInsightAiJobs).set(updateData).where(condition);
  }

  private mapAiRow(row: {
    snapshotDigest: string;
    promptVersion: string;
    status: string;
    narrative: string | null;
    model: string | null;
    tokensPrompt: number | null;
    tokensCompletion: number | null;
    tokensTotal: number | null;
    generatedAt: Date | null;
    lastRequestedAt: Date | null;
    expiresAt: Date | null;
    pollAfterMs: number | null;
    errorCode: string | null;
    errorMessage: string | null;
  }): PlayerInsightAiData {
    const hasTokens =
      row.tokensPrompt !== null || row.tokensCompletion !== null || row.tokensTotal !== null;
    return {
      snapshotDigest: row.snapshotDigest,
      promptVersion: row.promptVersion,
      status: row.status as PlayerInsightAiData['status'],
      narrative: row.narrative ?? null,
      model: row.model ?? null,
      generatedAt: row.generatedAt ? row.generatedAt.toISOString() : null,
      tokens: hasTokens
        ? {
            prompt: row.tokensPrompt ?? 0,
            completion: row.tokensCompletion ?? 0,
            total:
              row.tokensTotal ??
              (row.tokensPrompt ?? 0) +
                (row.tokensCompletion ?? 0),
          }
        : null,
      lastRequestedAt: row.lastRequestedAt ? row.lastRequestedAt.toISOString() : null,
      expiresAt: row.expiresAt ? row.expiresAt.toISOString() : null,
      pollAfterMs: row.pollAfterMs ?? null,
      errorCode: row.errorCode ?? null,
      errorMessage: row.errorMessage ?? null,
    } satisfies PlayerInsightAiData;
  }

  async listPlayers(query: PlayerListQuery): Promise<PlayerListResult> {
    const limit = clampLimit(query.limit);
    await this.ctx.assertOrganizationExists(query.organizationId);
    const filters: SqlFilter[] = [eq(players.organizationId, query.organizationId)];

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
      birthDate: players.birthDate,
        countryCode: players.countryCode,
        regionId: players.regionId,
        externalRef: players.externalRef,
        competitiveProfile: players.competitiveProfile,
        attributes: players.attributes,
        profilePhotoId: players.profilePhotoId,
        profilePhotoUploadedAt: players.profilePhotoUploadedAt,
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
        birthDate: Date | null;
        countryCode: string | null;
        regionId: string | null;
        externalRef: string | null;
        competitiveProfile: unknown | null;
        attributes: unknown | null;
        profilePhotoId: string | null;
        profilePhotoUploadedAt: Date | null;
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
      birthDate: row.birthDate ? row.birthDate.toISOString().slice(0, 10) : undefined,
      countryCode: row.countryCode ?? undefined,
      regionId: row.regionId ?? undefined,
      externalRef: row.externalRef ?? undefined,
      competitiveProfile: (row.competitiveProfile as PlayerCompetitiveProfile | null) ?? null,
      attributes: (row.attributes as PlayerAttributes | null) ?? null,
      profilePhotoId: row.profilePhotoId ?? undefined,
      profilePhotoUploadedAt: row.profilePhotoUploadedAt?.toISOString(),
    }));

    return { items, nextCursor };
  }

  async countPlayersBySport(query: PlayerSportTotalsQuery): Promise<PlayerSportTotalsResult> {
    await this.ctx.assertOrganizationExists(query.organizationId);

    const filters: SqlFilter[] = [eq(players.organizationId, query.organizationId)];

    if (query.sport) {
      filters.push(eq(ratingLadders.sport, query.sport));
    }

    if (query.discipline) {
      filters.push(eq(ratingLadders.discipline, query.discipline));
    }

    const condition = combineFilters(filters);

    let totalsQuery = this.db
      .select({
        sport: ratingLadders.sport,
        totalPlayers: sql<number>`CAST(count(DISTINCT ${playerRatings.playerId}) AS INTEGER)`,
      })
      .from(playerRatings)
      .innerJoin(ratingLadders, eq(ratingLadders.ladderId, playerRatings.ladderId))
      .innerJoin(players, eq(players.playerId, playerRatings.playerId))
      .groupBy(ratingLadders.sport)
      .orderBy(ratingLadders.sport);

    if (condition) {
      totalsQuery = totalsQuery.where(condition);
    }

    const rows = (await totalsQuery) as Array<{ sport: Sport; totalPlayers: number }>;

    return {
      totals: rows.map((row) => ({ sport: row.sport, totalPlayers: row.totalPlayers })),
    };
  }

  async listMatches(query: MatchListQuery): Promise<MatchListResult> {
    const limit = clampLimit(query.limit);
    await this.ctx.assertOrganizationExists(query.organizationId);
    const filters: SqlFilter[] = [eq(matches.organizationId, query.organizationId)];

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
        winnerSide: matches.winnerSide,
        ratingStatus: matches.ratingStatus,
        ratingSkipReason: matches.ratingSkipReason,
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
      winnerSide: string | null;
      ratingStatus: string | null;
      ratingSkipReason: string | null;
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
      rawPayload: unknown | null;
    }>;

    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    const nextCursor = hasMore && last
      ? buildMatchCursor({ startTime: last.startTime, matchId: last.matchId })
      : undefined;

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
      const stage = extractMatchStageFromRaw(row.rawPayload);
      return {
        matchId: row.matchId,
        providerId: row.providerId,
        externalRef: row.externalRef ?? null,
        organizationId: row.organizationId,
        sport: row.sport as MatchInput['sport'],
        discipline: row.discipline as MatchInput['discipline'],
        format: row.format,
        tier: row.tier ?? undefined,
        stage,
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
        ratingStatus: (row.ratingStatus as MatchRatingStatus) ?? 'RATED',
        ratingSkipReason: (row.ratingSkipReason as MatchRatingSkipReason | null) ?? null,
        winnerSide: (row.winnerSide as WinnerSide | null) ?? null,
      };
    });

    if (query.includeRatingEvents) {
      const ratingEventsMap = await this.getRatingEventsForMatches(matchIds);
      for (const item of items) {
        item.ratingEvents = ratingEventsMap.get(item.matchId) ?? [];
      }
    }

    let includedPlayers: MatchPlayerSummaryEmbed[] | undefined;
    if (query.includePlayers) {
      const playerIds = new Set<string>();
      for (const match of items) {
        for (const side of match.sides) {
          side.players.forEach((id) => playerIds.add(id));
        }
      }
      if (playerIds.size) {
        const playerRows = (await this.db
          .select({
            playerId: players.playerId,
            displayName: players.displayName,
            shortName: players.shortName,
            givenName: players.givenName,
            familyName: players.familyName,
            countryCode: players.countryCode,
            regionId: players.regionId,
          })
          .from(players)
          .where(inArray(players.playerId, Array.from(playerIds)))) as Array<{
            playerId: string;
            displayName: string;
            shortName: string | null;
            givenName: string | null;
            familyName: string | null;
            countryCode: string | null;
            regionId: string | null;
          }>;

        includedPlayers = playerRows.map((row) => ({
          playerId: row.playerId,
          displayName: row.displayName,
          shortName: row.shortName ?? undefined,
          givenName: row.givenName ?? undefined,
          familyName: row.familyName ?? undefined,
          countryCode: row.countryCode ?? undefined,
          regionId: row.regionId ?? undefined,
        } satisfies MatchPlayerSummaryEmbed));
      }
    }

    let includedEvents: MatchEventSummaryEmbed[] | undefined;
    if (query.includeEvents) {
      const eventIds = new Set<string>();
      for (const match of items) {
        if (match.eventId) eventIds.add(match.eventId);
      }
      if (eventIds.size) {
        const eventRows = (await this.db
          .select({
            eventId: events.eventId,
            name: events.name,
            slug: events.slug,
            startDate: events.startDate,
            endDate: events.endDate,
            classification: events.classification,
            season: events.season,
          })
          .from(events)
          .where(inArray(events.eventId, Array.from(eventIds)))) as Array<{
            eventId: string;
            name: string | null;
            slug: string | null;
            startDate: Date | null;
            endDate: Date | null;
            classification: string | null;
            season: string | null;
          }>;

        includedEvents = eventRows.map((row) => ({
          eventId: row.eventId,
          name: row.name ?? null,
          slug: row.slug ?? null,
          startDate: row.startDate ? row.startDate.toISOString() : null,
          endDate: row.endDate ? row.endDate.toISOString() : null,
          classification: row.classification ?? null,
          season: row.season ?? null,
        } satisfies MatchEventSummaryEmbed));
      }
    }

    const included = {
      ...(includedPlayers && includedPlayers.length ? { players: includedPlayers } : {}),
      ...(includedEvents && includedEvents.length ? { events: includedEvents } : {}),
    };

    return {
      items,
      nextCursor,
      ...(Object.keys(included).length ? { included } : {}),
    } satisfies MatchListResult;
  }

  async countMatchesBySport(query: MatchSportTotalsQuery): Promise<MatchSportTotalsResult> {
    await this.ctx.assertOrganizationExists(query.organizationId);

    const filters: SqlFilter[] = [eq(matches.organizationId, query.organizationId)];

    if (query.sport) {
      filters.push(eq(matches.sport, query.sport));
    }

    if (query.discipline) {
      filters.push(eq(matches.discipline, query.discipline));
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
        sport: matches.sport,
        totalMatches: sql<number>`CAST(count(*) AS INTEGER)`,
      })
      .from(matches)
      .groupBy(matches.sport)
      .orderBy(matches.sport);

    if (condition) {
      matchQuery = matchQuery.where(condition);
    }

    const rows = (await matchQuery) as Array<{ sport: MatchInput['sport']; totalMatches: number }>;

    return {
      totals: rows.map((row) => ({ sport: row.sport, totalMatches: row.totalMatches })),
    };
  }
}
