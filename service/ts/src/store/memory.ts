import { randomUUID } from 'crypto';
import { P } from '../engine/params.js';
import type { MatchInput, PairState, PairUpdate, PlayerState, UpdateResult } from '../engine/types.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerUpdateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
  RecordMatchResult,
  EnsurePairSynergiesParams,
  EnsurePairSynergiesResult,
  PlayerListQuery,
  PlayerListResult,
  MatchListQuery,
  MatchListResult,
  MatchSummary,
  MatchGameSummary,
  MatchUpdateInput,
  OrganizationCreateInput,
  OrganizationUpdateInput,
  OrganizationListQuery,
  OrganizationListResult,
  OrganizationRecord,
  RatingEventListQuery,
  RatingEventListResult,
  RatingEventRecord,
  RatingSnapshot,
  NightlyStabilizationOptions,
  LeaderboardQuery,
  LeaderboardResult,
  LeaderboardEntry,
  LeaderboardMoversQuery,
  LeaderboardMoversResult,
  LeaderboardMoverEntry,
} from './types.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError } from './types.js';
import { buildLadderId, DEFAULT_REGION } from './helpers.js';

interface MemoryRatingRecord extends PlayerState {
  updatedAt: Date;
}

interface MemoryPlayerRecord extends PlayerRecord {
  ratings: Map<string, MemoryRatingRecord>;
}

interface MemoryRatingEvent {
  ratingEventId: string;
  organizationId: string;
  playerId: string;
  ladderId: string;
  matchId: string | null;
  appliedAt: Date;
  muBefore: number;
  muAfter: number;
  delta: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  winProbPre: number | null;
  movWeight: number | null;
}

interface MemoryPairSynergy {
  key: string;
  pairId: string;
  ladderId: string;
  players: string[];
  gamma: number;
  matches: number;
  updatedAt: Date;
}

interface MemoryPairSynergyHistory {
  pairId: string;
  ladderId: string;
  matchId: string;
  gammaBefore: number;
  gammaAfter: number;
  delta: number;
  createdAt: Date;
}

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 200;

const clampLimit = (limit?: number) => {
  if (!limit || limit < 1) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
};

const slugify = (value: string) =>
  value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '') || randomUUID();

const buildMatchCursor = (startTime: Date, matchId: string) => `${startTime.toISOString()}|${matchId}`;

const parseMatchCursor = (cursor: string) => {
  const [ts, id] = cursor.split('|');
  if (!ts || !id) return null;
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return null;
  return { startTime: date, matchId: id };
};

const buildRatingEventCursor = (appliedAt: Date, id: string) => `${appliedAt.toISOString()}|${id}`;

const parseRatingEventCursor = (cursor: string) => {
  const [ts, id] = cursor.split('|');
  if (!ts || !id) return null;
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return null;
  return { appliedAt: date, id };
};

const clampValue = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));
const MS_PER_WEEK = 7 * 24 * 60 * 60 * 1000;

const toPlayerRecord = (player: MemoryPlayerRecord): PlayerRecord => ({
  playerId: player.playerId,
  organizationId: player.organizationId,
  displayName: player.displayName,
  shortName: player.shortName,
  nativeName: player.nativeName,
  givenName: player.givenName,
  familyName: player.familyName,
  sex: player.sex,
  birthYear: player.birthYear,
  countryCode: player.countryCode,
  regionId: player.regionId,
  externalRef: player.externalRef,
});

const paginatePlayers = (
  players: MemoryPlayerRecord[],
  cursor: string | undefined,
  limit: number
): PlayerListResult => {
  let startIndex = 0;
  if (cursor) {
    startIndex = players.findIndex((p) => p.playerId > cursor);
    if (startIndex === -1) {
      return { items: [], nextCursor: undefined };
    }
  }

  const slice = players.slice(startIndex, startIndex + limit);
  const nextCursor = players.length > startIndex + slice.length && slice.length
    ? slice[slice.length - 1].playerId
    : undefined;

  return {
    items: slice.map(toPlayerRecord),
    nextCursor,
  };
};

export class MemoryStore implements RatingStore {
  private organizations = new Map<string, OrganizationRecord>();
  private organizationsBySlug = new Map<string, string>();
  private players = new Map<string, MemoryPlayerRecord>();
  private matches: Array<{
    matchId: string;
    ladderId: string;
    match: MatchInput;
    result: UpdateResult;
    startTime: Date;
    organizationId: string;
    sport: MatchInput['sport'];
    discipline: MatchInput['discipline'];
    format: string;
    tier?: string;
    venueId?: string | null;
    regionId?: string | null;
  }> = [];
  private ratingEvents = new Map<string, Map<string, MemoryRatingEvent[]>>();
  private pairSynergies = new Map<string, MemoryPairSynergy>();
  private pairSynergyHistory: MemoryPairSynergyHistory[] = [];

  async createPlayer(input: PlayerCreateInput): Promise<PlayerRecord> {
    const playerId = randomUUID();
    this.assertOrganizationExists(input.organizationId);
    const record: MemoryPlayerRecord = {
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
      regionId: input.regionId,
      externalRef: input.externalRef,
      ratings: new Map(),
    };
    this.players.set(playerId, record);
    return record;
  }

  async updatePlayer(playerId: string, organizationId: string, input: PlayerUpdateInput): Promise<PlayerRecord> {
    const player = this.players.get(playerId);
    if (!player) {
      throw new PlayerLookupError(`Player not found: ${playerId}`, { missing: [playerId] });
    }
    if (player.organizationId !== organizationId) {
      throw new PlayerLookupError(
        `Player not registered to organization ${organizationId}: ${playerId}`,
        { wrongOrganization: [playerId] }
      );
    }

    if (input.displayName !== undefined) player.displayName = input.displayName;
    if (input.shortName !== undefined) player.shortName = input.shortName ?? undefined;
    if (input.nativeName !== undefined) player.nativeName = input.nativeName ?? undefined;
    if (input.externalRef !== undefined) player.externalRef = input.externalRef ?? undefined;
    if (input.givenName !== undefined) player.givenName = input.givenName ?? undefined;
    if (input.familyName !== undefined) player.familyName = input.familyName ?? undefined;
    if (input.sex !== undefined) player.sex = (input.sex ?? undefined) as MemoryPlayerRecord['sex'];
    if (input.birthYear !== undefined) player.birthYear = input.birthYear ?? undefined;
    if (input.countryCode !== undefined) player.countryCode = input.countryCode ?? undefined;
    if (input.regionId !== undefined) player.regionId = input.regionId ?? undefined;

    return toPlayerRecord(player);
  }

  async ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult> {
    const ladderId = buildLadderId(ladderKey);
    this.assertOrganizationExists(ladderKey.organizationId);
    const playersMap = new Map<string, PlayerState>();

    const missing: string[] = [];
    const wrongOrg: string[] = [];

    for (const id of ids) {
      const player = this.players.get(id);
      if (!player) {
        missing.push(id);
        continue;
      }
      if (player.organizationId !== ladderKey.organizationId) {
        wrongOrg.push(id);
        continue;
      }

      let rating = player.ratings.get(ladderId);
      if (!rating) {
        rating = {
          playerId: id,
          mu: P.baseMu,
          sigma: P.baseSigma,
          matchesCount: 0,
          updatedAt: new Date(0),
        };
        player.ratings.set(ladderId, rating);
      } else if (!rating.updatedAt) {
        rating.updatedAt = new Date(0);
      }
      playersMap.set(id, rating);
    }

    if (missing.length || wrongOrg.length) {
      throw new PlayerLookupError(
        missing.length
          ? `Players not found: ${missing.join(', ')}`
          : `Players not registered to organization ${ladderKey.organizationId}: ${wrongOrg.join(', ')}`,
        { missing: missing.length ? missing : undefined, wrongOrganization: wrongOrg.length ? wrongOrg : undefined }
      );
    }

    return { ladderId, players: playersMap };
  }

  async ensurePairSynergies(params: EnsurePairSynergiesParams): Promise<EnsurePairSynergiesResult> {
    const map = new Map<string, PairState>();
    const ladderId = params.ladderId;
    const now = new Date();

    for (const pair of params.pairs) {
      const key = `${ladderId}::${pair.pairId}`;
      let record = this.pairSynergies.get(key);
      if (!record) {
        record = {
          key,
          pairId: pair.pairId,
          ladderId,
          players: pair.players,
          gamma: 0,
          matches: 0,
          updatedAt: new Date(0),
        };
        this.pairSynergies.set(key, record);
      } else {
        record.players = pair.players;
      }

      map.set(pair.pairId, {
        pairId: record.pairId,
        players: [...pair.players],
        gamma: record.gamma,
        matches: record.matches,
      });
    }

    if (params.pairs.length) {
      // touch timestamp for tracked pairs to reflect access (optional)
      params.pairs.forEach((pair) => {
        const key = `${ladderId}::${pair.pairId}`;
        const record = this.pairSynergies.get(key);
        if (record && !record.updatedAt) {
          record.updatedAt = now;
        }
      });
    }

    return map;
  }

  async recordMatch(params: RecordMatchParams): Promise<RecordMatchResult> {
    const matchId = randomUUID();
    this.assertOrganizationExists(params.ladderKey.organizationId);
    const ladderId = buildLadderId(params.ladderKey);
    this.matches.push({
      matchId,
      ladderId,
      match: params.match,
      result: params.result,
      startTime: new Date(params.submissionMeta.startTime),
      organizationId: params.ladderKey.organizationId,
      sport: params.match.sport,
      discipline: params.match.discipline,
      format: params.match.format,
      tier: params.match.tier,
      venueId: params.submissionMeta.venueId ?? null,
      regionId: params.submissionMeta.regionId ?? null,
    });

    const ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }> = [];
    const appliedAtBase = new Date(params.submissionMeta.startTime ?? new Date().toISOString());
    for (const entry of params.result.perPlayer) {
      const appliedAt = new Date(appliedAtBase);
      const event: MemoryRatingEvent = {
        ratingEventId: randomUUID(),
        organizationId: params.ladderKey.organizationId,
        playerId: entry.playerId,
        ladderId,
        matchId,
        appliedAt,
        muBefore: entry.muBefore,
        muAfter: entry.muAfter,
        delta: entry.delta,
        sigmaBefore: entry.sigmaBefore,
        sigmaAfter: entry.sigmaAfter,
        winProbPre: entry.winProbPre,
        movWeight: params.match.movWeight ?? null,
      };

      const bucket = this.getRatingEventBucket(ladderId, entry.playerId, true)!;
      bucket.unshift(event);
      ratingEvents.push({
        playerId: entry.playerId,
        ratingEventId: event.ratingEventId,
        appliedAt: appliedAt.toISOString(),
      });

      const player = this.players.get(entry.playerId);
      const ratingRecord = player?.ratings.get(ladderId);
      if (ratingRecord) {
        ratingRecord.updatedAt = appliedAt;
      }
    }

    const pairTimestamp = new Date(params.submissionMeta.startTime ?? new Date().toISOString());
    for (const update of params.pairUpdates) {
      const key = `${ladderId}::${update.pairId}`;
      let record = this.pairSynergies.get(key);
      if (!record) {
        record = {
          key,
          pairId: update.pairId,
          ladderId,
          players: update.players,
          gamma: update.gammaAfter,
          matches: update.matchesAfter,
          updatedAt: pairTimestamp,
        };
        this.pairSynergies.set(key, record);
      } else {
        record.gamma = update.gammaAfter;
        record.matches = update.matchesAfter;
        record.players = update.players;
        record.updatedAt = pairTimestamp;
      }

      this.pairSynergyHistory.push({
        pairId: update.pairId,
        ladderId,
        matchId,
        gammaBefore: update.gammaBefore,
        gammaAfter: update.gammaAfter,
        delta: update.delta,
        createdAt: new Date(),
      });
    }

    return { matchId, ratingEvents };
  }

  async updateMatch(matchId: string, organizationId: string, input: MatchUpdateInput): Promise<MatchSummary> {
    const match = this.matches.find((entry) => entry.matchId === matchId);
    if (!match) {
      throw new MatchLookupError(`Match not found: ${matchId}`);
    }
    if (match.organizationId !== organizationId) {
      throw new MatchLookupError(`Match does not belong to organization ${organizationId}`);
    }

    if (input.startTime !== undefined) {
      const date = new Date(input.startTime);
      if (Number.isNaN(date.getTime())) {
        throw new MatchLookupError('Invalid start time provided');
      }
      match.startTime = date;
    }

    if (input.venueId !== undefined) {
      match.venueId = input.venueId ?? null;
    }

    if (input.regionId !== undefined) {
      match.regionId = input.regionId ?? null;
    }

    return {
      matchId: match.matchId,
      organizationId: match.organizationId,
      sport: match.sport,
      discipline: match.discipline,
      format: match.format,
      tier: match.tier,
      startTime: match.startTime.toISOString(),
      venueId: match.venueId ?? null,
      regionId: match.regionId ?? null,
      sides: ['A', 'B'].map((side) => ({
        side: side as 'A' | 'B',
        players: match.match.sides[side as 'A' | 'B'].players,
      })),
      games: match.match.games.map((g) => ({ gameNo: g.game_no, a: g.a, b: g.b })),
    } satisfies MatchSummary;
  }

  async getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null> {
    const ladderId = buildLadderId(ladderKey);
    const player = this.players.get(playerId);
    if (!player) return null;
    return player.ratings.get(ladderId) ?? null;
  }

  async listRatingEvents(query: RatingEventListQuery): Promise<RatingEventListResult> {
    const ladderId = buildLadderId(query.ladderKey);
    this.assertOrganizationExists(query.ladderKey.organizationId);
    this.assertPlayerInOrganization(query.playerId, query.ladderKey.organizationId);

    const limit = clampLimit(query.limit);
    let events = [...(this.getRatingEventBucket(ladderId, query.playerId) ?? [])];

    if (query.matchId) {
      events = events.filter((event) => event.matchId === query.matchId);
    }

    if (query.since) {
      const since = new Date(query.since);
      if (!Number.isNaN(since.getTime())) {
        events = events.filter((event) => event.appliedAt >= since);
      }
    }

    if (query.until) {
      const until = new Date(query.until);
      if (!Number.isNaN(until.getTime())) {
        events = events.filter((event) => event.appliedAt < until);
      }
    }

    if (query.cursor) {
      const parsed = parseRatingEventCursor(query.cursor);
      if (parsed) {
        events = events.filter((event) => {
          if (event.appliedAt < parsed.appliedAt) return true;
          if (event.appliedAt > parsed.appliedAt) return false;
          return event.ratingEventId < parsed.id;
        });
      }
    }

    const slice = events.slice(0, limit);
    const nextCursor = events.length > limit && slice.length
      ? buildRatingEventCursor(slice[slice.length - 1].appliedAt, slice[slice.length - 1].ratingEventId)
      : undefined;

    return {
      items: slice.map((event) => this.toRatingEventRecord(event)),
      nextCursor,
    } satisfies RatingEventListResult;
  }

  async getRatingEvent(
    identifiers: { ladderKey: LadderKey; playerId: string; ratingEventId: string }
  ): Promise<RatingEventRecord | null> {
    const ladderId = buildLadderId(identifiers.ladderKey);
    this.assertOrganizationExists(identifiers.ladderKey.organizationId);
    this.assertPlayerInOrganization(identifiers.playerId, identifiers.ladderKey.organizationId);

    const events = this.getRatingEventBucket(ladderId, identifiers.playerId) ?? [];
    const event = events.find((entry) => entry.ratingEventId === identifiers.ratingEventId);
    if (!event) return null;
    return this.toRatingEventRecord(event);
  }

  async getRatingSnapshot(
    params: { playerId: string; ladderKey: LadderKey; asOf?: string }
  ): Promise<RatingSnapshot | null> {
    const ladderId = buildLadderId(params.ladderKey);
    this.assertOrganizationExists(params.ladderKey.organizationId);
    this.assertPlayerInOrganization(params.playerId, params.ladderKey.organizationId);

    const events = this.getRatingEventBucket(ladderId, params.playerId) ?? [];
    const asOfDate = params.asOf ? new Date(params.asOf) : null;
    const validAsOf = asOfDate && !Number.isNaN(asOfDate.getTime()) ? asOfDate : null;

    const event = validAsOf
      ? events.find((entry) => entry.appliedAt <= validAsOf)
      : events[0];

    const player = this.players.get(params.playerId);
    const rating = player?.ratings.get(ladderId);

    if (!event && !rating) {
      return null;
    }

    const mu = event ? event.muAfter : rating?.mu ?? P.baseMu;
    const sigma = event ? event.sigmaAfter : rating?.sigma ?? P.baseSigma;
    const asOf = validAsOf
      ? validAsOf.toISOString()
      : event
        ? event.appliedAt.toISOString()
        : new Date().toISOString();

    return {
      organizationId: params.ladderKey.organizationId,
      playerId: params.playerId,
      ladderId,
      asOf,
      mu,
      sigma,
      ratingEvent: event ? this.toRatingEventRecord(event) : null,
    } satisfies RatingSnapshot;
  }

  async listLeaderboard(params: LeaderboardQuery): Promise<LeaderboardResult> {
    const ladderId = buildLadderId(params.ladderKey);
    this.assertOrganizationExists(params.ladderKey.organizationId);
    const limit = clampLimit(params.limit);

    const candidates: Array<{
      player: MemoryPlayerRecord;
      rating: MemoryRatingRecord;
      latest?: MemoryRatingEvent;
    }> = [];

    for (const player of this.players.values()) {
      if (player.organizationId !== params.ladderKey.organizationId) continue;
      const rating = player.ratings.get(ladderId);
      if (!rating) continue;
      const events = this.getRatingEventBucket(ladderId, player.playerId) ?? [];
      candidates.push({ player, rating, latest: events[0] });
    }

    candidates.sort((a, b) => {
      const diff = b.rating.mu - a.rating.mu;
      if (diff !== 0) return diff;
      return a.player.playerId.localeCompare(b.player.playerId);
    });

    const items: LeaderboardEntry[] = candidates.slice(0, limit).map((entry, index) => ({
      rank: index + 1,
      playerId: entry.player.playerId,
      displayName: entry.player.displayName,
      shortName: entry.player.shortName ?? undefined,
      givenName: entry.player.givenName ?? undefined,
      familyName: entry.player.familyName ?? undefined,
      countryCode: entry.player.countryCode ?? undefined,
      regionId: entry.player.regionId ?? undefined,
      mu: entry.rating.mu,
      sigma: entry.rating.sigma,
      matches: entry.rating.matchesCount,
      delta: entry.latest?.delta ?? null,
      lastEventAt: entry.latest ? entry.latest.appliedAt.toISOString() : null,
      lastMatchId: entry.latest?.matchId ?? null,
    }));

    return { items };
  }

  async listLeaderboardMovers(params: LeaderboardMoversQuery): Promise<LeaderboardMoversResult> {
    const ladderId = buildLadderId(params.ladderKey);
    this.assertOrganizationExists(params.ladderKey.organizationId);
    const since = new Date(params.since);
    if (Number.isNaN(since.getTime())) {
      throw new Error('Invalid since timestamp');
    }
    const limit = clampLimit(params.limit);

    const movers: Array<{
      player: MemoryPlayerRecord;
      rating: MemoryRatingRecord;
      events: MemoryRatingEvent[];
      change: number;
    }> = [];

    for (const player of this.players.values()) {
      if (player.organizationId !== params.ladderKey.organizationId) continue;
      const rating = player.ratings.get(ladderId);
      if (!rating) continue;
      const events = (this.getRatingEventBucket(ladderId, player.playerId) ?? []).filter((event) => event.appliedAt >= since);
      if (!events.length) continue;
      const change = events.reduce((sum, event) => sum + event.delta, 0);
      if (change === 0) continue;
      movers.push({ player, rating, events, change });
    }

    movers.sort((a, b) => {
      const diff = b.change - a.change;
      if (diff !== 0) return diff;
      const aTime = a.events[0]?.appliedAt.getTime() ?? 0;
      const bTime = b.events[0]?.appliedAt.getTime() ?? 0;
      return bTime - aTime;
    });

    const items: LeaderboardMoverEntry[] = movers.slice(0, limit).map((entry) => ({
      playerId: entry.player.playerId,
      displayName: entry.player.displayName,
      shortName: entry.player.shortName ?? undefined,
      givenName: entry.player.givenName ?? undefined,
      familyName: entry.player.familyName ?? undefined,
      countryCode: entry.player.countryCode ?? undefined,
      regionId: entry.player.regionId ?? undefined,
      mu: entry.rating.mu,
      sigma: entry.rating.sigma,
      matches: entry.rating.matchesCount,
      change: entry.change,
      events: entry.events.length,
      lastEventAt: entry.events[0]?.appliedAt.toISOString() ?? null,
    }));

    return { items };
  }

  async runNightlyStabilization(options: NightlyStabilizationOptions = {}): Promise<void> {
    const asOf = options.asOf ?? new Date();
    const horizonDays = options.horizonDays ?? P.graph.horizonDays;
    this.applyInactivity(asOf);
    this.applySynergyDecay(asOf);
    this.applyRegionBias(asOf);
    this.applyGraphSmoothing(asOf, horizonDays);
    this.applyDriftControl();
  }

  async listPlayers(query: PlayerListQuery): Promise<PlayerListResult> {
    const limit = clampLimit(query.limit);
    this.assertOrganizationExists(query.organizationId);
    let players = Array.from(this.players.values()).filter((player) => player.organizationId === query.organizationId);

    if (query.q) {
      const lower = query.q.toLowerCase();
      players = players.filter((player) =>
        player.displayName.toLowerCase().includes(lower) ||
        (player.givenName ?? '').toLowerCase().includes(lower) ||
        (player.familyName ?? '').toLowerCase().includes(lower)
      );
    }

    players.sort((a, b) => a.playerId.localeCompare(b.playerId));
    return paginatePlayers(players, query.cursor, limit);
  }

  async listMatches(query: MatchListQuery): Promise<MatchListResult> {
    const limit = clampLimit(query.limit);
    this.assertOrganizationExists(query.organizationId);
    let matches = this.matches.filter((entry) => entry.organizationId === query.organizationId);

    if (query.sport) {
      matches = matches.filter((entry) => entry.sport === query.sport);
    }

    if (query.playerId) {
      matches = matches.filter((entry) =>
        ['A', 'B'].some((side) => entry.match.sides[side as 'A' | 'B'].players.includes(query.playerId!))
      );
    }

    if (query.startAfter) {
      const after = new Date(query.startAfter);
      if (!Number.isNaN(after.getTime())) {
        matches = matches.filter((entry) => entry.startTime >= after);
      }
    }

    if (query.startBefore) {
      const before = new Date(query.startBefore);
      if (!Number.isNaN(before.getTime())) {
        matches = matches.filter((entry) => entry.startTime <= before);
      }
    }

    matches.sort((a, b) => {
      const diff = b.startTime.getTime() - a.startTime.getTime();
      if (diff !== 0) return diff;
      return b.matchId.localeCompare(a.matchId);
    });

    if (query.cursor) {
      const parsed = parseMatchCursor(query.cursor);
      if (parsed) {
        matches = matches.filter((entry) => {
          if (entry.startTime < parsed.startTime) return true;
          if (entry.startTime > parsed.startTime) return false;
          return entry.matchId < parsed.matchId;
        });
      }
    }

    const slice = matches.slice(0, limit);
    const nextCursor = matches.length > limit && slice.length
      ? buildMatchCursor(slice[slice.length - 1].startTime, slice[slice.length - 1].matchId)
      : undefined;

    const items: MatchSummary[] = slice.map((entry) => ({
      matchId: entry.matchId,
      organizationId: entry.organizationId,
      sport: entry.sport,
      discipline: entry.discipline,
      format: entry.format,
      tier: entry.tier,
      startTime: entry.startTime.toISOString(),
      venueId: entry.venueId ?? null,
      regionId: entry.regionId ?? null,
      sides: ['A', 'B'].map((side) => ({
        side: side as 'A' | 'B',
        players: entry.match.sides[side as 'A' | 'B'].players,
      })),
      games: entry.match.games.map((g) => ({ gameNo: g.game_no, a: g.a, b: g.b })),
    }));

    return { items, nextCursor };
  }

  async createOrganization(input: OrganizationCreateInput): Promise<OrganizationRecord> {
    const organizationId = randomUUID();
    const slug = (input.slug ?? slugify(input.name)).toLowerCase();
    if (this.organizationsBySlug.has(slug)) {
      throw new OrganizationLookupError(`Slug already in use: ${slug}`);
    }
    const record: OrganizationRecord = {
      organizationId,
      name: input.name,
      slug,
      description: input.description ?? null,
      createdAt: new Date().toISOString(),
    };
    this.organizations.set(organizationId, record);
    this.organizationsBySlug.set(slug, organizationId);
    return record;
  }

  async updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord> {
    const record = this.organizations.get(organizationId);
    if (!record) {
      throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
    }

    let currentSlug = record.slug;

    if (input.slug !== undefined) {
      const nextSlug = input.slug.toLowerCase();
      if (nextSlug !== currentSlug) {
        if (this.organizationsBySlug.has(nextSlug)) {
          throw new OrganizationLookupError(`Slug already in use: ${nextSlug}`);
        }
        this.organizationsBySlug.delete(currentSlug);
        this.organizationsBySlug.set(nextSlug, organizationId);
        currentSlug = nextSlug;
      }
    }

    if (input.name !== undefined) {
      record.name = input.name;
    }
    if (input.description !== undefined) {
      record.description = input.description;
    }
    if (input.slug !== undefined) {
      record.slug = currentSlug;
    }

    return { ...record, slug: currentSlug } satisfies OrganizationRecord;
  }

  async listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult> {
    const limit = clampLimit(query.limit);
    let orgs = Array.from(this.organizations.values());
    if (query.q) {
      const lower = query.q.toLowerCase();
      orgs = orgs.filter((org) => org.name.toLowerCase().includes(lower) || org.slug.includes(lower));
    }
    orgs.sort((a, b) => a.slug.localeCompare(b.slug));
    let startIndex = 0;
    if (query.cursor) {
      startIndex = orgs.findIndex((org) => org.slug > query.cursor!);
      if (startIndex === -1) return { items: [], nextCursor: undefined };
    }
    const slice = orgs.slice(startIndex, startIndex + limit);
    const nextCursor = orgs.length > startIndex + slice.length && slice.length
      ? slice[slice.length - 1].slug
      : undefined;
    return { items: slice, nextCursor };
  }

  async getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null> {
    const id = this.organizationsBySlug.get(slug);
    if (!id) return null;
    return this.organizations.get(id) ?? null;
  }

  async getOrganizationById(id: string): Promise<OrganizationRecord | null> {
    return this.organizations.get(id) ?? null;
  }

  private getRatingEventBucket(ladderId: string, playerId: string, create = false) {
    let ladderBuckets = this.ratingEvents.get(ladderId);
    if (!ladderBuckets && create) {
      ladderBuckets = new Map<string, MemoryRatingEvent[]>();
      this.ratingEvents.set(ladderId, ladderBuckets);
    }
    if (!ladderBuckets) return undefined;

    let events = ladderBuckets.get(playerId);
    if (!events && create) {
      events = [];
      ladderBuckets.set(playerId, events);
    }
    return events;
  }

  private toRatingEventRecord(event: MemoryRatingEvent): RatingEventRecord {
    return {
      ratingEventId: event.ratingEventId,
      organizationId: event.organizationId,
      playerId: event.playerId,
      ladderId: event.ladderId,
      matchId: event.matchId,
      appliedAt: event.appliedAt.toISOString(),
      ratingSystem: 'OPENRATING_TRUESKILL_LITE',
      muBefore: event.muBefore,
      muAfter: event.muAfter,
      delta: event.delta,
      sigmaBefore: event.sigmaBefore,
      sigmaAfter: event.sigmaAfter,
      winProbPre: event.winProbPre,
      movWeight: event.movWeight,
      metadata: null,
    } satisfies RatingEventRecord;
  }

  private applyInactivity(asOf: Date) {
    for (const player of this.players.values()) {
      for (const rating of player.ratings.values()) {
        const weeks = Math.max(0, (asOf.getTime() - rating.updatedAt.getTime()) / MS_PER_WEEK);
        if (weeks <= 0) continue;
        const factor = Math.pow(1 + P.idle.ratePerWeek, weeks);
        let nextVar = rating.sigma * rating.sigma * factor;
        nextVar = Math.min(P.sigmaMax * P.sigmaMax, nextVar);
        rating.sigma = Math.max(P.sigmaMin, Math.sqrt(nextVar));
        rating.updatedAt = asOf;
      }
    }
  }

  private applySynergyDecay(asOf: Date) {
    for (const record of this.pairSynergies.values()) {
      const weeks = Math.max(0, (asOf.getTime() - record.updatedAt.getTime()) / MS_PER_WEEK);
      if (weeks <= 0) continue;
      const decayFactor = Math.pow(Math.max(0, 1 - P.synergy.decayRatePerWeek), weeks);
      let gamma = clampValue(record.gamma * decayFactor, P.synergy.gammaMin, P.synergy.gammaMax);
      gamma = clampValue(gamma - gamma * P.synergy.regularization, P.synergy.gammaMin, P.synergy.gammaMax);
      record.gamma = gamma;
      record.updatedAt = asOf;
    }
  }

  private applyRegionBias(asOf: Date) {
    const muValues: number[] = [];
    const regionStats = new Map<string, { sum: number; count: number }>();

    for (const player of this.players.values()) {
      const regionId = player.regionId ?? DEFAULT_REGION;
      for (const rating of player.ratings.values()) {
        muValues.push(rating.mu);
        if (regionId === DEFAULT_REGION) continue;
        const stat = regionStats.get(regionId) ?? { sum: 0, count: 0 };
        stat.sum += rating.mu;
        stat.count += 1;
        regionStats.set(regionId, stat);
      }
    }

    if (!muValues.length) return;
    const globalMean = muValues.reduce((acc, val) => acc + val, 0) / muValues.length;

    const adjustments = new Map<string, number>();
    for (const [regionId, stat] of regionStats.entries()) {
      if (!stat.count) continue;
      const mean = stat.sum / stat.count;
      const shift = clampValue(mean - globalMean, -P.region.maxShiftPerDay, P.region.maxShiftPerDay);
      if (Math.abs(shift) < 1e-6) continue;
      adjustments.set(regionId, shift);
    }

    if (!adjustments.size) return;

    for (const player of this.players.values()) {
      const shift = adjustments.get(player.regionId ?? DEFAULT_REGION);
      if (shift === undefined) continue;
      for (const rating of player.ratings.values()) {
        rating.mu -= shift;
        rating.updatedAt = asOf;
      }
    }
  }

  private applyGraphSmoothing(asOf: Date, horizonDays: number) {
    const cutoff = new Date(asOf.getTime() - horizonDays * 24 * 60 * 60 * 1000);
    const lambda = P.graph.smoothingLambda;
    if (lambda <= 0) return;

    const ratingMap = new Map<string, MemoryRatingRecord>();
    for (const player of this.players.values()) {
      for (const [ladderId, rating] of player.ratings.entries()) {
        ratingMap.set(`${ladderId}|${player.playerId}`, rating);
      }
    }

    if (!ratingMap.size) return;

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

    for (const match of this.matches) {
      if (match.startTime < cutoff) continue;
      const ladderId = match.ladderId;
      const sideA = match.match.sides.A.players.map((pid) => `${ladderId}|${pid}`);
      const sideB = match.match.sides.B.players.map((pid) => `${ladderId}|${pid}`);

      for (const node of sideA) {
        for (const teammate of sideA) addEdge(node, teammate);
        for (const opponent of sideB) addEdge(node, opponent);
      }

      for (const node of sideB) {
        for (const teammate of sideB) addEdge(node, teammate);
        for (const opponent of sideA) addEdge(node, opponent);
      }
    }

    for (const [node, neighbors] of adjacency.entries()) {
      const rating = ratingMap.get(node);
      if (!rating || rating.sigma > P.sigmaProvisional) continue;
      const neighborMus: number[] = [];
      neighbors.forEach((neighbor) => {
        const state = ratingMap.get(neighbor);
        if (state) neighborMus.push(state.mu);
      });
      if (!neighborMus.length) continue;
      const neighborMean = neighborMus.reduce((acc, val) => acc + val, 0) / neighborMus.length;
      const delta = lambda * (rating.mu - neighborMean);
      rating.mu -= delta;
      rating.updatedAt = asOf;
    }
  }

  private applyDriftControl() {
    const ratings: MemoryRatingRecord[] = [];
    for (const player of this.players.values()) {
      for (const rating of player.ratings.values()) {
        ratings.push(rating);
      }
    }
    if (!ratings.length) return;

    const mean = ratings.reduce((acc, r) => acc + r.mu, 0) / ratings.length;
    const variance = ratings.reduce((acc, r) => acc + (r.mu - mean) ** 2, 0) / ratings.length;
    const std = Math.sqrt(variance);
    const targetMean = P.baseMu;
    const targetStd = P.drift.targetStd;
    const now = new Date();

    for (const rating of ratings) {
      let newMu = rating.mu;
      if (std > 1e-6) {
        newMu = targetMean + (rating.mu - mean) * (targetStd / std);
      } else {
        newMu = targetMean;
      }
      let delta = newMu - rating.mu;
      delta = clampValue(delta, -P.drift.maxDailyDelta, P.drift.maxDailyDelta);
      rating.mu += delta;
      rating.updatedAt = now;
    }
  }

  private assertPlayerInOrganization(playerId: string, organizationId: string) {
    const player = this.players.get(playerId);
    if (!player) {
      throw new PlayerLookupError(`Player not found: ${playerId}`, { missing: [playerId] });
    }
    if (player.organizationId !== organizationId) {
      throw new PlayerLookupError(
        `Player not registered to organization ${organizationId}: ${playerId}`,
        { wrongOrganization: [playerId] }
      );
    }
  }

  private assertOrganizationExists(id: string) {
    if (!this.organizations.has(id)) {
      throw new OrganizationLookupError(`Organization not found: ${id}`);
    }
  }
}
