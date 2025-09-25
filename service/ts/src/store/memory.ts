import { randomUUID } from 'crypto';
import { P } from '../engine/params.js';
import type { MatchInput, PlayerState, UpdateResult } from '../engine/types.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerUpdateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
  RecordMatchResult,
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
} from './types.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError } from './types.js';
import { buildLadderId } from './helpers.js';

interface MemoryPlayerRecord extends PlayerRecord {
  ratings: Map<string, PlayerState>;
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
        };
        player.ratings.set(ladderId, rating);
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

  async recordMatch(params: RecordMatchParams): Promise<RecordMatchResult> {
    const matchId = randomUUID();
    this.assertOrganizationExists(params.ladderKey.organizationId);
    const ladderId = buildLadderId(params.ladderKey);
    this.matches.push({
      matchId,
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
    for (const entry of params.result.perPlayer) {
      const appliedAt = new Date();
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
