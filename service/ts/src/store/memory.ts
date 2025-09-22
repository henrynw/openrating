import { randomUUID } from 'crypto';
import { P } from '../engine/params.js';
import type { MatchInput, PlayerState, UpdateResult } from '../engine/types.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
  PlayerListQuery,
  PlayerListResult,
  MatchListQuery,
  MatchListResult,
  MatchSummary,
  MatchGameSummary,
} from './types.js';
import { PlayerLookupError } from './types.js';
import { buildLadderId } from './helpers.js';

interface MemoryPlayerRecord extends PlayerRecord {
  ratings: Map<string, PlayerState>;
}

const DEFAULT_PAGE_SIZE = 50;
const MAX_PAGE_SIZE = 200;

const clampLimit = (limit?: number) => {
  if (!limit || limit < 1) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
};

const buildMatchCursor = (startTime: Date, matchId: string) => `${startTime.toISOString()}|${matchId}`;

const parseMatchCursor = (cursor: string) => {
  const [ts, id] = cursor.split('|');
  if (!ts || !id) return null;
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return null;
  return { startTime: date, matchId: id };
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

  async createPlayer(input: PlayerCreateInput): Promise<PlayerRecord> {
    const playerId = randomUUID();
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

  async ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult> {
    const ladderId = buildLadderId(ladderKey);
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

  async recordMatch(params: RecordMatchParams): Promise<{ matchId: string }> {
    const matchId = randomUUID();
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
    return { matchId };
  }

  async getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null> {
    const ladderId = buildLadderId(ladderKey);
    const player = this.players.get(playerId);
    if (!player) return null;
    return player.ratings.get(ladderId) ?? null;
  }

  async listPlayers(query: PlayerListQuery): Promise<PlayerListResult> {
    const limit = clampLimit(query.limit);
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
}
