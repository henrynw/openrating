import type { MatchInput, PlayerState, UpdateResult } from '../engine/types.js';

export interface LadderKey {
  organizationId: string;
  sport: MatchInput['sport'];
  discipline: MatchInput['discipline'];
  format: MatchInput['format'];
  tier: string;
  regionId: string;
}

export interface PlayerCreateInput {
  organizationId: string;
  displayName: string;
  shortName?: string;
  nativeName?: string;
  externalRef?: string;
  givenName?: string;
  familyName?: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
}

export interface PlayerRecord {
  playerId: string;
  organizationId: string;
  displayName: string;
  shortName?: string;
  nativeName?: string;
  givenName?: string;
  familyName?: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
  externalRef?: string;
}

export interface PlayerListQuery {
  organizationId: string;
  cursor?: string;
  limit?: number;
  q?: string;
}

export interface PlayerListResult {
  items: PlayerRecord[];
  nextCursor?: string;
}

export interface MatchSideSummary {
  side: 'A' | 'B';
  players: string[];
}

export interface MatchGameSummary {
  gameNo: number;
  a: number;
  b: number;
}

export interface MatchSummary {
  matchId: string;
  organizationId: string;
  sport: MatchInput['sport'];
  discipline: MatchInput['discipline'];
  format: string;
  tier?: string;
  startTime: string;
  venueId?: string | null;
  regionId?: string | null;
  sides: MatchSideSummary[];
  games: MatchGameSummary[];
}

export interface MatchListQuery {
  organizationId: string;
  sport?: string;
  playerId?: string;
  cursor?: string;
  limit?: number;
  startAfter?: string;
  startBefore?: string;
}

export interface MatchListResult {
  items: MatchSummary[];
  nextCursor?: string;
}

export interface EnsurePlayersResult {
  ladderId: string;
  players: Map<string, PlayerState>;
}

export interface RecordMatchParams {
  ladderId: string;
  ladderKey: LadderKey;
  match: MatchInput;
  result: UpdateResult;
  playerStates: Map<string, PlayerState>;
  submissionMeta: {
    providerId: string;
    organizationId: string;
    startTime: string;
    rawPayload: unknown;
    venueId?: string | null;
    regionId?: string | null;
  };
}

export interface RatingStore {
  createPlayer(input: PlayerCreateInput): Promise<PlayerRecord>;
  ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult>;
  recordMatch(params: RecordMatchParams): Promise<{ matchId: string }>;
  getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null>;
  listPlayers(query: PlayerListQuery): Promise<PlayerListResult>;
  listMatches(query: MatchListQuery): Promise<MatchListResult>;
}

export class PlayerLookupError extends Error {
  constructor(
    message: string,
    public readonly context: {
      missing?: string[];
      wrongOrganization?: string[];
    } = {}
  ) {
    super(message);
    this.name = 'PlayerLookupError';
  }
}
