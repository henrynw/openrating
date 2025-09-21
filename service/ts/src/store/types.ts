import type { MatchInput, PlayerState, UpdateResult } from '../engine/types';

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
  externalRef?: string;
  givenName: string;
  familyName: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
}

export interface PlayerRecord {
  playerId: string;
  organizationId: string;
  givenName: string;
  familyName: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
  externalRef?: string;
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
}
