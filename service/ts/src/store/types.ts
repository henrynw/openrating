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

export interface PlayerUpdateInput {
  displayName?: string;
  shortName?: string | null;
  nativeName?: string | null;
  externalRef?: string | null;
  givenName?: string | null;
  familyName?: string | null;
  sex?: 'M' | 'F' | 'X' | null;
  birthYear?: number | null;
  countryCode?: string | null;
  regionId?: string | null;
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
  organizationSlug?: string | null;
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

export interface MatchUpdateInput {
  startTime?: string;
  venueId?: string | null;
  regionId?: string | null;
}

export interface OrganizationCreateInput {
  name: string;
  slug?: string;
  description?: string;
}

export interface OrganizationUpdateInput {
  name?: string;
  slug?: string;
  description?: string | null;
}

export interface OrganizationRecord {
  organizationId: string;
  name: string;
  slug: string;
  description?: string | null;
  createdAt?: string;
}

export interface OrganizationListQuery {
  cursor?: string;
  limit?: number;
  q?: string;
}

export interface OrganizationListResult {
  items: OrganizationRecord[];
  nextCursor?: string;
}

export type OrganizationIdentifier = { id?: string; slug?: string };

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
  updatePlayer(playerId: string, organizationId: string, input: PlayerUpdateInput): Promise<PlayerRecord>;
  ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult>;
  recordMatch(params: RecordMatchParams): Promise<{ matchId: string }>;
  updateMatch(matchId: string, organizationId: string, input: MatchUpdateInput): Promise<MatchSummary>;
  getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null>;
  listPlayers(query: PlayerListQuery): Promise<PlayerListResult>;
  listMatches(query: MatchListQuery): Promise<MatchListResult>;
  createOrganization(input: OrganizationCreateInput): Promise<OrganizationRecord>;
  updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord>;
  listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult>;
  getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null>;
  getOrganizationById(id: string): Promise<OrganizationRecord | null>;
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

export class OrganizationLookupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OrganizationLookupError';
  }
}

export class MatchLookupError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MatchLookupError';
  }
}
