import type { LadderKey } from './common.js';

export interface RatingEventRecord {
  ratingEventId: string;
  organizationId: string;
  playerId: string;
  ladderId: string;
  matchId: string | null;
  appliedAt: string;
  ratingSystem?: string | null;
  muBefore: number;
  muAfter: number;
  delta: number;
  sigmaBefore?: number | null;
  sigmaAfter: number;
  winProbPre?: number | null;
  movWeight?: number | null;
  metadata?: Record<string, unknown> | null;
}

export interface RatingEventListQuery {
  playerId: string;
  ladderKey: LadderKey;
  matchId?: string;
  since?: string;
  until?: string;
  organizationId?: string;
  scope?: string | null;
  cursor?: string;
  limit?: number;
}

export interface RatingEventListResult {
  items: RatingEventRecord[];
  nextCursor?: string;
}

export interface RatingSnapshot {
  sport: LadderKey['sport'];
  discipline: LadderKey['discipline'];
  scope?: string | null;
  organizationId?: string | null;
  playerId: string;
  ladderId: string;
  asOf: string;
  mu: number;
  muRaw?: number;
  sigma: number;
  ratingEvent?: RatingEventRecord | null;
}

export interface LeaderboardPlayerInfo {
  playerId: string;
  displayName: string;
  shortName?: string;
  givenName?: string;
  familyName?: string;
  countryCode?: string;
  regionId?: string;
}

export interface LeaderboardEntry extends LeaderboardPlayerInfo {
  rank: number;
  mu: number;
  muRaw?: number;
  sigma: number;
  matches: number;
  delta?: number | null;
  lastEventAt?: string | null;
  lastMatchId?: string | null;
}

export interface LeaderboardQuery {
  sport: LadderKey['sport'];
  discipline: LadderKey['discipline'];
  scope?: string | null;
  organizationId?: string | null;
  segment?: string | null;
  classCodes?: string[] | null;
  sex?: 'M' | 'F' | 'X' | null;
  countryCode?: string | null;
  regionId?: string | null;
  ageGroup?: string | null;
  ageFrom?: number | null;
  ageTo?: number | null;
  ageCutoff?: string | null;
  limit?: number;
  cursor?: string;
}

export interface LeaderboardResult {
  items: LeaderboardEntry[];
  nextCursor?: string;
  totalCount: number;
  pageSize: number;
}

export interface LeaderboardMoverEntry extends LeaderboardPlayerInfo {
  mu: number;
  muRaw?: number;
  sigma: number;
  matches: number;
  change: number;
  events: number;
  lastEventAt?: string | null;
}

export interface LeaderboardMoversQuery extends LeaderboardQuery {
  since: string;
}

export interface LeaderboardMoversResult {
  items: LeaderboardMoverEntry[];
}

export interface PlayerRatingsSummaryQuery {
  sport: LadderKey['sport'];
  discipline: LadderKey['discipline'];
  scope?: string | null;
  organizationId?: string | null;
  segment?: string | null;
  classCodes?: string[] | null;
  playerIds: string[];
}

export interface PlayerRatingsSummaryItem extends LeaderboardPlayerInfo {
  mu: number;
  muRaw?: number;
  sigma: number;
  matches: number;
  delta?: number | null;
  lastEventAt?: string | null;
  lastMatchId?: string | null;
  rank?: number | null;
}

export interface PlayerRatingsSummaryResult {
  items: PlayerRatingsSummaryItem[];
}
