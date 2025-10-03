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
  limit?: number;
}

export interface LeaderboardResult {
  items: LeaderboardEntry[];
}

export interface LeaderboardMoverEntry extends LeaderboardPlayerInfo {
  mu: number;
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
