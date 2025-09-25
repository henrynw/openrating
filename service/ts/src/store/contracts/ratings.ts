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
  cursor?: string;
  limit?: number;
}

export interface RatingEventListResult {
  items: RatingEventRecord[];
  nextCursor?: string;
}

export interface RatingSnapshot {
  organizationId: string;
  playerId: string;
  ladderId: string;
  asOf: string;
  mu: number;
  sigma: number;
  ratingEvent?: RatingEventRecord | null;
}
