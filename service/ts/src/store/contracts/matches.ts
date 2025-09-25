import type { MatchInput } from '../../engine/types.js';

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
