import type { MatchInput } from '../../engine/types.js';

export interface MatchParticipant {
  playerId: string;
  role?: string | null;
  seed?: number | null;
  status?: string | null;
  externalRef?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface MatchTiming {
  scheduledStart?: string | null;
  actualStart?: string | null;
  completedAt?: string | null;
  durationSeconds?: number | null;
  timeZone?: string | null;
  status?: string | null;
}

export interface MatchSegment {
  sequence?: number | null;
  phase?: string | null;
  label?: string | null;
  side?: string | null;
  value?: number | null;
  unit?: string | null;
  elapsedSeconds?: number | null;
  timestamp?: string | null;
  metadata?: Record<string, unknown> | null;
}

export type MatchMetricValue = number | Record<string, unknown>;
export type MatchStatistics = Record<string, MatchMetricValue> | null;

export interface MatchSideSummary {
  side: 'A' | 'B';
  players: string[];
  participants?: MatchParticipant[] | null;
}

export interface MatchGameSummary {
  gameNo: number;
  a: number;
  b: number;
  segments?: MatchSegment[] | null;
  statistics?: MatchStatistics;
}

export interface MatchSummary {
  matchId: string;
  organizationId: string;
  organizationSlug?: string | null;
  sport: MatchInput['sport'];
  discipline: MatchInput['discipline'];
  format: string;
  tier?: string;
  eventId?: string | null;
  startTime: string;
  venueId?: string | null;
  regionId?: string | null;
  timing?: MatchTiming | null;
  statistics?: MatchStatistics;
  segments?: MatchSegment[] | null;
  sides: MatchSideSummary[];
  games: MatchGameSummary[];
}

export interface MatchListQuery {
  organizationId: string;
  sport?: string;
  playerId?: string;
  eventId?: string;
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
  eventId?: string | null;
  timing?: MatchTiming | null;
  statistics?: MatchStatistics;
  segments?: MatchSegment[] | null;
}
