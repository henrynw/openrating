import type { MatchInput } from '../../engine/types.js';

export type MatchStatus =
  | 'SCHEDULED'
  | 'IN_PROGRESS'
  | 'COMPLETED'
  | 'RETIRED'
  | 'WALKOVER'
  | 'CANCELLED';

export type MatchSegmentSide = 'A' | 'B' | 'HOME' | 'AWAY';

export type MatchParticipantRole = 'STARTER' | 'SUBSTITUTE' | 'RESERVE' | 'LEAD' | 'OTHER';

export type MatchParticipantStatus = 'ACTIVE' | 'STARTER' | 'BENCH' | 'WITHDRAWN' | 'INACTIVE' | 'OTHER';

export interface MatchParticipant {
  playerId: string;
  role?: MatchParticipantRole | null;
  seed?: number | null;
  status?: MatchParticipantStatus | null;
  externalRef?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface MatchTiming {
  scheduledStart?: string | null;
  actualStart?: string | null;
  completedAt?: string | null;
  durationSeconds?: number | null;
  timeZone?: string | null;
  status?: MatchStatus | null;
}

export interface MatchSegment {
  sequence?: number | null;
  phase?: string | null;
  label?: string | null;
  side?: MatchSegmentSide | null;
  value?: number | null;
  unit?: string | null;
  elapsedSeconds?: number | null;
  timestamp?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface MatchMetric {
  value: number;
  unit?: string | null;
  breakdown?: Record<string, number> | null;
  source?: string | null;
  metadata?: Record<string, unknown> | null;
}

export type MatchStatistics = Record<string, MatchMetric> | null;

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
  providerId: string;
  externalRef?: string | null;
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
