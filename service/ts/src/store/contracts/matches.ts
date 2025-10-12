import type { MatchInput, WinnerSide } from '../../engine/types.js';
import type { RatingEventRecord } from './ratings.js';

export type MatchRatingStatus = 'RATED' | 'UNRATED';

export type MatchRatingSkipReason = 'MISSING_SCORES' | 'UNKNOWN';

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

export type MatchStageType =
  | 'ROUND_OF'
  | 'GROUP'
  | 'QUARTERFINAL'
  | 'SEMIFINAL'
  | 'FINAL'
  | 'PLAYOFF'
  | 'OTHER';

export interface MatchStage {
  type: MatchStageType;
  value?: number | null;
  label?: string | null;
}

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
  stage?: MatchStage | null;
  eventId?: string | null;
  competitionId?: string | null;
  competitionSlug?: string | null;
  startTime: string;
  venueId?: string | null;
  regionId?: string | null;
  timing?: MatchTiming | null;
  statistics?: MatchStatistics;
  segments?: MatchSegment[] | null;
  sides: MatchSideSummary[];
  games: MatchGameSummary[];
  ratingStatus: MatchRatingStatus;
  ratingSkipReason?: MatchRatingSkipReason | null;
  winnerSide?: WinnerSide | null;
  ratingEvents?: RatingEventRecord[] | null;
}

export interface MatchListQuery {
  organizationId: string;
  sport?: string;
  playerId?: string;
  eventId?: string;
  competitionId?: string;
  cursor?: string;
  limit?: number;
  startAfter?: string;
  startBefore?: string;
  includeRatingEvents?: boolean;
}

export interface MatchListResult {
  items: MatchSummary[];
  nextCursor?: string;
}

export interface MatchSportTotal {
  sport: MatchInput['sport'];
  totalMatches: number;
}

export interface MatchSportTotalsQuery {
  organizationId: string;
  sport?: MatchInput['sport'];
  discipline?: MatchInput['discipline'];
  startAfter?: string;
  startBefore?: string;
  playerId?: string;
  eventId?: string;
  competitionId?: string;
}

export interface MatchSportTotalsResult {
  totals: MatchSportTotal[];
}

export interface MatchUpdateInput {
  startTime?: string;
  venueId?: string | null;
  regionId?: string | null;
  eventId?: string | null;
  competitionId?: string | null;
  stage?: MatchStage | null;
  timing?: MatchTiming | null;
  statistics?: MatchStatistics;
  segments?: MatchSegment[] | null;
}
