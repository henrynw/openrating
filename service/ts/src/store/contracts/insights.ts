import type { Discipline, Sport } from '../../engine/types.js';

export type PlayerInsightsCadence = 'DAILY' | 'WEEKLY' | 'MONTHLY';

export interface PlayerInsightMeta {
  schemaVersion: number;
  generatedAt: string;
  playerId: string;
  sport?: Sport | null;
  disciplines: Discipline[];
  sampleRange?: { start?: string | null; end?: string | null } | null;
}

export interface PlayerInsightRatingPoint {
  periodStart: string;
  mu: number;
  sigma: number;
  muDelta?: number | null;
  sampleCount?: number | null;
}

export interface PlayerInsightTrendExtrema {
  mu: number;
  sigma: number;
  occurredAt: string;
}

export interface PlayerInsightRatingTrend {
  cadence: PlayerInsightsCadence;
  points: PlayerInsightRatingPoint[];
  lifetimeHigh?: PlayerInsightTrendExtrema | null;
  lifetimeLow?: PlayerInsightTrendExtrema | null;
  latestSnapshotId?: string | null;
}

export interface PlayerInsightFormWindow {
  matches: number;
  wins: number;
  losses: number;
  draws?: number | null;
  netDelta: number;
  avgDelta?: number | null;
  avgOpponentMu?: number | null;
  ratingEvents?: number | null;
  lastEventAt?: string | null;
}

export interface PlayerInsightDiscipline {
  sport?: Sport | null;
  discipline?: Discipline | null;
  currentRank?: number | null;
  bestRank?: number | null;
  mu?: number | null;
  sigma?: number | null;
  matchesPlayed: number;
  wins: number;
  losses?: number | null;
  netDelta: number;
  eventsPlayed?: number | null;
  lastEventAt?: string | null;
}

export interface PlayerInsightMilestone {
  type: string;
  occurredAt: string;
  delta?: number | null;
  label?: string | null;
  matchId?: string | null;
  ratingEventId?: string | null;
}

export type PlayerInsightStreakType =
  | 'WIN_STREAK'
  | 'LOSS_STREAK'
  | 'RATING_MOMENTUM'
  | 'VOLATILITY_SHIFT';

export interface PlayerInsightStreak {
  type: PlayerInsightStreakType;
  startAt: string;
  endAt: string;
  matches?: number | null;
  netDelta?: number | null;
}

export type PlayerInsightVolatilityStatus = 'STABLE' | 'ELEVATED' | 'HIGH' | 'UNKNOWN' | null;

export interface PlayerInsightVolatility {
  sigmaNow: number;
  sigma30dChange?: number | null;
  inactiveStreakDays?: number | null;
  volatilityStatus?: PlayerInsightVolatilityStatus;
}

export interface PlayerInsightCacheKeys {
  etag?: string | null;
  digest?: string | null;
}

export interface PlayerInsightsSnapshot {
  meta: PlayerInsightMeta;
  ratingTrend: PlayerInsightRatingTrend;
  formSummary: Record<string, PlayerInsightFormWindow>;
  disciplineOverview: Record<string, PlayerInsightDiscipline>;
  milestones?: PlayerInsightMilestone[] | null;
  streaks?: PlayerInsightStreak[] | null;
  volatility: PlayerInsightVolatility;
  cacheKeys?: PlayerInsightCacheKeys | null;
}

export interface PlayerInsightsQuery {
  organizationId: string;
  playerId: string;
  sport?: Sport | null;
  discipline?: Discipline | null;
}

export interface PlayerInsightsEnqueueInput extends PlayerInsightsQuery {
  runAt?: Date | string;
  reason?: string;
  payload?: Record<string, unknown> | null;
  dedupe?: boolean;
}

export interface PlayerInsightsJob {
  jobId: string;
  playerId: string;
  organizationId: string;
  sport?: Sport | null;
  discipline?: Discipline | null;
  runAt: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'FAILED' | 'COMPLETED';
  attempts: number;
  lockedAt?: string | null;
  lockedBy?: string | null;
  payload?: Record<string, unknown> | null;
  lastError?: string | null;
}

export interface PlayerInsightsJobClaimOptions {
  workerId: string;
  visibilityTimeoutSec?: number;
}

export interface PlayerInsightsJobCompletion {
  jobId: string;
  workerId: string;
  success: boolean;
  error?: string;
  rescheduleAt?: Date | string | null;
}

export interface PlayerInsightsUpsertResult {
  snapshot: PlayerInsightsSnapshot;
  etag: string;
  digest?: string | null;
}

export interface PlayerInsightsBuildOptions {
  schemaVersion?: number;
  now?: Date;
}
