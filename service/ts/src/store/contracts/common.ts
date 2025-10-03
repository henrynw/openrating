import type { MatchInput, PairState, PairUpdate, PlayerState, UpdateResult } from '../../engine/types.js';
import type {
  MatchParticipant,
  MatchSegment,
  MatchStatistics,
  MatchTiming,
} from './matches.js';

export interface LadderKey {
  organizationId: string;
  sport: MatchInput['sport'];
  discipline: MatchInput['discipline'];
  tier: string;
  regionId: string;
}

export interface EnsurePlayersResult {
  ladderId: string;
  players: Map<string, PlayerState>;
}

export interface EnsurePairSynergiesParams {
  ladderId: string;
  ladderKey: LadderKey;
  pairs: Array<{ pairId: string; players: string[] }>;
}

export type EnsurePairSynergiesResult = Map<string, PairState>;

export interface RecordMatchParams {
  ladderId: string;
  ladderKey: LadderKey;
  match: MatchInput;
  result: UpdateResult;
  eventId?: string | null;
  competitionId?: string | null;
  playerStates: Map<string, PlayerState>;
  pairUpdates: PairUpdate[];
  timing?: MatchTiming | null;
  statistics?: MatchStatistics;
  segments?: MatchSegment[] | null;
  sideParticipants?: Record<'A' | 'B', MatchParticipant[] | null | undefined> | null;
  gameDetails?: Array<{
    gameNo: number;
    segments?: MatchSegment[] | null;
    statistics?: MatchStatistics;
  }>;
  submissionMeta: {
    providerId: string;
    externalRef?: string | null;
    organizationId: string;
    startTime: string;
    rawPayload: unknown;
    venueId?: string | null;
    regionId?: string | null;
  };
}

export interface RecordMatchResult {
  matchId: string;
  ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }>;
}

export interface NightlyStabilizationOptions {
  asOf?: Date;
  horizonDays?: number;
}
