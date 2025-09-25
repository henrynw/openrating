import type { MatchInput, PlayerState, UpdateResult } from '../../engine/types.js';

export interface LadderKey {
  organizationId: string;
  sport: MatchInput['sport'];
  discipline: MatchInput['discipline'];
  format: MatchInput['format'];
  tier: string;
  regionId: string;
}

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

export interface RecordMatchResult {
  matchId: string;
  ratingEvents: Array<{ playerId: string; ratingEventId: string; appliedAt: string }>;
}
