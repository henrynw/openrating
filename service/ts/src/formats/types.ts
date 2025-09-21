import type { Discipline, MatchInput, Sport } from '../engine/types.js';

export type NormalizationErrorCode = 'unsupported_format' | 'validation_failed';

export interface MatchSubmission {
  sport: Sport;
  discipline: Discipline;
  format: string;
  tier?: MatchInput['tier'];
  sides: MatchInput['sides'];
  games: MatchInput['games'];
}

export interface NormalizationSuccess {
  ok: true;
  match: MatchInput;
}

export interface NormalizationFailure {
  ok: false;
  error: NormalizationErrorCode;
  message: string;
  issues?: unknown;
}

export type NormalizationResult = NormalizationSuccess | NormalizationFailure;

export interface RegisteredFormat {
  sport: Sport;
  discipline: Discipline;
  format: string;
  normalize(submission: MatchSubmission): NormalizationResult;
}
