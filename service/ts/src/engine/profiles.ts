import type { MatchInput } from './types.js';
import { P } from './params.js';

export type SportProfile = {
  beta: number;
  movType: 'rally' | 'set';
};

const DEFAULT_PROFILE: SportProfile = {
  beta: P.beta,
  movType: 'rally',
};

const SPORT_PROFILES: Record<MatchInput['sport'], SportProfile> = {
  BADMINTON: { beta: 200, movType: 'rally' },
  TENNIS: { beta: 230, movType: 'set' },
  SQUASH: { beta: 210, movType: 'rally' },
  PADEL: { beta: 230, movType: 'set' },
  PICKLEBALL: { beta: 220, movType: 'set' },
};

export const getSportProfile = (sport: MatchInput['sport']): SportProfile =>
  SPORT_PROFILES[sport] ?? DEFAULT_PROFILE;
