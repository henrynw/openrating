import type { LadderKey } from '../../store/index.js';

export const buildLadderKey = (params: {
  sport?: string;
  discipline?: string;
}): LadderKey => ({
  sport: (params.sport ?? 'BADMINTON') as LadderKey['sport'],
  discipline: (params.discipline ?? 'SINGLES') as LadderKey['discipline'],
});
