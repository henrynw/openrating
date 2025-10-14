import type { LadderKey, CompetitionSegment } from '../../store/index.js';
import { normalizeClassCodes } from '../../store/helpers.js';

interface BuildLadderKeyParams {
  sport?: string;
  discipline?: string;
  segment?: CompetitionSegment | null;
  classCodes?: string[] | null;
}

export const buildLadderKey = (params: BuildLadderKeyParams): LadderKey => {
  const key: LadderKey = {
    sport: (params.sport ?? 'BADMINTON') as LadderKey['sport'],
    discipline: (params.discipline ?? 'SINGLES') as LadderKey['discipline'],
  };

  const segment = params.segment ?? null;
  if (segment) {
    key.segment = segment;
  }

  const codes = normalizeClassCodes(params.classCodes ?? null);
  if (codes.length > 0) {
    key.classCodes = codes;
  }

  return key;
};
