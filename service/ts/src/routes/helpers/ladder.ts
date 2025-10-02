import type { LadderKey } from '../../store/index.js';
import { normalizeRegion, normalizeTier } from '../../store/helpers.js';

export const buildLadderKeyForOrganization = (
  organizationId: string,
  params: {
    sport?: string;
    discipline?: string;
    tier?: string;
    region_id?: string | null;
  }
): LadderKey => ({
  organizationId,
  sport: (params.sport ?? 'BADMINTON') as LadderKey['sport'],
  discipline: (params.discipline ?? 'SINGLES') as LadderKey['discipline'],
  tier: normalizeTier(params.tier),
  regionId: normalizeRegion(params.region_id ?? null),
});

