import type { LadderKey } from './types';

const NULL_TIER = 'UNSPECIFIED';
const NULL_REGION = 'GLOBAL';

export const normalizeTier = (tier?: string | null) => tier ?? NULL_TIER;
export const normalizeRegion = (region?: string | null) => region ?? NULL_REGION;

export const buildLadderId = (key: LadderKey) =>
  [key.organizationId, key.sport, key.discipline, key.format, key.tier, key.regionId].join(':');

export const DEFAULT_TIER = NULL_TIER;
export const DEFAULT_REGION = NULL_REGION;
