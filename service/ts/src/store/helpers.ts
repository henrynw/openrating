import type { LadderKey } from './types.js';

const NULL_TIER = 'UNSPECIFIED';
const NULL_REGION = 'GLOBAL';

export const normalizeTier = (tier?: string | null) => tier ?? NULL_TIER;
export const normalizeRegion = (region?: string | null) => (region && region.trim().length ? region : NULL_REGION);

export const isDefaultRegion = (regionId: string) => regionId === NULL_REGION;

export const buildLadderId = (key: LadderKey) => [key.sport, key.discipline].join(':');

const sortPair = (players: string[]) => [...players].sort((a, b) => a.localeCompare(b));

export const buildPairKey = (players: string[]) => sortPair(players).join('|');

export const sortPairPlayers = sortPair;

export const DEFAULT_TIER = NULL_TIER;
export const DEFAULT_REGION = NULL_REGION;

export const toDbRegionId = (regionId: string) => (isDefaultRegion(regionId) ? null : regionId);
