import type { LadderKey } from './types.js';

const NULL_TIER = 'UNSPECIFIED';
const NULL_REGION = 'GLOBAL';

export const normalizeTier = (tier?: string | null) => tier ?? NULL_TIER;
export const normalizeRegion = (region?: string | null) => (region && region.trim().length ? region : NULL_REGION);

export const isDefaultRegion = (regionId: string) => regionId === NULL_REGION;

export const normalizeClassCodes = (codes?: string[] | null): string[] => {
  if (!codes) return [];
  const unique = new Set<string>();
  for (const raw of codes) {
    if (!raw) continue;
    const trimmed = raw.trim();
    if (!trimmed) continue;
    unique.add(trimmed.toUpperCase());
  }
  return [...unique].sort((a, b) => a.localeCompare(b));
};

export const buildLadderId = (key: LadderKey) => {
  const parts: string[] = [key.sport, key.discipline];

  const classCodes = normalizeClassCodes(key.classCodes ?? null);
  const segment = key.segment ?? null;

  if (segment && (segment === 'PARA' || classCodes.length > 0)) {
    parts.push(`segment=${segment}`);
  }

  if (classCodes.length > 0) {
    parts.push(`class=${classCodes.join('+')}`);
  }

  return parts.join(':');
};

const sortPair = (players: string[]) => [...players].sort((a, b) => a.localeCompare(b));

export const buildPairKey = (players: string[]) => sortPair(players).join('|');

export const sortPairPlayers = sortPair;

export const DEFAULT_TIER = NULL_TIER;
export const DEFAULT_REGION = NULL_REGION;

export const toDbRegionId = (regionId: string) => (isDefaultRegion(regionId) ? null : regionId);

export const buildInsightScopeKey = (sport?: string | null, discipline?: string | null) =>
  `${sport ?? ''}:${discipline ?? ''}`;
