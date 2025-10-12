import type { LadderKey } from './types.js';
import { InvalidLeaderboardFilterError } from './errors.js';

export interface AgeBandDefinition {
  minAge?: number;
  maxAge?: number;
}

export interface LadderAgePolicy {
  cutoff?: string | null;
  groups?: Record<string, AgeBandDefinition> | null;
}

export interface AgeFilterInput {
  ageGroup?: string | null;
  ageFrom?: number | null;
  ageTo?: number | null;
  ageCutoff?: string | null;
}

export interface ResolvedAgeFilter {
  minBirthDate?: Date;
  maxBirthDate?: Date;
  minBirthYear?: number;
  maxBirthYear?: number;
}

const MS_PER_DAY = 86_400_000;

const shiftYears = (date: Date, years: number) => {
  const result = new Date(Date.UTC(date.getUTCFullYear() + years, date.getUTCMonth(), date.getUTCDate()));
  return result;
};

const shiftDays = (date: Date, days: number) => new Date(date.getTime() + days * MS_PER_DAY);

const toDateOnly = (isoDate: string): Date => {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(isoDate.trim());
  if (!match) {
    throw new InvalidLeaderboardFilterError(`Invalid age_cutoff format: ${isoDate}`, 'invalid_age_cutoff');
  }
  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    Number.isNaN(date.getTime()) ||
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new InvalidLeaderboardFilterError(`Invalid age_cutoff value: ${isoDate}`, 'invalid_age_cutoff');
  }
  return date;
};

const defaultCutoffForNow = (now: Date) => new Date(Date.UTC(now.getUTCFullYear(), 11, 31));

const DEFAULT_GROUPS: Record<string, AgeBandDefinition> = {
  U23: { maxAge: 22 },
  U21: { maxAge: 20 },
  U20: { maxAge: 19 },
  U19: { maxAge: 18 },
  U18: { maxAge: 17 },
  U17: { maxAge: 16 },
  U16: { maxAge: 15 },
  U15: { maxAge: 14 },
  U14: { maxAge: 13 },
  U13: { maxAge: 12 },
  U12: { maxAge: 11 },
  OPEN: {},
};

const normalizeGroups = (policy: LadderAgePolicy | null | undefined) => {
  if (!policy?.groups) return DEFAULT_GROUPS;
  return { ...DEFAULT_GROUPS, ...policy.groups };
};

const requireValidRange = (minAge?: number | null, maxAge?: number | null) => {
  if (minAge != null && minAge < 0) {
    throw new InvalidLeaderboardFilterError('age_from must be >= 0', 'invalid_age_range');
  }
  if (maxAge != null && maxAge < 0) {
    throw new InvalidLeaderboardFilterError('age_to must be >= 0', 'invalid_age_range');
  }
  if (minAge != null && maxAge != null && minAge > maxAge) {
    throw new InvalidLeaderboardFilterError('age_from cannot be greater than age_to', 'invalid_age_range');
  }
};

const computeBounds = (
  cutoff: Date,
  range: { minAge?: number | null; maxAge?: number | null }
): ResolvedAgeFilter => {
  const minAge = range.minAge ?? null;
  const maxAge = range.maxAge ?? null;
  requireValidRange(minAge, maxAge);

  const bounds: ResolvedAgeFilter = {};

  if (maxAge != null) {
    const start = shiftDays(shiftYears(cutoff, -(maxAge + 1)), 1);
    bounds.minBirthDate = start;
    bounds.minBirthYear = start.getUTCFullYear();
  }

  if (minAge != null) {
    const end = shiftYears(cutoff, -minAge);
    bounds.maxBirthDate = end;
    bounds.maxBirthYear = end.getUTCFullYear();
  }

  return bounds;
};

export const resolveAgePolicy = (
  _ladderKey: LadderKey,
  policyFromStore?: LadderAgePolicy | null,
  now: Date = new Date()
): { cutoff: Date; groups: Record<string, AgeBandDefinition> } => {
  const groups = normalizeGroups(policyFromStore);
  const cutoffSource = policyFromStore?.cutoff;
  const cutoff = cutoffSource ? toDateOnly(cutoffSource) : defaultCutoffForNow(now);
  return { cutoff, groups };
};

export const resolveAgeFilter = (
  ladderKey: LadderKey,
  policyFromStore: LadderAgePolicy | null | undefined,
  filters: AgeFilterInput,
  now: Date = new Date()
): ResolvedAgeFilter | null => {
  const wantsFiltering =
    filters.ageGroup != null || filters.ageFrom != null || filters.ageTo != null || filters.ageCutoff != null;
  if (!wantsFiltering) {
    return null;
  }

  const policy = resolveAgePolicy(ladderKey, policyFromStore, now);
  const cutoff = filters.ageCutoff ? toDateOnly(filters.ageCutoff) : policy.cutoff;

  if (filters.ageGroup) {
    const normalizedGroup = filters.ageGroup.toUpperCase();
    const group = policy.groups[normalizedGroup];
    if (!group) {
      throw new InvalidLeaderboardFilterError(
        `Unknown age_group: ${filters.ageGroup}`,
        'invalid_age_group'
      );
    }
    return computeBounds(cutoff, group);
  }

  return computeBounds(cutoff, {
    minAge: filters.ageFrom ?? null,
    maxAge: filters.ageTo ?? null,
  });
};
