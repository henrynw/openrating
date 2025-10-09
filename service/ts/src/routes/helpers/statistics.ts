import type { MatchMetric, MatchStatistics } from '../../store/index.js';

export type LooseMatchStatistics = Record<string, unknown>;

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isMatchMetric = (value: unknown): value is MatchMetric =>
  isPlainObject(value) && typeof value.value === 'number' && Number.isFinite(value.value);

const coerceNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
};

const sanitizeKey = (key: string) => {
  const normalized = key
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s./-]+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .toLowerCase()
    .replace(/^_+|_+$/g, '');
  return normalized.length ? normalized : 'statistic';
};

const ensureUniqueKey = (base: string, used: Set<string>) => {
  let key = base;
  let counter = 2;
  while (used.has(key)) {
    key = `${base}_${counter}`;
    counter += 1;
  }
  used.add(key);
  return key;
};

export const isMatchMetricRecord = (
  value: unknown
): value is Record<string, MatchMetric> => {
  if (!isPlainObject(value)) return false;
  const entries = Object.entries(value);
  if (!entries.length) return true;
  return entries.every(([, metric]) => isMatchMetric(metric));
};

export const normalizeMatchStatistics = (
  statistics: LooseMatchStatistics
): MatchStatistics => {
  const metrics: Record<string, MatchMetric> = {};
  const usedKeys = new Set<string>();

  const walk = (
    node: Record<string, unknown>,
    originalPath: string[],
    keyPrefix: string | null
  ) => {
    for (const [rawKey, rawValue] of Object.entries(node)) {
      const sanitizedKey = sanitizeKey(rawKey);
      const compositeKey = keyPrefix ? `${keyPrefix}_${sanitizedKey}` : sanitizedKey;
      const sourcePath = [...originalPath, rawKey];

      if (isMatchMetric(rawValue)) {
        const finalKey = ensureUniqueKey(compositeKey, usedKeys);
        metrics[finalKey] = rawValue;
        continue;
      }

      const numeric = coerceNumber(rawValue);
      if (numeric !== null) {
        const finalKey = ensureUniqueKey(compositeKey, usedKeys);
        metrics[finalKey] = {
          value: numeric,
          metadata: { source_path: sourcePath.join('.') },
        };
        continue;
      }

      if (isPlainObject(rawValue)) {
        walk(rawValue, sourcePath, compositeKey);
      }
    }
  };

  walk(statistics, [], null);

  return Object.keys(metrics).length ? metrics : null;
};
