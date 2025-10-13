export interface LeaderboardCursor {
  mu: number;
  playerId: string;
  rank: number;
}

const toBase64Url = (value: string): string =>
  Buffer.from(value, 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');

const fromBase64Url = (value: string): string => {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const padding = (4 - (normalized.length % 4)) % 4;
  const padded = normalized.padEnd(normalized.length + padding, '=');
  return Buffer.from(padded, 'base64').toString('utf8');
};

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value);

const isNonEmptyString = (value: unknown): value is string =>
  typeof value === 'string' && value.length > 0;

export const encodeLeaderboardCursor = (payload: LeaderboardCursor): string => toBase64Url(JSON.stringify(payload));

export const decodeLeaderboardCursor = (cursor: string): LeaderboardCursor | null => {
  try {
    const json = fromBase64Url(cursor);
    const parsed = JSON.parse(json) as Partial<LeaderboardCursor>;
    if (!parsed || typeof parsed !== 'object') return null;
    if (!isFiniteNumber(parsed.mu) || !isFiniteNumber(parsed.rank)) return null;
    if (!isNonEmptyString(parsed.playerId)) return null;
    const mu = parsed.mu;
    const rank = Math.max(0, Math.floor(parsed.rank));
    return { mu, playerId: parsed.playerId, rank };
  } catch {
    return null;
  }
};
