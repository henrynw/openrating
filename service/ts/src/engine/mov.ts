import type { MatchInput } from './types.js';
import { P } from './params.js';

export function movWeight(match: MatchInput) {
  const profile = match.sport === 'BADMINTON' || match.sport === 'SQUASH'
    ? { type: 'rally' as const, ...P.mov.rally }
    : { type: 'set' as const, ...P.mov.set };

  if (profile.type === 'rally') {
    const spreads = match.games.map((g) => Math.min(profile.capPerGame, Math.abs(g.a - g.b)));
    const mean = spreads.reduce((s, x) => s + x, 0) / Math.max(1, spreads.length);
    const scaled = Math.min(8, mean);
    return profile.min + (profile.max - profile.min) * (scaled / 8);
  }

  // set-based sports: use set differential capped at capPerSet (e.g., 3)
  const setDiff = Math.abs(
    match.games.reduce((acc, g) => acc + (g.a > g.b ? 1 : g.a < g.b ? -1 : 0), 0)
  );
  const normalized = Math.min(profile.capPerSet, setDiff) / profile.capPerSet;
  return profile.min + (profile.max - profile.min) * normalized;
}
