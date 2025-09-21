export function movWeight(
  games: { a: number; b: number }[],
  min = 0.7, max = 1.3, capPerGame = 11
) {
  const spreads = games.map(g => Math.min(capPerGame, Math.abs(g.a - g.b)));
  const mean = spreads.reduce((s, x) => s + x, 0) / Math.max(1, spreads.length);
  const scaled = Math.min(8, mean); // normalize into [0, 8]
  return min + (max - min) * (scaled / 8);
}
