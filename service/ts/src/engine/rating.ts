import { P } from './params.js';
import { movWeight } from './mov.js';
import type { MatchInput, PlayerState, UpdateResult } from './types.js';

// Numerical approximation (Abramowitz & Stegun 7.1.26) for environments lacking Math.erf.
const erf = (x:number) => {
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * ax);
  const poly = (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t);
  const y = 1 - poly * Math.exp(-ax * ax);
  return sign * y;
};
const phi = (z:number) => 0.5 * (1 + erf(z / Math.SQRT2));
const clamp = (x:number, a:number, b:number) => Math.max(a, Math.min(b, x));
const winnerIsA = (games:{a:number;b:number}[]) => {
  const wa = games.filter(g => g.a > g.b).length;
  return wa > (games.length - wa);
};
const tierToWeight = (t?:string) => t==='SANCTIONED'?1.2 : t==='SOCIAL'?0.8 : t==='EXHIBITION'?0.6 : 1.0;
const avgSigma2 = (xs:PlayerState[]) => xs.reduce((s,p)=>s+p.sigma*p.sigma,0)/Math.max(1,xs.length);

export function expectedWinProb(Ra:number, Rb:number, beta=200) {
  return phi((Ra - Rb) / (Math.SQRT2 * beta));
}

function mismatchMultiplier(pA:number, y:number) {
  // If A is heavy favorite and wins → tiny gain; if A loses → bigger loss.
  if (pA > P.mismatchSoftcapProb) return y===1 ? P.winnerGainMultWhenHeavyFavorite : P.loserLossMultWhenHeavyFavorite;
  if (pA < 1 - P.mismatchSoftcapProb) return y===1 ? P.loserLossMultWhenHeavyFavorite : P.winnerGainMultWhenHeavyFavorite;
  return 1;
}

export function updateMatch(
  match: MatchInput,
  getPlayer: (id:string)=>PlayerState
): UpdateResult {
  const Aids = match.sides.A.players, Bids = match.sides.B.players;
  const Aplayers = Aids.map(getPlayer);
  const Bplayers = Bids.map(getPlayer);

  // Team ratings: sum of mus (synergy omitted in this minimal version)
  const Ra = Aplayers.reduce((s,p)=>s+p.mu,0);
  const Rb = Bplayers.reduce((s,p)=>s+p.mu,0);

  const pA = expectedWinProb(Ra, Rb, P.beta);
  const winner = match.winner ?? (match.games.length ? (winnerIsA(match.games) ? 'A' : 'B') : 'A');
  const y  = winner === 'A' ? 1 : 0;
  const surprise = y - pA;

  const w  = match.movWeight ?? movWeight(match.games, P.movWeight.min, P.movWeight.max, P.movCapPerGame);
  const wt = w * tierToWeight(match.tier);

  // K scaled by uncertainty
  const u = Math.sqrt((avgSigma2(Aplayers) + avgSigma2(Bplayers)) / (2*P.sigmaRef*P.sigmaRef));
  let K = clamp(P.K0 * u, P.KBounds.min, P.KBounds.max);

  // Rookie boost (more movement for new players)
  const rookieFactor = (ps:PlayerState[]) =>
    Math.max(...ps.map(p => p.matchesCount < P.rookieBoostMatches ? P.rookieKMultiplier : 1));
  K *= Math.max(rookieFactor(Aplayers), rookieFactor(Bplayers));

  // Asymmetric multiplier for huge mismatches
  const mult = mismatchMultiplier(pA, y);

  const deltaTeam = mult * K * surprise * wt;

  // Remember before-values
  const before = [...Aplayers, ...Bplayers].map(p => ({ id: p.playerId, mu: p.mu }));

  // Apply deltas equally to teammates
  for (const p of Aplayers) p.mu += deltaTeam / Aplayers.length;
  for (const p of Bplayers) p.mu -= deltaTeam / Bplayers.length;

  // Update sigma (shrink with evidence; bounded)
  const shrink = (p:PlayerState)=>{
    const alpha = P.alpha0 * Math.abs(surprise) * wt;
    const s2 = p.sigma*p.sigma*(1-alpha) + alpha*(P.sigmaMin*P.sigmaMin);
    p.sigma = clamp(Math.sqrt(s2), P.sigmaMin, P.sigmaMax);
    p.matchesCount += 1;
  };
  [...Aplayers, ...Bplayers].forEach(shrink);

  return {
    perPlayer: before.map(b => {
      const p = [...Aplayers, ...Bplayers].find(x=>x.playerId===b.id)!;
      return {
        playerId: p.playerId,
        muBefore: b.mu,
        muAfter: p.mu,
        delta: +(p.mu - b.mu).toFixed(2),
        sigmaAfter: +p.sigma.toFixed(1),
        winProbPre: +pA.toFixed(2),
      };
    })
  };
}
