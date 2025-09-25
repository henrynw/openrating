import { P } from './params.js';
import { movWeight } from './mov.js';
import { getSportProfile } from './profiles.js';
import type { MatchInput, MatchUpdateContext, PairState, PairUpdate, PlayerState, UpdateResult } from './types.js';

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
const tierToWeight = (tier?: string) => {
  if (!tier) return P.tierWeights.DEFAULT;
  return P.tierWeights[tier] ?? P.tierWeights.DEFAULT;
};
const avgSigma2 = (xs:PlayerState[]) => xs.reduce((s,p)=>s+p.sigma*p.sigma,0)/Math.max(1,xs.length);
const sortPlayers = (players: string[]) => [...players].sort((a, b) => a.localeCompare(b));

export function expectedWinProb(Ra:number, Rb:number, beta=200) {
  return phi((Ra - Rb) / (Math.SQRT2 * beta));
}

const mismatchMultiplier = (pA: number, y: number) => 1 + P.mismatchLambda * (2 * pA - 1) * (1 - 2 * y);

export function updateMatch(
  match: MatchInput,
  context: MatchUpdateContext
): UpdateResult {
  const getPlayer = context.getPlayer;
  const getPair = context.getPair;

  const Aids = match.sides.A.players;
  const Bids = match.sides.B.players;
  const Aplayers = Aids.map(getPlayer);
  const Bplayers = Bids.map(getPlayer);

  const pairAPlayers = Aids.length > 1 ? sortPlayers(Aids) : null;
  const pairBPlayers = Bids.length > 1 ? sortPlayers(Bids) : null;
  const pairA = pairAPlayers && getPair ? getPair(pairAPlayers) : undefined;
  const pairB = pairBPlayers && getPair ? getPair(pairBPlayers) : undefined;

  // Team ratings: sum of player mus plus any active pair synergy bonuses
  const gammaA = pairA?.gamma ?? 0;
  const gammaB = pairB?.gamma ?? 0;
  const Ra = Aplayers.reduce((s,p)=>s+p.mu,0) + gammaA;
  const Rb = Bplayers.reduce((s,p)=>s+p.mu,0) + gammaB;

  const sportProfile = getSportProfile(match.sport);
  const pA = expectedWinProb(Ra, Rb, sportProfile.beta);
  const winner = match.winner ?? (match.games.length ? (winnerIsA(match.games) ? 'A' : 'B') : 'A');
  const y  = winner === 'A' ? 1 : 0;
  const surprise = y - pA;

  const w  = match.movWeight ?? movWeight(match);
  const wt = Math.min(P.multiplierCap, w * tierToWeight(match.tier));

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
  const before = [...Aplayers, ...Bplayers].map(p => ({ id: p.playerId, mu: p.mu, sigma: p.sigma }));

  // Apply deltas equally to teammates
  for (const p of Aplayers) p.mu += deltaTeam / Aplayers.length;
  for (const p of Bplayers) p.mu -= deltaTeam / Bplayers.length;

  const info = 4 * pA * (1 - pA);
  const adjustSigma = (p: PlayerState) => {
    const baseVar = p.sigma * p.sigma;
    let nextVar = baseVar - P.etaDown * info * baseVar;
    if (Math.abs(surprise) > P.surpriseThreshold) {
      nextVar = Math.min(
        P.sigmaMax * P.sigmaMax,
        nextVar + P.etaUp * (Math.abs(surprise) - P.surpriseThreshold) * baseVar
      );
    }
    nextVar = Math.max(P.sigmaMin * P.sigmaMin, nextVar);
    p.sigma = Math.sqrt(nextVar);
    p.matchesCount += 1;
  };

  [...Aplayers, ...Bplayers].forEach(adjustSigma);

  const pairUpdates: PairUpdate[] = [];
  const processPair = (pair: PairState | undefined, players: string[], direction: number) => {
    if (!pair) return;
    const matchesBefore = pair.matches;
    const gammaBefore = pair.gamma;
    pair.matches += 1;
    let gammaAfter = gammaBefore;
    let delta = 0;
    let activated = false;

    if (pair.matches >= P.synergy.activationMatches) {
      activated = true;
      delta = clamp(P.synergy.K0 * surprise * direction, -P.synergy.deltaMax, P.synergy.deltaMax);
      gammaAfter = clamp(gammaBefore + delta, P.synergy.gammaMin, P.synergy.gammaMax);
      pair.gamma = gammaAfter;
    }

    pairUpdates.push({
      pairId: pair.pairId,
      players: [...players],
      gammaBefore,
      gammaAfter,
      delta,
      matchesBefore,
      matchesAfter: pair.matches,
      activated,
    });
  };

  const directionA = winner === 'A' ? 1 : -1;
  const directionB = -directionA;

  if (pairAPlayers) {
    processPair(pairA, pairAPlayers, directionA);
  }
  if (pairBPlayers) {
    processPair(pairB, pairBPlayers, directionB);
  }

  return {
    perPlayer: before.map((b) => {
      const p = [...Aplayers, ...Bplayers].find((x) => x.playerId === b.id)!;
      return {
        playerId: p.playerId,
        muBefore: b.mu,
        muAfter: p.mu,
        delta: +(p.mu - b.mu).toFixed(2),
        sigmaBefore: +b.sigma.toFixed(1),
        sigmaAfter: +p.sigma.toFixed(1),
        winProbPre: +pA.toFixed(2),
      };
    }),
    pairUpdates,
    teamDelta: deltaTeam,
    winProbability: pA,
  };
}
