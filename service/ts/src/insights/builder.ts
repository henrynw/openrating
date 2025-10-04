import { createHash } from 'crypto';

import type { Discipline, Sport } from '../engine/types.js';
import type {
  PlayerInsightsSnapshot,
  PlayerInsightRatingPoint,
  PlayerInsightDiscipline,
  PlayerInsightMilestone,
  PlayerInsightStreak,
  PlayerInsightFormWindow,
  PlayerInsightsBuildOptions,
} from '../store/contracts/insights.js';

export interface PlayerInsightSourceEvent {
  id: string;
  createdAt: Date;
  sport: Sport;
  discipline: Discipline;
  muBefore: number;
  muAfter: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  delta: number;
  winProbPre: number | null;
  matchId: string | null;
}

export interface PlayerInsightCurrentRating {
  sport: Sport;
  discipline: Discipline;
  mu: number;
  sigma: number;
  matchesCount: number;
  updatedAt: Date;
}

export interface BuildPlayerInsightsParams {
  playerId: string;
  sport?: Sport | null;
  discipline?: Discipline | null;
  events: PlayerInsightSourceEvent[];
  ratings: PlayerInsightCurrentRating[];
  options?: PlayerInsightsBuildOptions;
}

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const EPSILON = 1e-6;

const toWeekStart = (date: Date) => {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay();
  // Align to Monday
  const diff = (day + 6) % 7;
  d.setUTCDate(d.getUTCDate() - diff);
  d.setUTCHours(0, 0, 0, 0);
  return d;
};

const makeScopeKey = (sport?: Sport | null, discipline?: Discipline | null) =>
  `${sport ?? ''}:${discipline ?? ''}`;

const sum = (values: number[]) => values.reduce((acc, value) => acc + value, 0);

const calculateDigest = (snapshot: PlayerInsightsSnapshot) =>
  createHash('sha1').update(JSON.stringify(snapshot)).digest('hex');

const determineVolatilityStatus = (sigma: number | null | undefined) => {
  if (sigma === null || sigma === undefined || Number.isNaN(sigma)) return 'UNKNOWN';
  if (sigma <= 0.9) return 'STABLE';
  if (sigma <= 1.5) return 'ELEVATED';
  return 'HIGH';
};

const buildTrendPoints = (events: PlayerInsightSourceEvent[]): PlayerInsightRatingPoint[] => {
  if (!events.length) return [];
  const buckets = new Map<string, { start: Date; events: PlayerInsightSourceEvent[] }>();

  for (const event of events) {
    const start = toWeekStart(event.createdAt);
    const key = start.toISOString();
    const bucket = buckets.get(key);
    if (bucket) {
      bucket.events.push(event);
    } else {
      buckets.set(key, { start, events: [event] });
    }
  }

  const sorted = Array.from(buckets.values()).sort((a, b) => a.start.getTime() - b.start.getTime());
  const points: PlayerInsightRatingPoint[] = [];
  let previousMu: number | null = null;

  for (const bucket of sorted) {
    const last = bucket.events[bucket.events.length - 1];
    const mu = last.muAfter;
    const sigma = last.sigmaAfter;
    const sampleCount = bucket.events.length;
    const muDelta = previousMu !== null ? mu - previousMu : null;
    points.push({
      periodStart: bucket.start.toISOString(),
      mu,
      sigma,
      muDelta,
      sampleCount,
    });
    previousMu = mu;
  }

  return points;
};

const buildFormSummary = (
  events: PlayerInsightSourceEvent[],
  now: Date
): Record<string, PlayerInsightFormWindow> => {
  const latestEvents = events.slice(-5);
  const since30d = events.filter((event) => now.getTime() - event.createdAt.getTime() <= 30 * DAY_MS);
  const since90d = events.filter((event) => now.getTime() - event.createdAt.getTime() <= 90 * DAY_MS);

  const compute = (list: PlayerInsightSourceEvent[]) => {
    const matches = list.length;
    if (!matches) {
      return {
        matches: 0,
        wins: 0,
        losses: 0,
        draws: 0,
        netDelta: 0,
        avgDelta: null,
        avgOpponentMu: null,
        ratingEvents: 0,
        lastEventAt: null,
      };
    }

    const wins = list.filter((event) => event.delta > EPSILON).length;
    const losses = list.filter((event) => event.delta < -EPSILON).length;
    const draws = matches - wins - losses;
    const netDelta = sum(list.map((event) => event.delta));
    const avgDelta = netDelta / matches;
    const lastEventAt = list[list.length - 1].createdAt.toISOString();

    return {
      matches,
      wins,
      losses,
      draws,
      netDelta,
      avgDelta,
      avgOpponentMu: null,
      ratingEvents: matches,
      lastEventAt,
    };
  };

  return {
    last_5_events: compute(latestEvents),
    last_30d: compute(since30d),
    last_90d: compute(since90d),
  };
};

const buildDisciplineOverview = (
  events: PlayerInsightSourceEvent[],
  ratings: PlayerInsightCurrentRating[]
) => {
  const overview = new Map<string, PlayerInsightDiscipline>();
  const groupedEvents = new Map<string, PlayerInsightSourceEvent[]>();

  for (const event of events) {
    const key = makeScopeKey(event.sport, event.discipline);
    const list = groupedEvents.get(key);
    if (list) {
      list.push(event);
    } else {
      groupedEvents.set(key, [event]);
    }
  }

  for (const [key, list] of groupedEvents.entries()) {
    if (!list.length) continue;
    const { sport, discipline } = list[0];
    const matchesPlayed = list.length;
    const wins = list.filter((event) => event.delta > EPSILON).length;
    const losses = list.filter((event) => event.delta < -EPSILON).length;
    const netDelta = sum(list.map((event) => event.delta));
    const lastEventAt = list[list.length - 1].createdAt.toISOString();

    overview.set(key, {
      sport,
      discipline,
      currentRank: null,
      bestRank: null,
      mu: null,
      sigma: null,
      matchesPlayed,
      wins,
      losses,
      netDelta,
      eventsPlayed: matchesPlayed,
      lastEventAt,
    });
  }

  for (const rating of ratings) {
    const key = makeScopeKey(rating.sport, rating.discipline);
    const existing = overview.get(key);
    const matchesPlayed = existing?.matchesPlayed ?? rating.matchesCount;
    overview.set(key, {
      sport: rating.sport,
      discipline: rating.discipline,
      currentRank: null,
      bestRank: null,
      mu: rating.mu,
      sigma: rating.sigma,
      matchesPlayed,
      wins: existing?.wins ?? 0,
      losses: existing?.losses ?? null,
      netDelta: existing?.netDelta ?? 0,
      eventsPlayed: existing?.eventsPlayed ?? rating.matchesCount,
      lastEventAt: existing?.lastEventAt ?? rating.updatedAt.toISOString(),
    });
  }

  return overview;
};

const buildMilestones = (events: PlayerInsightSourceEvent[]): PlayerInsightMilestone[] => {
  if (!events.length) return [];
  const bestRating = events.reduce((best, current) =>
    current.muAfter > best.muAfter ? current : best
  );
  const biggestGain = events.reduce((best, current) =>
    current.delta > best.delta ? current : best
  );
  const biggestLoss = events.reduce((best, current) =>
    current.delta < best.delta ? current : best
  );

  const milestones: PlayerInsightMilestone[] = [];
  milestones.push({
    type: 'BEST_RATING',
    occurredAt: bestRating.createdAt.toISOString(),
    delta: bestRating.delta,
    label: `Reached rating ${bestRating.muAfter.toFixed(2)}`,
    matchId: bestRating.matchId,
    ratingEventId: bestRating.id,
  });
  if (biggestGain.delta > EPSILON) {
    milestones.push({
      type: 'BIGGEST_GAIN',
      occurredAt: biggestGain.createdAt.toISOString(),
      delta: biggestGain.delta,
      label: 'Largest rating gain',
      matchId: biggestGain.matchId,
      ratingEventId: biggestGain.id,
    });
  }
  if (biggestLoss.delta < -EPSILON) {
    milestones.push({
      type: 'BIGGEST_DROP',
      occurredAt: biggestLoss.createdAt.toISOString(),
      delta: biggestLoss.delta,
      label: 'Largest rating drop',
      matchId: biggestLoss.matchId,
      ratingEventId: biggestLoss.id,
    });
  }
  return milestones;
};

const buildStreaks = (events: PlayerInsightSourceEvent[]): PlayerInsightStreak[] => {
  if (!events.length) return [];
  let bestWinStreak: PlayerInsightStreak | null = null;
  let bestLossStreak: PlayerInsightStreak | null = null;

  let currentWinStart: PlayerInsightSourceEvent | null = null;
  let currentWinCount = 0;
  let currentWinDelta = 0;

  let currentLossStart: PlayerInsightSourceEvent | null = null;
  let currentLossCount = 0;
  let currentLossDelta = 0;

  const finalizeWin = (lastEvent: PlayerInsightSourceEvent | null) => {
    if (currentWinCount === 0 || !currentWinStart || !lastEvent) return;
    const streak: PlayerInsightStreak = {
      type: 'WIN_STREAK',
      startAt: currentWinStart.createdAt.toISOString(),
      endAt: lastEvent.createdAt.toISOString(),
      matches: currentWinCount,
      netDelta: currentWinDelta,
    };
    if (!bestWinStreak || (streak.matches ?? 0) > (bestWinStreak.matches ?? 0)) {
      bestWinStreak = streak;
    }
    currentWinStart = null;
    currentWinCount = 0;
    currentWinDelta = 0;
  };

  const finalizeLoss = (lastEvent: PlayerInsightSourceEvent | null) => {
    if (currentLossCount === 0 || !currentLossStart || !lastEvent) return;
    const streak: PlayerInsightStreak = {
      type: 'LOSS_STREAK',
      startAt: currentLossStart.createdAt.toISOString(),
      endAt: lastEvent.createdAt.toISOString(),
      matches: currentLossCount,
      netDelta: currentLossDelta,
    };
    if (!bestLossStreak || (streak.matches ?? 0) > (bestLossStreak.matches ?? 0)) {
      bestLossStreak = streak;
    }
    currentLossStart = null;
    currentLossCount = 0;
    currentLossDelta = 0;
  };

  for (const event of events) {
    if (event.delta > EPSILON) {
      if (!currentWinStart) {
        currentWinStart = event;
      }
      currentWinCount += 1;
      currentWinDelta += event.delta;
      finalizeLoss(event);
    } else if (event.delta < -EPSILON) {
      if (!currentLossStart) {
        currentLossStart = event;
      }
      currentLossCount += 1;
      currentLossDelta += event.delta;
      finalizeWin(event);
    } else {
      finalizeWin(event);
      finalizeLoss(event);
    }
  }

  finalizeWin(events.at(-1) ?? null);
  finalizeLoss(events.at(-1) ?? null);

  const streaks: PlayerInsightStreak[] = [];
  if (bestWinStreak) streaks.push(bestWinStreak);
  if (bestLossStreak) streaks.push(bestLossStreak);
  return streaks;
};

export const buildPlayerInsightsSnapshot = (
  params: BuildPlayerInsightsParams
): PlayerInsightsSnapshot => {
  const { playerId, sport, discipline, events, ratings, options } = params;
  const schemaVersion = options?.schemaVersion ?? 1;
  const now = options?.now ?? new Date();

  const filteredEvents = events
    .filter((event) => (sport ? event.sport === sport : true))
    .filter((event) => (discipline ? event.discipline === discipline : true))
    .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());

  const filteredRatings = ratings
    .filter((rating) => (sport ? rating.sport === sport : true))
    .filter((rating) => (discipline ? rating.discipline === discipline : true));

  const disciplines = new Set<Discipline>();
  filteredEvents.forEach((event) => disciplines.add(event.discipline));
  filteredRatings.forEach((rating) => disciplines.add(rating.discipline));

  const highestEvent = filteredEvents.reduce<PlayerInsightSourceEvent | null>(
    (best, current) => {
      if (!best) return current;
      return current.muAfter > best.muAfter ? current : best;
    },
    null
  );
  const lowestEvent = filteredEvents.reduce<PlayerInsightSourceEvent | null>(
    (best, current) => {
      if (!best) return current;
      return current.muAfter < best.muAfter ? current : best;
    },
    null
  );

  const ratingTrend = {
    cadence: 'WEEKLY' as const,
    points: buildTrendPoints(filteredEvents),
    lifetimeHigh: highestEvent
      ? {
          mu: highestEvent.muAfter,
          sigma: highestEvent.sigmaAfter,
          occurredAt: highestEvent.createdAt.toISOString(),
        }
      : null,
    lifetimeLow: lowestEvent
      ? {
          mu: lowestEvent.muAfter,
          sigma: lowestEvent.sigmaAfter,
          occurredAt: lowestEvent.createdAt.toISOString(),
        }
      : null,
    latestSnapshotId: null,
  } satisfies PlayerInsightsSnapshot['ratingTrend'];

  const formSummary = buildFormSummary(filteredEvents, now);

  const disciplineOverview = Object.fromEntries(
    buildDisciplineOverview(filteredEvents, filteredRatings).entries()
  );

  const milestones = buildMilestones(filteredEvents);
  const streaks = buildStreaks(filteredEvents);

  const latestRating = filteredRatings
    .slice()
    .sort((a, b) => b.updatedAt.getTime() - a.updatedAt.getTime())
    .at(0);
  const latestEvent = filteredEvents.at(-1);

  const sigmaNow = latestRating?.sigma ?? latestEvent?.sigmaAfter ?? null;
  const thirtyDaysAgo = new Date(now.getTime() - 30 * DAY_MS);
  const sigmaThirtyDaysAgo = filteredEvents
    .filter((event) => event.createdAt <= thirtyDaysAgo)
    .at(-1)?.sigmaAfter ?? null;
  const sigma30dChange =
    sigmaNow !== null && sigmaThirtyDaysAgo !== null
      ? sigmaNow - sigmaThirtyDaysAgo
      : null;

  const lastActivity = latestEvent?.createdAt ?? latestRating?.updatedAt ?? null;
  const inactiveStreakDays = lastActivity
    ? Math.floor((now.getTime() - lastActivity.getTime()) / DAY_MS)
    : null;

  const volatilityStatus = determineVolatilityStatus(sigmaNow);

  const volatility = {
    sigmaNow: sigmaNow ?? 0,
    sigma30dChange,
    inactiveStreakDays,
    volatilityStatus,
  } satisfies PlayerInsightsSnapshot['volatility'];

  const snapshot: PlayerInsightsSnapshot = {
    meta: {
      schemaVersion,
      generatedAt: now.toISOString(),
      playerId,
      sport: sport ?? null,
      disciplines: Array.from(disciplines.values()),
      sampleRange: filteredEvents.length
        ? {
            start: filteredEvents[0].createdAt.toISOString(),
            end: filteredEvents[filteredEvents.length - 1].createdAt.toISOString(),
          }
        : null,
    },
    ratingTrend,
    formSummary,
    disciplineOverview,
    milestones,
    streaks,
    volatility,
  };

  return snapshot;
};

export const enrichSnapshotWithCache = (
  snapshot: PlayerInsightsSnapshot
): { snapshot: PlayerInsightsSnapshot; etag: string; digest: string } => {
  const digest = calculateDigest({ ...snapshot, cacheKeys: undefined });
  const etag = `"${digest}"`;
  const enriched: PlayerInsightsSnapshot = {
    ...snapshot,
    cacheKeys: { etag, digest },
  };
  return { snapshot: enriched, etag, digest };
};
