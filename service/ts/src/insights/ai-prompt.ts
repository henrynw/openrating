import type { PlayerInsightsSnapshot, PlayerInsightStreak, PlayerInsightMilestone } from '../store/contracts/insights.js';

interface BuildAiPromptParams {
  snapshot: PlayerInsightsSnapshot;
  playerName?: string | null;
}

const pickNotableStreaks = (streaks: PlayerInsightStreak[] | null | undefined, limit = 3) =>
  streaks?.slice(0, limit) ?? null;

const pickNotableMilestones = (milestones: PlayerInsightMilestone[] | null | undefined, limit = 2) =>
  milestones?.slice(0, limit) ?? null;

export const buildAiPrompt = ({ snapshot, playerName }: BuildAiPromptParams) => {
  const trendPointsRaw = snapshot.ratingTrend.points;
  const trendPoints = trendPointsRaw.slice(-6).map((point) => ({
    period_start: point.periodStart,
    mu: Number(point.mu.toFixed(2)),
    mu_delta: point.muDelta === null || point.muDelta === undefined ? null : Number(point.muDelta.toFixed(2)),
    sigma: Number(point.sigma.toFixed(2)),
    sample_count: point.sampleCount ?? null,
  }));

  const firstTrendPoint = trendPointsRaw.at(0) ?? null;
  const latestTrendPoint = trendPointsRaw.at(-1) ?? null;
  const priorTrendPoint = trendPointsRaw.length > 1 ? trendPointsRaw.at(-2) ?? null : null;

  const safeNumber = (value: number | null | undefined, digits = 2) =>
    value === null || value === undefined ? null : Number(value.toFixed(digits));

  const totalRatingChange =
    firstTrendPoint && latestTrendPoint ? safeNumber(latestTrendPoint.mu - firstTrendPoint.mu) : null;

  const recentRatingChange =
    priorTrendPoint && latestTrendPoint ? safeNumber(latestTrendPoint.mu - priorTrendPoint.mu) : null;

  const largestSwing = trendPointsRaw.reduce<
    { value: number; periodStart: string; direction: 'up' | 'down' } | null
  >((current, point) => {
    if (point.muDelta === null || point.muDelta === undefined) return current;
    const magnitude = Math.abs(point.muDelta);
    if (!current || magnitude > current.value) {
      return {
        value: safeNumber(magnitude) ?? 0,
        periodStart: point.periodStart,
        direction: point.muDelta >= 0 ? 'up' : 'down',
      };
    }
    return current;
  }, null);

  const sampleRangeStart = snapshot.meta.sampleRange?.start ?? firstTrendPoint?.periodStart ?? null;
  const sampleRangeEnd = snapshot.meta.sampleRange?.end ?? latestTrendPoint?.periodStart ?? null;

  const totalSamples = trendPointsRaw.reduce<number>((total, point) => {
    if (typeof point.sampleCount === 'number') {
      return total + point.sampleCount;
    }
    return total;
  }, 0);

  const formSummary = snapshot.formSummary;
  const summaryPayload = {
    player: {
      id: snapshot.meta.playerId,
      name: playerName ?? null,
      sport: snapshot.meta.sport ?? null,
      disciplines: snapshot.meta.disciplines,
    },
    rating_trend: {
      cadence: snapshot.ratingTrend.cadence,
      points: trendPoints,
      lifetime_high: snapshot.ratingTrend.lifetimeHigh ?? null,
      lifetime_low: snapshot.ratingTrend.lifetimeLow ?? null,
    },
    current_rating: trendPoints.at(-1) ?? null,
    journey_summary: {
      sample_range: {
        start: sampleRangeStart,
        end: sampleRangeEnd,
      },
      earliest_rating: firstTrendPoint
        ? {
            period_start: firstTrendPoint.periodStart,
            mu: safeNumber(firstTrendPoint.mu),
            sigma: safeNumber(firstTrendPoint.sigma),
          }
        : null,
      latest_rating: latestTrendPoint
        ? {
            period_start: latestTrendPoint.periodStart,
            mu: safeNumber(latestTrendPoint.mu),
            sigma: safeNumber(latestTrendPoint.sigma),
          }
        : null,
      total_change: totalRatingChange,
      recent_change: recentRatingChange,
      largest_swing: largestSwing,
      total_samples: totalSamples || null,
    },
    form_windows: {
      last_5_events: formSummary.last_5_events ?? null,
      last_30d: formSummary.last_30d ?? null,
      last_90d: formSummary.last_90d ?? null,
    },
    discipline_overview: snapshot.disciplineOverview,
    streaks: pickNotableStreaks(snapshot.streaks, 3),
    milestones: pickNotableMilestones(snapshot.milestones, 2),
    volatility: snapshot.volatility,
  };

  const system =
    'You are an analytics assistant that transforms racket-sport rating data into concise, factual insights focused on the player\'s story. Keep responses under 120 words, use two to three plain sentences, stay neutral and data-driven, avoid speculation, and never fabricate numbers.';

  const directive = [
    `Tell the story of ${playerName ?? 'the player'}'s rating journey using only the provided data.`,
    'Open by naming the player (or "This player" if the name is missing) and anchoring their current rating against where it started, including timeframe and total change when available.',
    'Weave in recent momentum, notable streaks or milestones, and comment on volatility or sample depth only when the data makes it meaningful.',
    'Write one player-centric paragraph in two or three sentences, avoid list formatting, and acknowledge limited data instead of guessing.',
  ].join(' ');

  const user = `${directive}\n\nData:\n${JSON.stringify(summaryPayload, null, 2)}`;

  return { system, user };
};
