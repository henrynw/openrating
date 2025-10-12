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

  const disciplineEntries = Object.values(snapshot.disciplineOverview ?? {});
  const aggregateRecord = disciplineEntries.reduce<
    | {
        matches: number;
        wins: number;
        losses: number;
        rating_events: number;
      }
    | null
  >((acc, discipline) => {
    if (!discipline) return acc;

    const matches = typeof discipline.matchesPlayed === 'number' ? discipline.matchesPlayed : 0;
    const wins = typeof discipline.wins === 'number' ? discipline.wins : 0;
    const losses = typeof discipline.losses === 'number' ? discipline.losses : 0;
    const ratingEvents = typeof discipline.eventsPlayed === 'number' ? discipline.eventsPlayed : Math.max(matches, 0);

    if (!acc) {
      return {
        matches,
        wins,
        losses,
        rating_events: ratingEvents,
      };
    }

    return {
      matches: acc.matches + matches,
      wins: acc.wins + wins,
      losses: acc.losses + losses,
      rating_events: acc.rating_events + ratingEvents,
    };
  }, null);
  const recordSummary = aggregateRecord && aggregateRecord.matches > 0 ? aggregateRecord : null;

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
    aggregate_record: recordSummary,
    discipline_overview: snapshot.disciplineOverview,
    streaks: pickNotableStreaks(snapshot.streaks, 3),
    milestones: pickNotableMilestones(snapshot.milestones, 2),
    volatility: snapshot.volatility,
  };

  const system = [
    'You are an analytics writer who turns racket-sport rating data into a neutral player recap.',
    'Work strictly from the provided data, keep the answer under 120 words, and respond with a single paragraph of two or three sentences.',
    'Do not describe your process, include headings, or add bullet lists.',
  ].join(' ');

  const directive = [
    `Write one concise paragraph about ${playerName ?? 'the player'}'s rating history and recent form using only the supplied data.`,
    'Start with the player name (or "This player") plus their current rating, referencing peak rating or net change when available.',
    'Mention match volume or win–loss record if provided, highlight recent rating momentum, streaks, milestones, or volatility when supported by the data, and acknowledge limited samples instead of guessing.',
    'Maintain a neutral, analytical tone, avoid speculation, and output the paragraph only—no lead-in text or explanations.',
    'Example style: "Pin-Chian Chiu currently holds a rating of around 1500, slightly below the mid-2025 peak of 1538. With 22 singles matches and a 7-15 record, the season has been volatile but recent events produced a 35-point upswing. That history shows sharp swings alongside modest net gains, pointing to elevated volatility."',
  ].join(' ');

  const user = `${directive}\n\nData:\n${JSON.stringify(summaryPayload, null, 2)}`;

  return { system, user };
};
