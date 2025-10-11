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
  const trendPoints = snapshot.ratingTrend.points.slice(-6).map((point) => ({
    period_start: point.periodStart,
    mu: Number(point.mu.toFixed(2)),
    mu_delta: point.muDelta === null || point.muDelta === undefined ? null : Number(point.muDelta.toFixed(2)),
    sigma: Number(point.sigma.toFixed(2)),
    sample_count: point.sampleCount ?? null,
  }));

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
    'You are an analytics assistant that transforms racket-sport rating data into concise, factual insights. Keep responses under 120 words, use two to three sentences, stay neutral and data-driven, avoid speculation, and never fabricate numbers.';

  const directive = [
    `Write a short recent-performance insight for ${playerName ?? 'the player'} based strictly on the provided data.`,
    'Mention meaningful rating movement, notable streaks or milestones, and comment on volatility when relevant.',
    'If data is sparse, acknowledge the limited sample rather than guessing.',
  ].join(' ');

  const user = `${directive}\n\nData:\n${JSON.stringify(summaryPayload, null, 2)}`;

  return { system, user };
};
