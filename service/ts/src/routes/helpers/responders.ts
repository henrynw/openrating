import type {
  MatchSummary,
  MatchSegment,
  MatchTiming,
  MatchStatistics,
  MatchParticipant,
  MatchSportTotal,
  OrganizationRecord,
  PlayerRecord,
  PlayerCompetitiveProfile,
  PlayerAttributes,
  PlayerRankingSnapshot,
  RatingEventRecord,
  PlayerInsightsSnapshot,
  PlayerInsightAiData,
} from '../../store/index.js';

export const toOrganizationResponse = (
  organization: OrganizationRecord,
  options: { includeCreatedAt?: boolean } = {}
) => {
  const response: Record<string, unknown> = {
    organization_id: organization.organizationId,
    name: organization.name,
    slug: organization.slug,
    description: organization.description ?? null,
  };

  if (options.includeCreatedAt) {
    response.created_at = organization.createdAt ?? null;
  }

  return response;
};

const serializeRankingSnapshot = (snapshot: PlayerRankingSnapshot) => ({
  source: snapshot.source,
  discipline: snapshot.discipline ?? null,
  position: snapshot.position ?? null,
  points: snapshot.points ?? null,
  as_of: snapshot.asOf ?? null,
  metadata: snapshot.metadata ?? null,
});

const serializeCompetitiveProfile = (profile?: PlayerCompetitiveProfile | null) => {
  if (profile === undefined) return undefined;
  if (profile === null) return null;
  return {
    discipline: profile.discipline ?? null,
    ranking_points: profile.rankingPoints ?? null,
    ranking_position: profile.rankingPosition ?? null,
    total_matches: profile.totalMatches ?? null,
    as_of: profile.asOf ?? null,
    external_rankings: profile.externalRankings
      ? profile.externalRankings.map(serializeRankingSnapshot)
      : null,
  };
};

const serializePlayerAttributes = (attributes?: PlayerAttributes | null) => {
  if (attributes === undefined) return undefined;
  if (attributes === null) return null;
  return {
    handedness: attributes.handedness ?? null,
    dominant_side: attributes.dominantSide ?? null,
    height_cm: attributes.heightCm ?? null,
    weight_kg: attributes.weightKg ?? null,
    birth_date: attributes.birthDate ?? null,
    residence: attributes.residence ?? null,
    metadata: attributes.metadata ?? null,
  };
};

const serializeMatchTiming = (timing?: MatchTiming | null) => {
  if (timing === undefined || timing === null) return undefined;
  return {
    scheduled_start: timing.scheduledStart ?? null,
    actual_start: timing.actualStart ?? null,
    completed_at: timing.completedAt ?? null,
    duration_seconds: timing.durationSeconds ?? null,
    time_zone: timing.timeZone ?? null,
    status: timing.status ?? null,
  };
};

const serializeMatchSegment = (segment: MatchSegment) => ({
  sequence: segment.sequence ?? null,
  phase: segment.phase ?? null,
  label: segment.label ?? null,
  side: segment.side ?? null,
  value: segment.value ?? null,
  unit: segment.unit ?? null,
  elapsed_seconds: segment.elapsedSeconds ?? null,
  timestamp: segment.timestamp ?? null,
  metadata: segment.metadata ?? null,
});

const serializeMatchSegments = (segments?: MatchSegment[] | null) => {
  if (segments === undefined || segments === null) return undefined;
  return segments.map(serializeMatchSegment);
};

const serializeMatchParticipant = (participant: MatchParticipant) => ({
  player_id: participant.playerId,
  role: participant.role ?? null,
  seed: participant.seed ?? null,
  status: participant.status ?? null,
  external_ref: participant.externalRef ?? null,
  metadata: participant.metadata ?? null,
});

const serializeMatchParticipants = (participants?: MatchParticipant[] | null) => {
  if (participants === undefined || participants === null) return undefined;
  return participants.map(serializeMatchParticipant);
};

const serializeMatchStatistics = (statistics?: MatchStatistics) => {
  if (statistics === undefined || statistics === null) return undefined;
  return statistics;
};

const serializeMatchGame = (game: MatchSummary['games'][number]) => {
  const segments = serializeMatchSegments(game.segments);
  const statistics = serializeMatchStatistics(game.statistics);
  return {
    game_no: game.gameNo,
    a: game.a,
    b: game.b,
    ...(segments !== undefined ? { segments } : {}),
    ...(statistics !== undefined ? { statistics } : {}),
  };
};

const serializeMatchFormat = (match: MatchSummary) => {
  if (!match.format) return null;
  return {
    family: match.sport,
    code: match.format,
    name: null,
  };
};

const serializeMatchStage = (stage: MatchSummary['stage']) => {
  if (stage === undefined) return undefined;
  if (stage === null) return null;
  return {
    type: stage.type,
    value: stage.value ?? null,
    label: stage.label ?? null,
  };
};

export const toPlayerResponse = (
  player: PlayerRecord,
  organizationSlug: string | null,
  options: { forceNullDefaults?: boolean } = {}
) => {
  const maybe = <T>(value: T | undefined | null) =>
    options.forceNullDefaults ? (value ?? null) : value ?? undefined;

  return {
    player_id: player.playerId,
    organization_id: player.organizationId,
    organization_slug: organizationSlug,
    display_name: player.displayName,
    short_name: maybe(player.shortName),
    native_name: maybe(player.nativeName),
    given_name: maybe(player.givenName),
    family_name: maybe(player.familyName),
    sex: maybe(player.sex),
    birth_year: maybe(player.birthYear),
    country_code: maybe(player.countryCode),
    region_id: maybe(player.regionId),
    external_ref: maybe(player.externalRef),
    competitive_profile: maybe(serializeCompetitiveProfile(player.competitiveProfile)),
    attributes: maybe(serializePlayerAttributes(player.attributes)),
    profile_photo_id: maybe(player.profilePhotoId),
    profile_photo_url: maybe(player.profilePhotoUrl),
    profile_photo_uploaded_at: maybe(player.profilePhotoUploadedAt),
  };
};

export const toMatchSummaryResponse = (match: MatchSummary, organizationSlug: string | null) => {
  const timing = serializeMatchTiming(match.timing);
  const statistics = serializeMatchStatistics(match.statistics);
  const segments = serializeMatchSegments(match.segments);
  const sides = match.sides.reduce((acc, side) => {
    const participants = serializeMatchParticipants(side.participants);
    acc[side.side] = {
      players: side.players,
      ...(participants !== undefined ? { participants } : {}),
    };
    return acc;
  }, {} as Record<'A' | 'B', { players: string[]; participants?: ReturnType<typeof serializeMatchParticipants> }>);

  const response: Record<string, unknown> = {
    match_id: match.matchId,
    provider_id: match.providerId,
    external_ref: match.externalRef ?? null,
    organization_id: match.organizationId,
    organization_slug: organizationSlug,
    sport: match.sport,
    discipline: match.discipline,
    format: serializeMatchFormat(match),
    tier: match.tier,
    start_time: match.startTime,
    venue_id: match.venueId,
    region_id: match.regionId,
    event_id: match.eventId ?? null,
    competition_id: match.competitionId ?? null,
    competition_slug: match.competitionSlug ?? null,
    sides,
    games: match.games.map(serializeMatchGame),
    rating_status: match.ratingStatus,
    rating_skip_reason: match.ratingSkipReason ?? null,
    winner: match.winnerSide ?? null,
  };

  if (timing !== undefined) response.timing = timing;
  if (statistics !== undefined) response.statistics = statistics;
  if (segments !== undefined) response.segments = segments;
  const stage = serializeMatchStage(match.stage);
  if (stage !== undefined) response.stage = stage;
  if (match.ratingEvents !== undefined) {
    response.rating_events = match.ratingEvents
      ? match.ratingEvents.map(toRatingEventResponse)
      : null;
  }

  return response;
};

export const toMatchSportTotals = (totals: MatchSportTotal[]) =>
  totals.map((item) => ({ sport: item.sport, total_matches: item.totalMatches }));

export const toRatingEventResponse = (event: RatingEventRecord) => ({
  rating_event_id: event.ratingEventId,
  organization_id: event.organizationId,
  player_id: event.playerId,
  match_id: event.matchId,
  applied_at: event.appliedAt,
  rating_system: event.ratingSystem ?? null,
  mu_before: event.muBefore,
  mu_after: event.muAfter,
  delta: event.delta,
  sigma_before: event.sigmaBefore ?? null,
  sigma_after: event.sigmaAfter,
  win_probability_pre: event.winProbPre ?? null,
  metadata: event.metadata ?? null,
});

const serializeFormSummary = (summary: PlayerInsightsSnapshot['formSummary']) =>
  Object.fromEntries(
    Object.entries(summary).map(([key, window]) => [
      key,
      {
        matches: window.matches,
        wins: window.wins,
        losses: window.losses,
        draws: window.draws ?? null,
        net_delta: window.netDelta,
        avg_delta: window.avgDelta ?? null,
        avg_opponent_mu: window.avgOpponentMu ?? null,
        rating_events: window.ratingEvents ?? null,
        last_event_at: window.lastEventAt ?? null,
      },
    ])
  );

const serializeDisciplineOverview = (
  overview: PlayerInsightsSnapshot['disciplineOverview']
) =>
  Object.fromEntries(
    Object.entries(overview).map(([key, discipline]) => [
      key,
      {
        sport: discipline.sport ?? null,
        discipline: discipline.discipline ?? null,
        current_rank: discipline.currentRank ?? null,
        best_rank: discipline.bestRank ?? null,
        mu: discipline.mu ?? null,
        sigma: discipline.sigma ?? null,
        matches_played: discipline.matchesPlayed,
        wins: discipline.wins,
        losses: discipline.losses ?? null,
        net_delta: discipline.netDelta,
        events_played: discipline.eventsPlayed ?? null,
        last_event_at: discipline.lastEventAt ?? null,
      },
    ])
  );

const serializeTrendPoints = (snapshot: PlayerInsightsSnapshot) =>
  snapshot.ratingTrend.points.map((point) => ({
    period_start: point.periodStart,
    mu: point.mu,
    sigma: point.sigma,
    mu_delta: point.muDelta ?? null,
    sample_count: point.sampleCount ?? null,
  }));

const serializeAiSummary = (ai: PlayerInsightAiData | null | undefined, jobId?: string | null) => {
  if (!ai) return null;
  return {
    status: ai.status.toLowerCase(),
    snapshot_digest: ai.snapshotDigest,
    prompt_version: ai.promptVersion,
    narrative: ai.narrative ?? null,
    model: ai.model ?? null,
    generated_at: ai.generatedAt ?? null,
    last_requested_at: ai.lastRequestedAt ?? null,
    expires_at: ai.expiresAt ?? null,
    poll_after_ms: ai.pollAfterMs ?? null,
    tokens: ai.tokens
      ? {
          prompt: ai.tokens.prompt,
          completion: ai.tokens.completion,
          total: ai.tokens.total,
        }
      : null,
    job_id: jobId ?? null,
    error: ai.errorCode
      ? {
          code: ai.errorCode,
          message: ai.errorMessage ?? null,
        }
      : null,
  };
};

export const toPlayerInsightsResponse = (
  snapshot: PlayerInsightsSnapshot,
  options?: { ai?: PlayerInsightAiData | null; aiJobId?: string | null }
) => {
  const { meta } = snapshot;
  const ratingTrend = snapshot.ratingTrend;
  const formSummary = serializeFormSummary(snapshot.formSummary);
  const disciplineOverview = serializeDisciplineOverview(snapshot.disciplineOverview);
  const milestones = snapshot.milestones?.map((milestone) => ({
    type: milestone.type,
    occurred_at: milestone.occurredAt,
    delta: milestone.delta ?? null,
    label: milestone.label ?? null,
    match_id: milestone.matchId ?? null,
    rating_event_id: milestone.ratingEventId ?? null,
  })) ?? null;
  const streaks = snapshot.streaks?.map((streak) => ({
    type: streak.type,
    start_at: streak.startAt,
    end_at: streak.endAt,
    matches: streak.matches ?? null,
    net_delta: streak.netDelta ?? null,
  })) ?? null;

  const lifetimeHigh = ratingTrend.lifetimeHigh
    ? {
        mu: ratingTrend.lifetimeHigh.mu,
        sigma: ratingTrend.lifetimeHigh.sigma,
        occurred_at: ratingTrend.lifetimeHigh.occurredAt,
      }
    : null;
  const lifetimeLow = ratingTrend.lifetimeLow
    ? {
        mu: ratingTrend.lifetimeLow.mu,
        sigma: ratingTrend.lifetimeLow.sigma,
        occurred_at: ratingTrend.lifetimeLow.occurredAt,
      }
    : null;

  return {
    meta: {
      schema_version: meta.schemaVersion,
      generated_at: meta.generatedAt,
      player_id: meta.playerId,
      sport: meta.sport ?? null,
      disciplines: meta.disciplines,
      sample_range: meta.sampleRange
        ? {
            start: meta.sampleRange.start ?? null,
            end: meta.sampleRange.end ?? null,
          }
        : null,
    },
    rating_trend: {
      cadence: ratingTrend.cadence,
      points: serializeTrendPoints(snapshot),
      lifetime_high: lifetimeHigh,
      lifetime_low: lifetimeLow,
      latest_snapshot_id: ratingTrend.latestSnapshotId ?? null,
    },
    form_summary: formSummary,
    discipline_overview: disciplineOverview,
    milestones,
    streaks,
    volatility: {
      sigma_now: snapshot.volatility.sigmaNow,
      sigma_30d_change: snapshot.volatility.sigma30dChange ?? null,
      inactive_streak_days: snapshot.volatility.inactiveStreakDays ?? null,
      volatility_status: snapshot.volatility.volatilityStatus ?? null,
    },
    cache_keys: snapshot.cacheKeys
      ? {
          etag: snapshot.cacheKeys.etag ?? null,
          digest: snapshot.cacheKeys.digest ?? null,
        }
      : null,
    ai: serializeAiSummary(options?.ai ?? null, options?.aiJobId ?? null),
  };
};
