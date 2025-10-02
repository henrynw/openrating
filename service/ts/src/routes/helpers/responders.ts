import type {
  MatchSummary,
  MatchSegment,
  MatchTiming,
  MatchStatistics,
  MatchParticipant,
  OrganizationRecord,
  PlayerRecord,
  PlayerCompetitiveProfile,
  PlayerAttributes,
  PlayerRankingSnapshot,
  RatingEventRecord,
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
    format: match.format,
    tier: match.tier,
    start_time: match.startTime,
    venue_id: match.venueId,
    region_id: match.regionId,
    event_id: match.eventId ?? null,
    sides,
    games: match.games.map(serializeMatchGame),
  };

  if (timing !== undefined) response.timing = timing;
  if (statistics !== undefined) response.statistics = statistics;
  if (segments !== undefined) response.segments = segments;

  return response;
};

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
