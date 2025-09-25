import type {
  MatchSummary,
  OrganizationRecord,
  PlayerRecord,
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
  };
};

export const toMatchSummaryResponse = (match: MatchSummary, organizationSlug: string | null) => ({
  match_id: match.matchId,
  organization_id: match.organizationId,
  organization_slug: organizationSlug,
  sport: match.sport,
  discipline: match.discipline,
  format: match.format,
  tier: match.tier,
  start_time: match.startTime,
  venue_id: match.venueId,
  region_id: match.regionId,
  sides: match.sides.reduce((acc, side) => {
    acc[side.side] = { players: side.players };
    return acc;
  }, {} as Record<'A' | 'B', { players: string[] }>),
  games: match.games.map((game) => ({ game_no: game.gameNo, a: game.a, b: game.b })),
});

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
