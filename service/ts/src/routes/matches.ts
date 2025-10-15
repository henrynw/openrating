import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  enforceMatchWrite,
  getSubjectId,
  hasScope,
  requireAuth,
  requireScope,
} from '../auth.js';
import { updateMatch } from '../engine/rating.js';
import type { MatchInput } from '../engine/types.js';
import type {
  LadderKey,
  MatchUpdateInput,
  RatingStore,
  OrganizationRecord,
  MatchSegment,
  MatchParticipant,
  MatchStatistics,
  CompetitionRecord,
  EventClassification,
} from '../store/index.js';
import { MatchLookupError, OrganizationLookupError, PlayerLookupError } from '../store/index.js';
import { normalizeMatchSubmission } from '../formats/index.js';
import {
  normalizeRegion,
  normalizeTier,
  isDefaultRegion,
  buildPairKey,
  sortPairPlayers,
  normalizeClassCodes,
} from '../store/helpers.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toMatchSummaryResponse, toMatchSportTotals } from './helpers/responders.js';
import {
  isMatchMetricRecord,
  normalizeMatchStatistics,
  type LooseMatchStatistics,
} from './helpers/statistics.js';
import type { PairState } from '../engine/types.js';
import { buildLadderKey as buildBaseLadderKey } from './helpers/ladder.js';

const LooseRecordSchema = z.record(z.unknown());

// Experimental telemetry payload; structure may evolve as we add richer sport-specific support.
const MatchStatisticsSchema = z.union([LooseRecordSchema, z.null()]);

const MatchSegmentSchema = z.object({
  sequence: z.number().int().min(1).nullable().optional(),
  phase: z.string().nullable().optional(),
  label: z.string().nullable().optional(),
  side: z.enum(['A', 'B', 'HOME', 'AWAY']).nullable().optional(),
  value: z.number().nullable().optional(),
  unit: z.string().nullable().optional(),
  elapsed_seconds: z.number().nullable().optional(),
  timestamp: z.string().datetime().nullable().optional(),
  metadata: LooseRecordSchema.nullable().optional(),
});

const MatchParticipantSchema = z.object({
  player_id: z.string(),
  role: z.enum(['STARTER', 'SUBSTITUTE', 'RESERVE', 'LEAD', 'OTHER']).nullable().optional(),
  seed: z.number().int().min(1).nullable().optional(),
  status: z.enum(['ACTIVE', 'STARTER', 'BENCH', 'WITHDRAWN', 'INACTIVE', 'OTHER']).nullable().optional(),
  external_ref: z.string().nullable().optional(),
  metadata: LooseRecordSchema.nullable().optional(),
});

const MatchTimingSchema = z.object({
  scheduled_start: z.string().datetime().nullable().optional(),
  actual_start: z.string().datetime().nullable().optional(),
  completed_at: z.string().datetime().nullable().optional(),
  duration_seconds: z.number().int().min(0).nullable().optional(),
  time_zone: z
    .string()
    .regex(/^[A-Za-z]+(?:\/[A-Za-z_]+)+$/)
    .nullable()
    .optional(),
  status: z.enum(['SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'RETIRED', 'WALKOVER', 'CANCELLED']).nullable().optional(),
});

const MatchGameSchema = z.object({
  game_no: z.number().int(),
  a: z.number().int(),
  b: z.number().int(),
  segments: z.array(MatchSegmentSchema).nullable().optional(),
  statistics: MatchStatisticsSchema.optional(),
});

const MatchFormatFamilyEnum = z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL', 'OTHER']);

const MatchFormatSchema = z
  .object({
    family: MatchFormatFamilyEnum,
    code: z.string().min(1),
    name: z.string().min(1).nullable().optional(),
  })
  .strict();

const MatchStageSchema = z
  .object({
    type: z.enum(['ROUND_OF', 'GROUP', 'QUARTERFINAL', 'SEMIFINAL', 'FINAL', 'PLAYOFF', 'OTHER']),
    value: z.number().int().min(1).nullable().optional(),
    label: z.string().min(1).nullable().optional(),
  })
  .strict();

const MatchSubmitSchema = z
  .object({
    provider_id: z.string().optional(),
    external_ref: z.string().optional(),
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']),
    discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']),
    format: MatchFormatSchema,
    start_time: z.string(),
    venue_id: z.string().optional(),
    venue_region_id: z.string().optional(),
    tier: z.enum(['SANCTIONED', 'LEAGUE', 'SOCIAL', 'EXHIBITION']).optional(),
    event_id: z.string().uuid().optional(),
    competition_id: z.string().uuid().optional(),
    stage: MatchStageSchema.nullable().optional(),
    timing: MatchTimingSchema.nullable().optional(),
    statistics: MatchStatisticsSchema.optional(),
    segments: z.array(MatchSegmentSchema).nullable().optional(),
    sides: z.object({
      A: z.object({
        players: z.array(z.string()).min(1).max(2),
        participants: z.array(MatchParticipantSchema).nullable().optional(),
      }),
      B: z.object({
        players: z.array(z.string()).min(1).max(2),
        participants: z.array(MatchParticipantSchema).nullable().optional(),
      }),
    }),
    games: z.array(MatchGameSchema),
    winner: z.enum(['A', 'B']).optional(),
    mov_weight: z.number().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const MatchListQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: z.string().optional(),
    player_id: z.string().optional(),
    event_id: z.string().optional(),
    competition_id: z.string().optional(),
    cursor: z.string().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
    start_after: z.string().datetime().optional(),
    start_before: z.string().datetime().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const MatchIncludeEnum = z.enum(['rating_events', 'players', 'events']);
const MatchIncludeParamSchema = z
  .union([MatchIncludeEnum, z.array(MatchIncludeEnum)])
  .optional()
  .transform((value) => {
    if (!value) return [];
    return Array.isArray(value) ? value : [value];
  });

const MatchSportFilterEnum = z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']);
const MatchDisciplineFilterEnum = z.enum(['SINGLES', 'DOUBLES', 'MIXED']);

const MatchSportTotalsQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: MatchSportFilterEnum.optional(),
    discipline: MatchDisciplineFilterEnum.optional(),
    player_id: z.string().optional(),
    event_id: z.string().optional(),
    competition_id: z.string().optional(),
    start_after: z.string().datetime().optional(),
    start_before: z.string().datetime().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  })
  .refine((data) => !data.discipline || data.sport, {
    message: 'sport is required when discipline provided',
    path: ['sport'],
  });

const MatchUpdateSchema = z.object({
  organization_id: z.string().uuid().optional(),
  organization_slug: z.string().optional(),
  start_time: z.string().datetime().optional(),
  venue_id: z.string().nullable().optional(),
  venue_region_id: z.string().nullable().optional(),
  event_id: z.string().uuid().nullable().optional(),
  competition_id: z.string().uuid().nullable().optional(),
  stage: MatchStageSchema.nullable().optional(),
  timing: MatchTimingSchema.nullable().optional(),
  statistics: MatchStatisticsSchema.optional(),
  segments: z.array(MatchSegmentSchema).nullable().optional(),
});

const MatchGetQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

type MatchTimingInput = z.infer<typeof MatchTimingSchema>;
type MatchSegmentInput = z.infer<typeof MatchSegmentSchema>;
type MatchParticipantInput = z.infer<typeof MatchParticipantSchema>;
type MatchStatisticsInput = z.infer<typeof MatchStatisticsSchema>;
type MatchGameInput = z.infer<typeof MatchGameSchema>;
type MatchStageInput = z.infer<typeof MatchStageSchema>;
type MatchFormatInput = z.infer<typeof MatchFormatSchema>;

const mapMatchStageInput = (
  input: MatchStageInput | null | undefined
): MatchUpdateInput['stage'] => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  return {
    type: input.type,
    value: input.value ?? null,
    label: input.label ?? null,
  };
};

const mapMatchTimingInput = (
  input: MatchTimingInput | null | undefined
): MatchUpdateInput['timing'] => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  return {
    scheduledStart: input.scheduled_start ?? null,
    actualStart: input.actual_start ?? null,
    completedAt: input.completed_at ?? null,
    durationSeconds: input.duration_seconds ?? null,
    timeZone: input.time_zone ?? null,
    status: input.status ?? null,
  };
};

const mapMatchSegmentInput = (segment: MatchSegmentInput) => ({
  sequence: segment.sequence ?? null,
  phase: segment.phase ?? null,
  label: segment.label ?? null,
  side: segment.side ?? null,
  value: segment.value ?? null,
  unit: segment.unit ?? null,
  elapsedSeconds: segment.elapsed_seconds ?? null,
  timestamp: segment.timestamp ?? null,
  metadata: segment.metadata ?? null,
});

const mapMatchSegmentsInput = (
  segments: MatchSegmentInput[] | null | undefined
): MatchSegment[] | null | undefined => {
  if (segments === undefined) return undefined;
  if (segments === null) return null;
  return segments.map(mapMatchSegmentInput);
};

const mapMatchParticipantInput = (participant: MatchParticipantInput): MatchParticipant => ({
  playerId: participant.player_id,
  role: participant.role ?? null,
  seed: participant.seed ?? null,
  status: participant.status ?? null,
  externalRef: participant.external_ref ?? null,
  metadata: participant.metadata ?? null,
});

const mapMatchParticipantsInput = (
  participants: MatchParticipantInput[] | null | undefined
): MatchParticipant[] | null | undefined => {
  if (participants === undefined) return undefined;
  if (participants === null) return null;
  return participants.map(mapMatchParticipantInput);
};

const mapMatchStatisticsInput = (
  statistics: MatchStatisticsInput | undefined
): MatchStatistics | undefined => {
  if (statistics === undefined) return undefined;
  if (statistics === null) return null;
  if (isMatchMetricRecord(statistics)) {
    return statistics;
  }
  return normalizeMatchStatistics(statistics as LooseMatchStatistics);
};

const mapSideParticipantsInput = (
  sides: {
    A: { participants?: MatchParticipantInput[] | null };
    B: { participants?: MatchParticipantInput[] | null };
  }
): Record<'A' | 'B', MatchParticipant[] | null | undefined> | undefined => {
  const participantsA = mapMatchParticipantsInput(sides.A.participants);
  const participantsB = mapMatchParticipantsInput(sides.B.participants);
  if (participantsA === undefined && participantsB === undefined) {
    return undefined;
  }
  return {
    A: participantsA,
    B: participantsB,
  };
};

const mapGameDetailsInput = (games: MatchGameInput[]) => {
  const details: Array<{
    gameNo: number;
    segments?: MatchSegment[] | null;
    statistics?: MatchStatistics | null;
  }> = [];

  for (const game of games) {
    const segments = mapMatchSegmentsInput(game.segments ?? undefined);
    const statistics = mapMatchStatisticsInput(game.statistics);
    if (segments !== undefined || statistics !== undefined) {
      details.push({
        gameNo: game.game_no,
        ...(segments !== undefined ? { segments } : {}),
        ...(statistics !== undefined ? { statistics } : {}),
      });
    }
  }

  return details.length ? details : undefined;
};

interface MatchRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

const deriveCompetitionLadderTaxonomy = (classification?: EventClassification | null) => {
  if (!classification) return { segment: null as EventClassification['segment'] | null, classCodes: undefined };

  const segment = classification.segment ?? null;
  const codes: string[] = [];

  const addCode = (value?: string | null) => {
    if (!value) return;
    const trimmed = value.trim();
    if (!trimmed) return;
    const parts = trimmed.split(/[^A-Za-z0-9]+/u).map((part) => part.trim()).filter(Boolean);
    if (parts.length) {
      codes.push(...parts);
    } else {
      codes.push(trimmed);
    }
  };

  classification.classCodes?.forEach(addCode);
  addCode(classification.classCode ?? null);

  const normalizedCodes = normalizeClassCodes(codes);

  return {
    segment,
    classCodes: normalizedCodes.length ? normalizedCodes : undefined,
  };
};

const createMatchLadderKey = (
  organizationId: string,
  match: MatchInput,
  options: { tier?: string; regionId?: string | null; classification?: EventClassification | null }
): LadderKey => {
  const { classification = null, tier, regionId } = options;
  const taxonomy = deriveCompetitionLadderTaxonomy(classification);
  const baseKey = buildBaseLadderKey({
    sport: match.sport,
    discipline: match.discipline,
    segment: taxonomy.segment ?? null,
    classCodes: taxonomy.classCodes ?? null,
  });

  return {
    ...baseKey,
    organizationId,
    tier: normalizeTier(tier),
    regionId: normalizeRegion(regionId ?? null),
  };
};

export const registerMatchRoutes = (app: Express, deps: MatchRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.post('/v1/matches', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = MatchSubmitSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const normalizedSides: MatchInput['sides'] = {
      A: { players: parsed.data.sides.A.players },
      B: { players: parsed.data.sides.B.players },
    };
    const normalizedGames: MatchInput['games'] = parsed.data.games.map((game) => ({
      game_no: game.game_no,
      a: game.a,
      b: game.b,
    }));

    const formatCode = parsed.data.format.code;
    if (parsed.data.format.family !== parsed.data.sport && parsed.data.format.family !== 'OTHER') {
      return res.status(400).send({
        error: 'invalid_format_family',
        message: 'format.family must match the submitted sport or be OTHER',
      });
    }

    const normalization = normalizeMatchSubmission({
      sport: parsed.data.sport,
      discipline: parsed.data.discipline,
      format: formatCode,
      tier: parsed.data.tier,
      sides: normalizedSides,
      games: normalizedGames,
    });

    const sortedGames = [...normalizedGames].sort((a, b) => a.game_no - b.game_no);

    const canRecordWithoutScores =
      !normalization.ok &&
      parsed.data.sport === 'BADMINTON' &&
      sortedGames.length === 0 &&
      Boolean(parsed.data.winner);

    if (!normalization.ok && !canRecordWithoutScores) {
      return res.status(400).send({
        error: normalization.error,
        message: normalization.message,
        issues: normalization.issues,
      });
    }

    const matchInput: MatchInput = normalization.ok
      ? normalization.match
      : {
          sport: parsed.data.sport,
          discipline: parsed.data.discipline,
          format: formatCode,
          tier: parsed.data.tier,
          sides: normalizedSides,
          games: sortedGames,
          winner: parsed.data.winner,
        };

    const ratingStatus: 'RATED' | 'UNRATED' = normalization.ok ? 'RATED' : 'UNRATED';
    const ratingSkipReason: 'MISSING_SCORES' | 'UNKNOWN' | null =
      ratingStatus === 'UNRATED' ? 'MISSING_SCORES' : null;

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      let eventId: string | null = parsed.data.event_id ?? null;
      let competition: CompetitionRecord | null = null;

      if (parsed.data.competition_id) {
        competition = await store.getCompetitionById(parsed.data.competition_id);
        if (!competition) {
          return res.status(404).send({ error: 'competition_not_found' });
        }
        if (competition.organizationId !== organization.organizationId) {
          return res.status(403).send({ error: 'competition_access_denied' });
        }
        if (eventId && competition.eventId !== eventId) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition does not belong to the specified event' });
        }
        eventId = competition.eventId;

        if (competition.sport && competition.sport !== matchInput.sport) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition sport does not match submission' });
        }
        if (competition.discipline && competition.discipline !== matchInput.discipline) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition discipline does not match submission' });
        }
        if (competition.format && competition.format !== matchInput.format) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition format does not match submission' });
        }
      }

      const timing = mapMatchTimingInput(parsed.data.timing);
      const statistics = mapMatchStatisticsInput(parsed.data.statistics);
      const segments = mapMatchSegmentsInput(parsed.data.segments);
      const stage = mapMatchStageInput(parsed.data.stage);
      const sideParticipants = mapSideParticipantsInput(parsed.data.sides);
      const gameDetails = mapGameDetailsInput(parsed.data.games);

      const ladderKey = createMatchLadderKey(organization.organizationId, matchInput, {
        tier: parsed.data.tier,
        regionId: parsed.data.venue_region_id ?? null,
        classification: competition?.classification ?? null,
      });

      const uniquePlayerIds = Array.from(
        new Set([
          ...matchInput.sides.A.players,
          ...matchInput.sides.B.players,
        ])
      );

      await enforceMatchWrite(req, {
        organizationId: organization.organizationId,
        sport: matchInput.sport,
        regionId: ladderKey.regionId ?? 'GLOBAL',
      });

      const { ladderId, players } = await store.ensurePlayers(uniquePlayerIds, ladderKey, {
        organizationId: organization.organizationId,
      });

      let pairStates: Map<string, PairState> = new Map();
      let result: ReturnType<typeof updateMatch> | null = null;

      if (ratingStatus === 'RATED') {
        const pairDescriptors: Array<{ pairId: string; players: string[] }> = [];
        const collectPair = (sidePlayers: string[]) => {
          if (sidePlayers.length < 2) return;
          const sorted = sortPairPlayers(sidePlayers);
          pairDescriptors.push({ pairId: buildPairKey(sorted), players: sorted });
        };

        collectPair(matchInput.sides.A.players);
        collectPair(matchInput.sides.B.players);

        if (pairDescriptors.length) {
          pairStates = await store.ensurePairSynergies({ ladderId, ladderKey, pairs: pairDescriptors });
        }

        result = updateMatch(matchInput, {
          getPlayer: (id) => {
            const state = players.get(id);
            if (!state) throw new Error(`missing player state for ${id}`);
            return state;
          },
          getPair: pairDescriptors.length
            ? (sidePlayers) => pairStates.get(buildPairKey(sidePlayers))
            : undefined,
        });
      }

      const subjectProviderId = await getSubjectId(req);
      if (parsed.data.provider_id && parsed.data.provider_id !== subjectProviderId) {
        return res.status(400).send({
          error: 'provider_mismatch',
          message: 'provider_id must match the authenticated subject',
        });
      }

      const providerId = subjectProviderId;

      const { matchId, ratingEvents } = await store.recordMatch({
        ladderId,
        ladderKey,
        match: matchInput,
        result: result ?? null,
        eventId,
        competitionId: competition?.competitionId ?? parsed.data.competition_id ?? null,
        playerStates: players,
        ...(timing !== undefined ? { timing } : {}),
        ...(statistics !== undefined ? { statistics } : {}),
        ...(segments !== undefined ? { segments } : {}),
        ...(stage !== undefined ? { stage } : {}),
        ...(sideParticipants !== undefined ? { sideParticipants } : {}),
        ...(gameDetails !== undefined ? { gameDetails } : {}),
        submissionMeta: {
          providerId,
          externalRef: parsed.data.external_ref ?? null,
          organizationId: organization.organizationId,
          startTime: parsed.data.start_time,
          rawPayload: req.body,
          venueId: parsed.data.venue_id ?? null,
          regionId: parsed.data.venue_region_id ?? null,
        },
        pairUpdates: ratingStatus === 'RATED' && result ? result.pairUpdates : [],
        ratingStatus,
        ratingSkipReason,
      });

      let ratingsPayload: Array<{
        player_id: string;
        rating_event_id: string | null;
        mu_before: number;
        mu_after: number;
        delta: number;
        sigma_after: number;
        win_probability_pre: number;
      }> = [];

      if (ratingStatus === 'RATED' && result) {
        const ratingEventByPlayer = new Map(
          ratingEvents.map((event) => [event.playerId, event.ratingEventId])
        );

        const missingRatingEvents = result.perPlayer
          .filter((p) => !ratingEventByPlayer.has(p.playerId))
          .map((p) => p.playerId);

        if (missingRatingEvents.length) {
          const fetched = await Promise.all(
            missingRatingEvents.map(async (playerId) => {
              try {
                const { items } = await store.listRatingEvents({
                  ladderKey,
                  playerId,
                  organizationId: organization.organizationId,
                  matchId,
                  limit: 1,
                });
                const event = items.find((item) => item.matchId === matchId);
                return event ? { playerId, ratingEventId: event.ratingEventId } : null;
              } catch (lookupError) {
                console.warn('rating_event_lookup_error', { playerId, matchId, error: lookupError });
                return null;
              }
            })
          );

          for (const entry of fetched) {
            if (entry) {
              ratingEventByPlayer.set(entry.playerId, entry.ratingEventId);
            }
          }
        }

        ratingsPayload = result.perPlayer.map((p) => {
          const ratingEventId = ratingEventByPlayer.get(p.playerId);
          if (!ratingEventId) {
            console.warn('match_rating_event_missing', {
              matchId,
              playerId: p.playerId,
              providerId,
            });
          }
          return {
            player_id: p.playerId,
            rating_event_id: ratingEventId ?? null,
            mu_before: p.muBefore,
            mu_after: p.muAfter,
            delta: p.delta,
            sigma_after: p.sigmaAfter,
            win_probability_pre: p.winProbPre,
          };
        });
      }

      return res.send({
        match_id: matchId,
        organization_id: organization.organizationId,
        organization_slug: organization.slug,
        event_id: eventId,
        competition_id: competition?.competitionId ?? parsed.data.competition_id ?? null,
        rating_status: ratingStatus,
        rating_skip_reason: ratingSkipReason,
        ratings: ratingsPayload,
      });
    } catch (err) {
      if (err instanceof PlayerLookupError) {
        return res.status(400).send({
          error: 'invalid_players',
          message: err.message,
          missing: err.context.missing,
          wrong_organization: err.context.wrongOrganization,
        });
      }
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      console.error('match_update_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/matches', requireAuth, async (req, res) => {
    const includeParsed = MatchIncludeParamSchema.safeParse(req.query.include);
    if (!includeParsed.success) {
      return res
        .status(400)
        .send({ error: 'validation_error', details: includeParsed.error.flatten() });
    }

    const includeValues = includeParsed.data;
    const includeRatingEvents = includeValues.includes('rating_events');
    const includePlayers = includeValues.includes('players');
    const includeEvents = includeValues.includes('events');

    const queryWithoutInclude = { ...req.query } as Record<string, unknown>;
    delete queryWithoutInclude.include;

    const parsed = MatchListQuerySchema.safeParse(queryWithoutInclude);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const {
      organization_id,
      organization_slug,
      sport,
      player_id,
      event_id,
      competition_id,
      cursor,
      limit,
      start_after,
      start_before,
    } = parsed.data;

    if (!(
      hasScope(req, 'matches:write') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'ratings:read')
    )) {
      return res.status(403).send({ error: 'insufficient_scope', required: 'matches:read|matches:write|ratings:read' });
    }

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        sport: sport ?? null,
        errorCode: 'matches_read_denied',
        errorMessage: 'Insufficient grants to read matches',
      });

      const result = await store.listMatches({
        organizationId: organization.organizationId,
        sport: sport ?? undefined,
        playerId: player_id ?? undefined,
        eventId: event_id ?? undefined,
        competitionId: competition_id ?? undefined,
        cursor,
        limit,
        startAfter: start_after ?? undefined,
        startBefore: start_before ?? undefined,
        includeRatingEvents,
        includePlayers,
        includeEvents,
      });

      const payload: Record<string, unknown> = {
        matches: result.items.map((match) => toMatchSummaryResponse(match, organization.slug)),
        next_cursor: result.nextCursor ?? null,
      };

      if (result.included) {
        const included: Record<string, unknown> = {};
        if (result.included.players?.length) {
          included.players = result.included.players.map((player) => ({
            player_id: player.playerId,
            display_name: player.displayName,
            short_name: player.shortName ?? null,
            given_name: player.givenName ?? null,
            family_name: player.familyName ?? null,
            country_code: player.countryCode ?? null,
            region_id: player.regionId ?? null,
          }));
        }
        if (result.included.events?.length) {
          included.events = result.included.events.map((event) => ({
            event_id: event.eventId,
            name: event.name ?? null,
            slug: event.slug ?? null,
            start_date: event.startDate ?? null,
            end_date: event.endDate ?? null,
            classification: event.classification ?? null,
            season: event.season ?? null,
          }));
        }
        if (Object.keys(included).length) {
          payload.included = included;
        }
      }

      return res.send(payload);
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('matches_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/matches/totals/by-sport', requireAuth, async (req, res) => {
    const parsed = MatchSportTotalsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const {
      organization_id,
      organization_slug,
      sport,
      discipline,
      player_id,
      event_id,
      competition_id,
      start_after,
      start_before,
    } = parsed.data;

    if (!(
      hasScope(req, 'matches:write') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'ratings:read')
    )) {
      return res.status(403).send({ error: 'insufficient_scope', required: 'matches:read|matches:write|ratings:read' });
    }

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        sport: sport ?? null,
        errorCode: 'matches_read_denied',
        errorMessage: 'Insufficient grants to read matches',
      });

      const result = await store.countMatchesBySport({
        organizationId: organization.organizationId,
        sport: sport ?? undefined,
        discipline: discipline ?? undefined,
        playerId: player_id ?? undefined,
        eventId: event_id ?? undefined,
        competitionId: competition_id ?? undefined,
        startAfter: start_after ?? undefined,
        startBefore: start_before ?? undefined,
      });

      return res.send({
        organization_id: organization.organizationId,
        organization_slug: organization.slug ?? null,
        sport_totals: toMatchSportTotals(result.totals),
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('match_totals_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/matches/:match_id', requireAuth, async (req, res) => {
    const includeParsed = MatchIncludeParamSchema.safeParse(req.query.include);
    if (!includeParsed.success) {
      return res
        .status(400)
        .send({ error: 'validation_error', details: includeParsed.error.flatten() });
    }

    const includeValues = includeParsed.data;
    const includeRatingEvents = includeValues.includes('rating_events');

    const queryWithoutInclude = { ...req.query } as Record<string, unknown>;
    delete queryWithoutInclude.include;

    const parsed = MatchGetQuerySchema.safeParse(queryWithoutInclude);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'matches_read_denied',
        errorMessage: 'Insufficient grants to read matches',
      });

      const match = await store.getMatch(req.params.match_id, organization.organizationId, {
        includeRatingEvents,
      });
      if (!match) {
        return res.status(404).send({ error: 'match_not_found' });
      }

      return res.send(toMatchSummaryResponse(match, organization.slug));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('match_get_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.patch('/v1/matches/:match_id', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = MatchUpdateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write'],
        errorCode: 'matches_update_denied',
        errorMessage: 'Insufficient grants for matches:write',
      });

      const updateInput: MatchUpdateInput = {};
      if (parsed.data.start_time !== undefined) updateInput.startTime = parsed.data.start_time;
      if (parsed.data.venue_id !== undefined) updateInput.venueId = parsed.data.venue_id;
      if (parsed.data.venue_region_id !== undefined) {
        updateInput.regionId = parsed.data.venue_region_id === null
          ? null
          : normalizeRegion(parsed.data.venue_region_id);
      }
      if (parsed.data.event_id !== undefined) {
        updateInput.eventId = parsed.data.event_id;
      }
      if (parsed.data.competition_id !== undefined) {
        if (parsed.data.competition_id === null) {
          updateInput.competitionId = null;
        } else {
          const competition = await store.getCompetitionById(parsed.data.competition_id);
          if (!competition) {
            return res.status(404).send({ error: 'competition_not_found' });
          }
          if (competition.organizationId !== organization.organizationId) {
            return res.status(403).send({ error: 'competition_access_denied' });
          }
          updateInput.competitionId = competition.competitionId;
          updateInput.eventId = competition.eventId;
        }
      }
      const timingUpdate = mapMatchTimingInput(parsed.data.timing);
      if (timingUpdate !== undefined) {
        updateInput.timing = timingUpdate;
      }
      const statisticsUpdate = mapMatchStatisticsInput(parsed.data.statistics);
      if (statisticsUpdate !== undefined) {
        updateInput.statistics = statisticsUpdate;
      }
      const segmentsUpdate = mapMatchSegmentsInput(parsed.data.segments);
      if (segmentsUpdate !== undefined) {
        updateInput.segments = segmentsUpdate;
      }
      const stageUpdate = mapMatchStageInput(parsed.data.stage);
      if (stageUpdate !== undefined) {
        updateInput.stage = stageUpdate;
      }

      const updated = await store.updateMatch(req.params.match_id, organization.organizationId, updateInput);

      return res.send(toMatchSummaryResponse(updated, organization.slug));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof MatchLookupError) {
        if (err.message.includes('does not belong')) {
          return res.status(403).send({ error: 'matches_update_denied', message: err.message });
        }
        if (err.message.startsWith('Invalid start time')) {
          return res.status(400).send({ error: 'invalid_match_update', message: err.message });
        }
        return res.status(404).send({ error: 'match_not_found', message: err.message });
      }
      console.error('match_modify_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
