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
} from '../store/index.js';
import { MatchLookupError, OrganizationLookupError, PlayerLookupError } from '../store/index.js';
import { normalizeMatchSubmission } from '../formats/index.js';
import { normalizeRegion, normalizeTier, isDefaultRegion } from '../store/helpers.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toMatchSummaryResponse } from './helpers/responders.js';
import {
  isMatchMetricRecord,
  normalizeMatchStatistics,
  type LooseMatchStatistics,
} from './helpers/statistics.js';
import { buildPairKey, sortPairPlayers } from '../store/helpers.js';
import type { PairState } from '../engine/types.js';

const LooseRecordSchema = z.record(z.unknown());

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

const MatchSubmitSchema = z
  .object({
    provider_id: z.string().optional(),
    external_ref: z.string().optional(),
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']),
    discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']),
    format: z.string(),
    start_time: z.string(),
    venue_id: z.string().optional(),
    venue_region_id: z.string().optional(),
    tier: z.enum(['SANCTIONED', 'LEAGUE', 'SOCIAL', 'EXHIBITION']).optional(),
    event_id: z.string().uuid().optional(),
    competition_id: z.string().uuid().optional(),
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

const MatchUpdateSchema = z.object({
  organization_id: z.string().uuid().optional(),
  organization_slug: z.string().optional(),
  start_time: z.string().datetime().optional(),
  venue_id: z.string().nullable().optional(),
  venue_region_id: z.string().nullable().optional(),
  event_id: z.string().uuid().nullable().optional(),
  competition_id: z.string().uuid().nullable().optional(),
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

const buildLadderKey = (
  organizationId: string,
  match: MatchInput,
  options: { tier?: string; regionId?: string | null }
): LadderKey => ({
  organizationId,
  sport: match.sport,
  discipline: match.discipline,
  tier: normalizeTier(options.tier),
  regionId: normalizeRegion(options.regionId ?? null),
});

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

    const normalization = normalizeMatchSubmission({
      sport: parsed.data.sport,
      discipline: parsed.data.discipline,
      format: parsed.data.format,
      tier: parsed.data.tier,
      sides: normalizedSides,
      games: normalizedGames,
    });

    if (!normalization.ok) {
      return res.status(400).send({
        error: normalization.error,
        message: normalization.message,
        issues: normalization.issues,
      });
    }

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

        if (competition.sport && competition.sport !== normalization.match.sport) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition sport does not match submission' });
        }
        if (competition.discipline && competition.discipline !== normalization.match.discipline) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition discipline does not match submission' });
        }
        if (competition.format && competition.format !== normalization.match.format) {
          return res.status(400).send({ error: 'invalid_competition', message: 'Competition format does not match submission' });
        }
      }

      const timing = mapMatchTimingInput(parsed.data.timing);
      const statistics = mapMatchStatisticsInput(parsed.data.statistics);
      const segments = mapMatchSegmentsInput(parsed.data.segments);
      const sideParticipants = mapSideParticipantsInput(parsed.data.sides);
      const gameDetails = mapGameDetailsInput(parsed.data.games);

      const ladderKey = buildLadderKey(organization.organizationId, normalization.match, {
        tier: parsed.data.tier,
        regionId: parsed.data.venue_region_id ?? null,
      });

      const uniquePlayerIds = Array.from(
        new Set([
          ...normalization.match.sides.A.players,
          ...normalization.match.sides.B.players,
        ])
      );

      await enforceMatchWrite(req, {
        organizationId: organization.organizationId,
        sport: normalization.match.sport,
        regionId: ladderKey.regionId ?? 'GLOBAL',
      });

      const { ladderId, players } = await store.ensurePlayers(uniquePlayerIds, ladderKey, {
        organizationId: organization.organizationId,
      });

      const pairDescriptors: Array<{ pairId: string; players: string[] }> = [];
      const collectPair = (sidePlayers: string[]) => {
        if (sidePlayers.length < 2) return;
        const sorted = sortPairPlayers(sidePlayers);
        pairDescriptors.push({ pairId: buildPairKey(sorted), players: sorted });
      };

      collectPair(normalization.match.sides.A.players);
      collectPair(normalization.match.sides.B.players);

      let pairStates: Map<string, PairState> = new Map();
      if (pairDescriptors.length) {
        pairStates = await store.ensurePairSynergies({ ladderId, ladderKey, pairs: pairDescriptors });
      }

      const result = updateMatch(normalization.match, {
        getPlayer: (id) => {
          const state = players.get(id);
          if (!state) throw new Error(`missing player state for ${id}`);
          return state;
        },
        getPair: pairDescriptors.length
          ? (sidePlayers) => pairStates.get(buildPairKey(sidePlayers))
          : undefined,
      });

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
        match: normalization.match,
        result,
        eventId,
        competitionId: competition?.competitionId ?? parsed.data.competition_id ?? null,
        playerStates: players,
        ...(timing !== undefined ? { timing } : {}),
        ...(statistics !== undefined ? { statistics } : {}),
        ...(segments !== undefined ? { segments } : {}),
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
        pairUpdates: result.pairUpdates,
      });

      const ratingEventByPlayer = new Map(
        ratingEvents.map((event) => [event.playerId, event.ratingEventId])
      );

      return res.send({
        match_id: matchId,
        organization_id: organization.organizationId,
        organization_slug: organization.slug,
        event_id: eventId,
        competition_id: competition?.competitionId ?? parsed.data.competition_id ?? null,
        ratings: result.perPlayer.map((p) => {
          const ratingEventId = ratingEventByPlayer.get(p.playerId);
          if (!ratingEventId) {
            throw new Error(`missing rating event for player ${p.playerId} in match ${matchId}`);
          }
          return {
            player_id: p.playerId,
            rating_event_id: ratingEventId,
            mu_before: p.muBefore,
            mu_after: p.muAfter,
            delta: p.delta,
            sigma_after: p.sigmaAfter,
            win_probability_pre: p.winProbPre,
          };
        }),
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
    const parsed = MatchListQuerySchema.safeParse(req.query);
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
      });

      return res.send({
        matches: result.items.map((match) => toMatchSummaryResponse(match, organization.slug)),
        next_cursor: result.nextCursor ?? null,
      });
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

  app.get('/v1/matches/:match_id', requireAuth, async (req, res) => {
    const parsed = MatchGetQuerySchema.safeParse(req.query);
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

      const match = await store.getMatch(req.params.match_id, organization.organizationId);
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
