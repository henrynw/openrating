import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  requireAuth,
  requireScope,
  getSubjectId,
} from '../auth.js';
import type {
  RatingStore,
  CompetitionParticipantRecord,
  EventClassification,
  EventMediaLinks,
} from '../store/index.js';
import { EventLookupError, PlayerLookupError } from '../store/index.js';
import { toCompetitionResponse } from './helpers/competition-serializer.js';

const LooseRecordSchema = z.record(z.unknown()).optional();

const OptionalSportEnum = z
  .enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL'])
  .nullable()
  .optional();

const OptionalDisciplineEnum = z.enum(['SINGLES', 'DOUBLES', 'MIXED']).nullable().optional();

const CompetitionClassificationSchema = z.object({
  level: z.enum(['WORLD_TOUR', 'CONTINENTAL', 'NATIONAL', 'REGIONAL', 'CLUB', 'SCHOOL', 'COMMUNITY', 'OTHER'])
    .nullable()
    .optional(),
  grade: z
    .enum([
      'SUPER_1000',
      'SUPER_750',
      'SUPER_500',
      'SUPER_300',
      'GOLD',
      'SILVER',
      'BRONZE',
      'MAJOR',
      'DIVISION_1',
      'DIVISION_2',
      'OPEN',
      'OTHER',
    ])
    .nullable()
    .optional(),
  age_group: z
    .enum(['U11', 'U13', 'U15', 'U17', 'U19', 'U21', 'SENIOR', 'ADULT', 'VETERAN', 'MASTER', 'OPEN', 'OTHER'])
    .nullable()
    .optional(),
  tour: z.string().nullable().optional(),
  category: z.string().nullable().optional(),
});

const CompetitionMediaLinksSchema = z.object({
  website: z.string().url().nullable().optional(),
  registration: z.string().url().nullable().optional(),
  live_scoring: z.string().url().nullable().optional(),
  streaming: z.string().url().nullable().optional(),
  social: z.record(z.string(), z.string().url()).nullable().optional(),
});

const CompetitionCreateSchema = z.object({
  provider_id: z.string().optional(),
  external_ref: z.string().optional(),
  name: z.string().min(1),
  slug: z.string().optional(),
  sport: OptionalSportEnum,
  discipline: OptionalDisciplineEnum,
  format: z.string().nullable().optional(),
  tier: z.string().nullable().optional(),
  status: z.string().nullable().optional(),
  draw_size: z.number().int().positive().nullable().optional(),
  start_date: z.string().datetime().nullable().optional(),
  end_date: z.string().datetime().nullable().optional(),
  classification: CompetitionClassificationSchema.nullable().optional(),
  purse: z.number().nullable().optional(),
  purse_currency: z.string().nullable().optional(),
  media_links: CompetitionMediaLinksSchema.nullable().optional(),
  metadata: LooseRecordSchema,
});

const CompetitionUpdateSchema = z.object({
  name: z.string().min(1).optional(),
  slug: z.string().optional(),
  sport: OptionalSportEnum,
  discipline: OptionalDisciplineEnum,
  format: z.string().nullable().optional(),
  tier: z.string().nullable().optional(),
  status: z.string().nullable().optional(),
  draw_size: z.number().int().positive().nullable().optional(),
  start_date: z.string().datetime().nullable().optional(),
  end_date: z.string().datetime().nullable().optional(),
  classification: CompetitionClassificationSchema.nullable().optional(),
  purse: z.number().nullable().optional(),
  purse_currency: z.string().nullable().optional(),
  media_links: CompetitionMediaLinksSchema.nullable().optional(),
  metadata: LooseRecordSchema,
});

const CompetitionParticipantSchema = z.object({
  player_id: z.string(),
  seed: z.number().int().nullable().optional(),
  status: z.string().nullable().optional(),
  metadata: LooseRecordSchema,
});

interface CompetitionRouteDeps {
  store: RatingStore;
}

export const registerCompetitionRoutes = (app: Express, deps: CompetitionRouteDeps) => {
  const { store } = deps;

  const requireEvent = async (eventId: string) => {
    const event = await store.getEventById(eventId);
    if (!event) {
      throw new EventLookupError(`Event not found: ${eventId}`);
    }
    return event;
  };

  const requireCompetition = async (competitionId: string) => {
    const competition = await store.getCompetitionById(competitionId);
    if (!competition) {
      throw new EventLookupError(`Competition not found: ${competitionId}`);
    }
    const event = await requireEvent(competition.eventId);
    return { competition, event };
  };

  app.post('/v1/events/:event_id/competitions', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = CompetitionCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const event = await requireEvent(req.params.event_id);

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write'],
        errorCode: 'competitions_create_denied',
        errorMessage: 'Insufficient grants to create competitions',
      });

      const subjectProviderId = await getSubjectId(req);
      if (parsed.data.provider_id && parsed.data.provider_id !== subjectProviderId) {
        return res.status(400).send({
          error: 'provider_mismatch',
          message: 'provider_id must match the authenticated subject',
        });
      }

      const classification = mapCompetitionClassificationInput(parsed.data.classification);
      const mediaLinks = mapCompetitionMediaLinksInput(parsed.data.media_links);

      const competition = await store.createCompetition({
        eventId: event.eventId,
        organizationId: event.organizationId,
        providerId: subjectProviderId,
        externalRef: parsed.data.external_ref ?? null,
        name: parsed.data.name,
        slug: parsed.data.slug ?? null,
        sport: parsed.data.sport ?? null,
        discipline: parsed.data.discipline ?? null,
        format: parsed.data.format ?? null,
        tier: parsed.data.tier ?? null,
        status: parsed.data.status ?? null,
        drawSize: parsed.data.draw_size ?? null,
        startDate: parsed.data.start_date ?? null,
        endDate: parsed.data.end_date ?? null,
        ...(classification !== undefined ? { classification: classification ?? null } : {}),
        ...(parsed.data.purse !== undefined ? { purse: parsed.data.purse ?? null } : {}),
        ...(parsed.data.purse_currency !== undefined
          ? { purseCurrency: parsed.data.purse_currency ?? null }
          : {}),
        ...(mediaLinks !== undefined ? { mediaLinks: mediaLinks ?? null } : {}),
        metadata: parsed.data.metadata ?? null,
      });

      return res.status(201).send({ competition: toCompetitionResponse(competition, event) });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'event_not_found', message: err.message });
      }
      console.error('competition_create_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/events/:event_id/competitions', requireAuth, async (req, res) => {
    try {
      const event = await requireEvent(req.params.event_id);

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'competitions_list_denied',
        errorMessage: 'Insufficient grants to list competitions',
      });

      const result = await store.listCompetitions({ eventId: event.eventId });

      return res.send({
        competitions: result.items.map((competition) => toCompetitionResponse(competition, event)),
      });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'event_not_found', message: err.message });
      }
      console.error('competitions_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/competitions/:competition_id', requireAuth, async (req, res) => {
    try {
      const competition = await store.getCompetitionById(req.params.competition_id);
      if (!competition) {
        return res.status(404).send({ error: 'competition_not_found' });
      }
      const event = await requireEvent(competition.eventId);

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'competitions_get_denied',
        errorMessage: 'Insufficient grants to read competitions',
      });

      return res.send({ competition: toCompetitionResponse(competition, event) });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'event_not_found', message: err.message });
      }
      console.error('competition_get_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.patch('/v1/competitions/:competition_id', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = CompetitionUpdateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const competition = await store.getCompetitionById(req.params.competition_id);
      if (!competition) {
        return res.status(404).send({ error: 'competition_not_found' });
      }
      const event = await requireEvent(competition.eventId);

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write'],
        errorCode: 'competitions_update_denied',
        errorMessage: 'Insufficient grants to update competitions',
      });

      const classification = mapCompetitionClassificationInput(parsed.data.classification);
      const mediaLinks = mapCompetitionMediaLinksInput(parsed.data.media_links);

      const updated = await store.updateCompetition(req.params.competition_id, {
        name: parsed.data.name,
        slug: parsed.data.slug ?? undefined,
        sport: parsed.data.sport ?? undefined,
        discipline: parsed.data.discipline ?? undefined,
        format: parsed.data.format ?? undefined,
        tier: parsed.data.tier ?? undefined,
        status: parsed.data.status ?? undefined,
        drawSize: parsed.data.draw_size ?? undefined,
        startDate: parsed.data.start_date ?? undefined,
        endDate: parsed.data.end_date ?? undefined,
        ...(parsed.data.classification !== undefined
          ? { classification: classification ?? null }
          : {}),
        ...(parsed.data.purse !== undefined ? { purse: parsed.data.purse ?? null } : {}),
        ...(parsed.data.purse_currency !== undefined
          ? { purseCurrency: parsed.data.purse_currency ?? null }
          : {}),
        ...(parsed.data.media_links !== undefined ? { mediaLinks: mediaLinks ?? null } : {}),
        metadata: parsed.data.metadata ?? undefined,
      });

      const nextEvent = parsed.data.slug || parsed.data.name ? await requireEvent(updated.eventId) : event;

      return res.send({ competition: toCompetitionResponse(updated, nextEvent) });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'event_not_found', message: err.message });
      }
      console.error('competition_update_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.post(
    '/v1/competitions/:competition_id/participants',
    requireAuth,
    requireScope('matches:write'),
    async (req, res) => {
      const parsed = CompetitionParticipantSchema.safeParse(req.body);
      if (!parsed.success) {
        return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
      }

      try {
        const { competition, event } = await requireCompetition(req.params.competition_id);

        await authorizeOrgAccess(req, event.organizationId, {
          permissions: ['matches:write'],
          errorCode: 'competitions_participant_denied',
          errorMessage: 'Insufficient grants to modify competition participants',
        });

        const participant = await store.upsertCompetitionParticipant({
          competitionId: competition.competitionId,
          playerId: parsed.data.player_id,
          seed: parsed.data.seed ?? null,
          status: parsed.data.status ?? null,
          metadata: parsed.data.metadata ?? null,
        });

        return res.status(201).send({ participant: toCompetitionParticipantResponse(participant) });
      } catch (err) {
        if (err instanceof PlayerLookupError) {
          return res.status(400).send({ error: 'invalid_player', message: err.message });
        }
        if (err instanceof AuthorizationError) {
          return res.status(err.status).send({ error: err.code, message: err.message });
        }
        if (err instanceof EventLookupError) {
          return res.status(404).send({ error: 'competition_not_found', message: err.message });
        }
        console.error('competition_participant_upsert_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

  app.get('/v1/competitions/:competition_id/participants', requireAuth, async (req, res) => {
    try {
      const { competition, event } = await requireCompetition(req.params.competition_id);

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'competitions_participant_list_denied',
        errorMessage: 'Insufficient grants to read competition participants',
      });

      const participants = await store.listCompetitionParticipants(competition.competitionId);

      return res.send({
        competition: toCompetitionResponse(competition, event),
        participants: participants.items.map(toCompetitionParticipantResponse),
      });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'competition_not_found', message: err.message });
      }
      console.error('competition_participant_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
type CompetitionClassificationInput = z.infer<typeof CompetitionClassificationSchema>;
type CompetitionMediaLinksInput = z.infer<typeof CompetitionMediaLinksSchema>;

const mapCompetitionClassificationInput = (
  input: CompetitionClassificationInput | null | undefined
): EventClassification | null | undefined => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  return {
    level: input.level ?? null,
    grade: input.grade ?? null,
    ageGroup: input.age_group ?? null,
    tour: input.tour ?? null,
    category: input.category ?? null,
  };
};

const mapCompetitionMediaLinksInput = (
  input: CompetitionMediaLinksInput | null | undefined
): EventMediaLinks | null | undefined => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  return {
    website: input.website ?? null,
    registration: input.registration ?? null,
    liveScoring: input.live_scoring ?? null,
    streaming: input.streaming ?? null,
    social: input.social ?? null,
  };
};

const toCompetitionParticipantResponse = (participant: CompetitionParticipantRecord) => ({
  competition_id: participant.competitionId,
  player_id: participant.playerId,
  seed: participant.seed ?? null,
  status: participant.status ?? null,
  metadata: participant.metadata ?? null,
  created_at: participant.createdAt ?? null,
  updated_at: participant.updatedAt ?? null,
});
