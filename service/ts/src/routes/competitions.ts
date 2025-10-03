import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  requireAuth,
  requireScope,
} from '../auth.js';
import type { RatingStore } from '../store/index.js';
import { EventLookupError } from '../store/index.js';
import { toCompetitionResponse } from './helpers/competition-serializer.js';

const LooseRecordSchema = z.record(z.unknown()).optional();

const OptionalSportEnum = z
  .enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL'])
  .nullable()
  .optional();

const OptionalDisciplineEnum = z.enum(['SINGLES', 'DOUBLES', 'MIXED']).nullable().optional();

const CompetitionCreateSchema = z.object({
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

      const competition = await store.createCompetition({
        eventId: event.eventId,
        organizationId: event.organizationId,
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
};
