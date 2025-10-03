import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  requireAuth,
  requireScope,
  hasScope,
} from '../auth.js';
import type {
  RatingStore,
  OrganizationRecord,
  EventRecord,
  CompetitionRecord,
} from '../store/index.js';
import { EventLookupError, OrganizationLookupError } from '../store/index.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toCompetitionResponse } from './helpers/competition-serializer.js';

const EventTypeEnum = z.enum([
  'TOURNAMENT',
  'LEAGUE',
  'LADDER',
  'BOX_LEAGUE',
  'CHAMPIONSHIP',
  'SERIES',
  'EXHIBITION',
  'CUSTOM',
]);

const MetadataSchema = z.record(z.string(), z.unknown()).optional();

const EventCreateSchema = z
  .object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    type: EventTypeEnum,
    name: z.string().min(1),
    slug: z.string().optional(),
    description: z.string().nullable().optional(),
    start_date: z.string().datetime().nullable().optional(),
    end_date: z.string().datetime().nullable().optional(),
    sanctioning_body: z.string().nullable().optional(),
    season: z.string().nullable().optional(),
    metadata: MetadataSchema,
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const EventUpdateSchema = z.object({
  type: EventTypeEnum.optional(),
  name: z.string().min(1).optional(),
  slug: z.string().optional(),
  description: z.string().nullable().optional(),
  start_date: z.string().datetime().nullable().optional(),
  end_date: z.string().datetime().nullable().optional(),
  sanctioning_body: z.string().nullable().optional(),
  season: z.string().nullable().optional(),
  metadata: MetadataSchema,
});

const EventListQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    types: z.array(EventTypeEnum).optional(),
    cursor: z.string().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
    q: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const toEventResponse = (
  event: EventRecord,
  organization: OrganizationRecord,
  options: { competitions?: CompetitionRecord[] } = {}
) => {
  const competitions = options.competitions ?? [];

  const response: Record<string, unknown> = {
    event_id: event.eventId,
    organization_id: event.organizationId,
    organization_slug: organization.slug,
    type: event.type,
    name: event.name,
    slug: event.slug,
    description: event.description ?? null,
    start_date: event.startDate ?? null,
    end_date: event.endDate ?? null,
    sanctioning_body: event.sanctioningBody ?? null,
    season: event.season ?? null,
    metadata: event.metadata ?? null,
    created_at: event.createdAt ?? null,
    updated_at: event.updatedAt ?? null,
    competitions: competitions.map((competition) => toCompetitionResponse(competition, event)),
  };

  return response;
};

interface EventRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

export const registerEventRoutes = (app: Express, deps: EventRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.post('/v1/events', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = EventCreateSchema.safeParse(req.body);
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
        errorCode: 'events_create_denied',
        errorMessage: 'Insufficient grants to create events',
      });

      const event = await store.createEvent({
        organizationId: organization.organizationId,
        type: parsed.data.type,
        name: parsed.data.name,
        slug: parsed.data.slug,
        description: parsed.data.description ?? null,
        startDate: parsed.data.start_date ?? null,
        endDate: parsed.data.end_date ?? null,
        ...(parsed.data.sanctioning_body !== undefined
          ? { sanctioningBody: parsed.data.sanctioning_body ?? null }
          : {}),
        ...(parsed.data.season !== undefined ? { season: parsed.data.season ?? null } : {}),
        metadata: parsed.data.metadata ?? null,
      });

      return res.status(201).send({ event: toEventResponse(event, organization, { competitions: [] }) });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof EventLookupError) {
        return res.status(409).send({ error: 'event_conflict', message: err.message });
      }
      console.error('event_create_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/events', requireAuth, async (req, res) => {
    const parsed = EventListQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const { organization_id, organization_slug, types, cursor, limit, q } = parsed.data;

    if (!(
      hasScope(req, 'matches:write') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'ratings:read')
    )) {
      return res.status(403).send({
        error: 'insufficient_scope',
        required: 'matches:read|matches:write|ratings:read',
      });
    }

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'events_list_denied',
        errorMessage: 'Insufficient grants to list events',
      });

      const result = await store.listEvents({
        organizationId: organization.organizationId,
        types: types ?? undefined,
        cursor: cursor ?? undefined,
        limit: limit ?? undefined,
        q: q ?? undefined,
      });

      const competitionsByEvent = new Map<string, CompetitionRecord[]>();
      await Promise.all(
        result.items.map(async (event) => {
          const competitions = await store.listCompetitions({ eventId: event.eventId });
          competitionsByEvent.set(event.eventId, competitions.items);
        })
      );

      return res.send({
        events: result.items.map((event) =>
          toEventResponse(event, organization, {
            competitions: competitionsByEvent.get(event.eventId) ?? [],
          })
        ),
        next_cursor: result.nextCursor ?? null,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('events_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/events/:event_id', requireAuth, async (req, res) => {
    try {
      const event = await store.getEventById(req.params.event_id);
      if (!event) {
        return res.status(404).send({ error: 'event_not_found' });
      }
      const organization = await store.getOrganizationById(event.organizationId);
      if (!organization) {
        return res.status(404).send({ error: 'organization_not_found' });
      }

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'events_get_denied',
        errorMessage: 'Insufficient grants to read events',
      });

      const competitions = await store.listCompetitions({ eventId: event.eventId });

      return res.send({
        event: toEventResponse(event, organization, { competitions: competitions.items }),
      });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('event_get_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.patch('/v1/events/:event_id', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = EventUpdateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const event = await store.getEventById(req.params.event_id);
      if (!event) {
        return res.status(404).send({ error: 'event_not_found' });
      }
      const organization = await store.getOrganizationById(event.organizationId);
      if (!organization) {
        return res.status(404).send({ error: 'organization_not_found' });
      }

      await authorizeOrgAccess(req, event.organizationId, {
        permissions: ['matches:write'],
        errorCode: 'events_update_denied',
        errorMessage: 'Insufficient grants to update events',
      });

      const updated = await store.updateEvent(req.params.event_id, {
        type: parsed.data.type,
        name: parsed.data.name,
        slug: parsed.data.slug,
        description: parsed.data.description ?? null,
        startDate: parsed.data.start_date ?? null,
        endDate: parsed.data.end_date ?? null,
        ...(parsed.data.sanctioning_body !== undefined
          ? { sanctioningBody: parsed.data.sanctioning_body ?? null }
          : {}),
        ...(parsed.data.season !== undefined ? { season: parsed.data.season ?? null } : {}),
        metadata: parsed.data.metadata ?? null,
      });

      const competitions = await store.listCompetitions({ eventId: updated.eventId });

      return res.send({
        event: toEventResponse(updated, organization, { competitions: competitions.items }),
      });
    } catch (err) {
      if (err instanceof EventLookupError) {
        return res.status(404).send({ error: 'event_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('event_update_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

};
