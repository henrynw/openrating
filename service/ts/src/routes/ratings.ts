import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  hasScope,
  requireAuth,
} from '../auth.js';
import type { LadderKey, RatingStore, OrganizationRecord } from '../store/index.js';
import { OrganizationLookupError, PlayerLookupError } from '../store/index.js';
import { normalizeRegion, normalizeTier, isDefaultRegion } from '../store/helpers.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toRatingEventResponse } from './helpers/responders.js';
import { buildLadderKeyForOrganization } from './helpers/ladder.js';

const RatingQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']).optional(),
    discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).optional(),
    tier: z.string().optional(),
    region_id: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const LadderDimensionsQuerySchema = z.object({
  sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']).optional(),
  discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).optional(),
  tier: z.string().optional(),
  region_id: z.string().optional(),
});

const RatingEventsQuerySchema = LadderDimensionsQuerySchema.extend({
  since: z.string().datetime().optional(),
  until: z.string().datetime().optional(),
  match_id: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(200).optional(),
  cursor: z.string().optional(),
});

const RatingSnapshotQuerySchema = LadderDimensionsQuerySchema.extend({
  as_of: z.string().datetime().optional(),
});

interface RatingRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

export const registerRatingRoutes = (app: Express, deps: RatingRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.get('/v1/ratings/:player_id', async (req, res) => {
    const parsed = RatingQuerySchema.safeParse({
      organization_id: req.query.organization_id,
      sport: req.query.sport,
      discipline: req.query.discipline,
      tier: req.query.tier,
      region_id: req.query.region_id,
    });

    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      const ladderKey = buildLadderKeyForOrganization(organization.organizationId, {
        sport: parsed.data.sport,
        discipline: parsed.data.discipline,
        tier: parsed.data.tier,
        region_id: parsed.data.region_id,
      });

      const rating = await store.getPlayerRating(req.params.player_id, ladderKey);
      if (!rating) {
        return res.status(404).send({ error: 'not_found' });
      }

      return res.send({
        player_id: rating.playerId,
        mu: rating.mu,
        sigma: rating.sigma,
        matches: rating.matchesCount,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      console.error('ratings_lookup_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/organizations/:organization_id/players/:player_id/rating-events', requireAuth, async (req, res) => {
    if (!(
      hasScope(req, 'ratings:read') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'matches:write')
    )) {
      return res.status(403).send({ error: 'insufficient_scope', required: 'ratings:read|matches:read|matches:write' });
    }

    const parsed = RatingEventsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({ organization_id: req.params.organization_id });
      const ladderKey = buildLadderKeyForOrganization(organization.organizationId, parsed.data);

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        sport: ladderKey.sport,
        regionId: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        errorCode: 'ratings_events_denied',
        errorMessage: 'Insufficient grants to read rating events',
      });

      const result = await store.listRatingEvents({
        playerId: req.params.player_id,
        ladderKey,
        matchId: parsed.data.match_id ?? undefined,
        since: parsed.data.since ?? undefined,
        until: parsed.data.until ?? undefined,
        limit: parsed.data.limit ?? undefined,
        cursor: parsed.data.cursor ?? undefined,
      });

      return res.send({
        rating_events: result.items.map(toRatingEventResponse),
        next_cursor: result.nextCursor ?? null,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      if (err instanceof PlayerLookupError) {
        return res.status(404).send({ error: 'player_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('rating_events_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get(
    '/v1/organizations/:organization_id/players/:player_id/rating-events/:rating_event_id',
    requireAuth,
    async (req, res) => {
      if (!(
        hasScope(req, 'ratings:read') ||
        hasScope(req, 'matches:read') ||
        hasScope(req, 'matches:write')
      )) {
        return res
          .status(403)
          .send({ error: 'insufficient_scope', required: 'ratings:read|matches:read|matches:write' });
      }

      const parsed = LadderDimensionsQuerySchema.safeParse(req.query);
      if (!parsed.success) {
        return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
      }

      try {
        const organization = await resolveOrganization({ organization_id: req.params.organization_id });
        const ladderKey = buildLadderKeyForOrganization(organization.organizationId, parsed.data);

        await authorizeOrgAccess(req, organization.organizationId, {
          permissions: ['matches:write', 'matches:read', 'ratings:read'],
          sport: ladderKey.sport,
          regionId: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
          errorCode: 'ratings_events_denied',
          errorMessage: 'Insufficient grants to read rating events',
        });

        const event = await store.getRatingEvent({
          ladderKey,
          playerId: req.params.player_id,
          ratingEventId: req.params.rating_event_id,
        });

        if (!event) {
          return res.status(404).send({ error: 'rating_event_not_found', message: 'Rating event not found' });
        }

        return res.send(toRatingEventResponse(event));
      } catch (err) {
        if (err instanceof OrganizationLookupError) {
          return res.status(404).send({ error: 'organization_not_found', message: err.message });
        }
        if (err instanceof PlayerLookupError) {
          return res.status(404).send({ error: 'player_not_found', message: err.message });
        }
        if (err instanceof AuthorizationError) {
          return res.status(err.status).send({ error: err.code, message: err.message });
        }
        console.error('rating_event_detail_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

  app.get('/v1/organizations/:organization_id/players/:player_id/rating-snapshot', requireAuth, async (req, res) => {
    if (!(
      hasScope(req, 'ratings:read') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'matches:write')
    )) {
      return res.status(403).send({ error: 'insufficient_scope', required: 'ratings:read|matches:read|matches:write' });
    }

    const parsed = RatingSnapshotQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({ organization_id: req.params.organization_id });
      const ladderKey = buildLadderKeyForOrganization(organization.organizationId, parsed.data);

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['matches:write', 'matches:read', 'ratings:read'],
        sport: ladderKey.sport,
        regionId: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        errorCode: 'ratings_snapshot_denied',
        errorMessage: 'Insufficient grants to read rating snapshots',
      });

      const snapshot = await store.getRatingSnapshot({
        playerId: req.params.player_id,
        ladderKey,
        asOf: parsed.data.as_of ?? undefined,
      });

      if (!snapshot) {
        return res.status(404).send({ error: 'rating_snapshot_not_found', message: 'Rating snapshot not found' });
      }

      return res.send({
        organization_id: snapshot.organizationId,
        player_id: snapshot.playerId,
        as_of: snapshot.asOf,
        mu: snapshot.mu,
        sigma: snapshot.sigma,
        rating_event: snapshot.ratingEvent ? toRatingEventResponse(snapshot.ratingEvent) : null,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      if (err instanceof PlayerLookupError) {
        return res.status(404).send({ error: 'player_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('rating_snapshot_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
