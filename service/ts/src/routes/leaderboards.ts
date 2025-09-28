import type { Express } from 'express';
import { z } from 'zod';

import type { RatingStore, OrganizationRecord } from '../store/index.js';
import { OrganizationLookupError } from '../store/index.js';
import { isDefaultRegion } from '../store/helpers.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { buildLadderKeyForOrganization } from './helpers/ladder.js';
import {
  AuthorizationError,
  authorizeOrgAccess,
  requireAuth,
} from '../auth.js';

const LeaderboardQuerySchema = z.object({
  sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']).optional(),
  discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).optional(),
  format: z.string().optional(),
  tier: z.string().optional(),
  region_id: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(200).optional(),
});

const LeaderboardMoversQuerySchema = LeaderboardQuerySchema.extend({
  since: z.string().datetime(),
});

interface LeaderboardRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

const maybe = <T>(value: T | null | undefined) => (value ?? null);

export const registerLeaderboardRoutes = (app: Express, deps: LeaderboardRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.get('/v1/organizations/:organization_slug/leaderboard', requireAuth, async (req, res) => {
    const parsed = LeaderboardQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({ organization_slug: req.params.organization_slug });

      const ladderKey = buildLadderKeyForOrganization(organization.organizationId, parsed.data);

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['ratings:read', 'matches:read', 'matches:write'],
        sport: ladderKey.sport,
        regionId: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        errorCode: 'leaderboard_read_denied',
        errorMessage: 'Insufficient grants to read leaderboards',
      });
      const result = await store.listLeaderboard({ ladderKey, limit: parsed.data.limit });

      return res.send({
        organization_id: organization.organizationId,
        organization_slug: organization.slug,
        sport: ladderKey.sport,
        discipline: ladderKey.discipline,
        format: ladderKey.format,
        tier: ladderKey.tier === 'UNSPECIFIED' ? null : ladderKey.tier,
        region_id: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        players: result.items.map((entry) => ({
          rank: entry.rank,
          player_id: entry.playerId,
          display_name: entry.displayName,
          short_name: maybe(entry.shortName),
          given_name: maybe(entry.givenName),
          family_name: maybe(entry.familyName),
          country_code: maybe(entry.countryCode),
          region_id: maybe(entry.regionId),
          mu: entry.mu,
          sigma: entry.sigma,
          matches: entry.matches,
          delta: maybe(entry.delta ?? null),
          last_event_at: maybe(entry.lastEventAt ?? null),
          last_match_id: maybe(entry.lastMatchId ?? null),
        })),
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('leaderboard_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/organizations/:organization_slug/leaderboard/movers', requireAuth, async (req, res) => {
    const parsed = LeaderboardMoversQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({ organization_slug: req.params.organization_slug });
      const ladderKey = buildLadderKeyForOrganization(organization.organizationId, parsed.data);

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['ratings:read', 'matches:read', 'matches:write'],
        sport: ladderKey.sport,
        regionId: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        errorCode: 'leaderboard_movers_denied',
        errorMessage: 'Insufficient grants to read leaderboard movers',
      });
      const result = await store.listLeaderboardMovers({
        ladderKey,
        limit: parsed.data.limit,
        since: parsed.data.since,
      });

      return res.send({
        organization_id: organization.organizationId,
        organization_slug: organization.slug,
        sport: ladderKey.sport,
        discipline: ladderKey.discipline,
        format: ladderKey.format,
        tier: ladderKey.tier === 'UNSPECIFIED' ? null : ladderKey.tier,
        region_id: isDefaultRegion(ladderKey.regionId) ? null : ladderKey.regionId,
        since: parsed.data.since,
        players: result.items.map((entry) => ({
          player_id: entry.playerId,
          display_name: entry.displayName,
          short_name: maybe(entry.shortName),
          given_name: maybe(entry.givenName),
          family_name: maybe(entry.familyName),
          country_code: maybe(entry.countryCode),
          region_id: maybe(entry.regionId),
          mu: entry.mu,
          sigma: entry.sigma,
          matches: entry.matches,
          change: entry.change,
          events: entry.events,
          last_event_at: maybe(entry.lastEventAt ?? null),
        })),
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('leaderboard_movers_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
