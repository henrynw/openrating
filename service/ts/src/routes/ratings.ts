import type { Express, Request } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  hasScope,
  requireAuth,
} from '../auth.js';
import type { RatingStore, OrganizationRecord, LadderKey } from '../store/index.js';
import { OrganizationLookupError, PlayerLookupError, InvalidLeaderboardFilterError } from '../store/index.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toRatingEventResponse } from './helpers/responders.js';
import { buildLadderKey } from './helpers/ladder.js';

const SportParamSchema = z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']);
const DisciplineParamSchema = z.enum(['SINGLES', 'DOUBLES', 'MIXED']);

const CompetitionSegmentSchema = z.enum([
  'STANDARD',
  'PARA',
  'JUNIOR',
  'MASTERS',
  'COLLEGIATE',
  'EXHIBITION',
  'OTHER',
]);

const ClassCodesParamSchema = z.preprocess((value) => {
  if (value == null) return undefined;
  const list = Array.isArray(value) ? value : [value];
  const tokens = list
    .flatMap((entry) => String(entry).split(','))
    .map((entry) => entry.trim())
    .filter(Boolean);
  return tokens.length ? tokens : undefined;
}, z.array(z.string().min(1)).optional());

const BaseFilterSchema = z.object({
  scope: z.string().trim().min(1).optional(),
  organization_id: z.string().trim().optional(),
  segment: CompetitionSegmentSchema.optional(),
  class_codes: ClassCodesParamSchema,
});

const LeaderboardQuerySchema = BaseFilterSchema.extend({
  limit: z.coerce.number().int().min(1).max(200).optional(),
  cursor: z.string().trim().min(1).optional(),
  sex: z.enum(['M', 'F', 'X']).optional(),
  country_code: z
    .string()
    .trim()
    .regex(/^[A-Za-z]{2,3}$/)
    .optional(),
  region_id: z.string().trim().optional(),
  age_group: z.string().trim().min(1).optional(),
  age_from: z.coerce.number().int().min(0).optional(),
  age_to: z.coerce.number().int().min(0).optional(),
  age_cutoff: z.string().date().optional(),
});

const MoversQuerySchema = BaseFilterSchema.extend({
  since: z.string().datetime(),
  limit: z.coerce.number().int().min(1).max(200).optional(),
});

const RatingEventsQuerySchema = BaseFilterSchema.extend({
  since: z.string().datetime().optional(),
  until: z.string().datetime().optional(),
  match_id: z.string().trim().optional(),
  limit: z.coerce.number().int().min(1).max(200).optional(),
  cursor: z.string().optional(),
});

const RatingSnapshotQuerySchema = BaseFilterSchema.extend({
  as_of: z.string().datetime().optional(),
});

const PlayerSummaryQuerySchema = BaseFilterSchema;

interface RatingRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

const parsePathParams = (params: any) => {
  const parsed = z
    .object({ sport: SportParamSchema, discipline: DisciplineParamSchema })
    .safeParse(params);
  if (!parsed.success) {
    return { success: false as const, error: parsed.error.flatten() };
  }
  return { success: true as const, data: parsed.data };
};

const withScopeOnKey = (key: LadderKey, scope?: string) => {
  if (scope) {
    key.tier = scope;
  }
  return key;
};

const ensureScopedAccess = async (
  req: Request,
  organization: OrganizationRecord | null,
  sport: string
) => {
  if (!organization) return;
  await authorizeOrgAccess(req as any, organization.organizationId, {
    permissions: ['ratings:read', 'matches:read', 'matches:write'],
    sport,
  });
};

export const registerRatingRoutes = (app: Express, deps: RatingRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.get('/v1/ratings/:sport/:discipline', requireAuth, async (req, res) => {
    const params = parsePathParams(req.params);
    if (!params.success) {
      return res.status(400).send({ error: 'validation_error', details: params.error });
    }

    const query = LeaderboardQuerySchema.safeParse(req.query);
    if (!query.success) {
      return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
    }

    try {
      let organization: OrganizationRecord | null = null;
      if (query.data.organization_id) {
        organization = await resolveOrganization({ organization_id: query.data.organization_id });
        await ensureScopedAccess(req, organization, params.data.sport);
      }

      const ladderKey = withScopeOnKey(
        buildLadderKey({
          sport: params.data.sport,
          discipline: params.data.discipline,
          segment: query.data.segment ?? null,
          classCodes: query.data.class_codes ?? null,
        }),
        query.data.scope
      );

      const leaderboard = await store.listLeaderboard({
        sport: params.data.sport,
        discipline: params.data.discipline,
        scope: query.data.scope ?? null,
        organizationId: organization?.organizationId ?? null,
        sex: query.data.sex ?? null,
        countryCode: query.data.country_code ? query.data.country_code.toUpperCase() : null,
        regionId: query.data.region_id ?? null,
        ageGroup: query.data.age_group,
        ageFrom: query.data.age_from,
        ageTo: query.data.age_to,
        ageCutoff: query.data.age_cutoff,
        limit: query.data.limit ?? undefined,
        cursor: query.data.cursor ?? undefined,
        segment: ladderKey.segment ?? null,
        classCodes: ladderKey.classCodes ?? null,
      });

      const totalPages = leaderboard.totalCount
        ? Math.ceil(leaderboard.totalCount / leaderboard.pageSize)
        : 0;

      return res.send({
        sport: params.data.sport,
        discipline: params.data.discipline,
        scope: query.data.scope ?? null,
        segment: ladderKey.segment ?? null,
        class_codes: ladderKey.classCodes ?? null,
        organization_id: organization ? organization.organizationId : null,
        total_players: leaderboard.totalCount,
        page_size: leaderboard.pageSize,
        total_pages: totalPages,
        has_more: Boolean(leaderboard.nextCursor),
        next_cursor: leaderboard.nextCursor ?? null,
        players: leaderboard.items.map((player) => ({
          rank: player.rank,
          player_id: player.playerId,
          display_name: player.displayName,
          short_name: player.shortName ?? null,
          given_name: player.givenName ?? null,
          family_name: player.familyName ?? null,
          country_code: player.countryCode ?? null,
          region_id: player.regionId ?? null,
          mu: player.mu,
          mu_raw: player.muRaw ?? null,
          sigma: player.sigma,
          matches: player.matches,
          delta: player.delta ?? null,
          last_event_at: player.lastEventAt ?? null,
          last_match_id: player.lastMatchId ?? null,
        })),
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof InvalidLeaderboardFilterError) {
        return res.status(400).send({ error: err.code, message: err.message });
      }
      console.error('leaderboard_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/ratings/:sport/:discipline/movers', requireAuth, async (req, res) => {
    const params = parsePathParams(req.params);
    if (!params.success) {
      return res.status(400).send({ error: 'validation_error', details: params.error });
    }

    const query = MoversQuerySchema.safeParse(req.query);
    if (!query.success) {
      return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
    }

    try {
      let organization: OrganizationRecord | null = null;
      if (query.data.organization_id) {
        organization = await resolveOrganization({ organization_id: query.data.organization_id });
        await ensureScopedAccess(req, organization, params.data.sport);
      }

      const ladderKey = withScopeOnKey(
        buildLadderKey({
          sport: params.data.sport,
          discipline: params.data.discipline,
          segment: query.data.segment ?? null,
          classCodes: query.data.class_codes ?? null,
        }),
        query.data.scope
      );

      const movers = await store.listLeaderboardMovers({
        sport: params.data.sport,
        discipline: params.data.discipline,
        scope: query.data.scope ?? null,
        organizationId: organization?.organizationId ?? null,
        since: query.data.since,
        limit: query.data.limit ?? undefined,
        segment: ladderKey.segment ?? null,
        classCodes: ladderKey.classCodes ?? null,
      });

      return res.send({
        sport: params.data.sport,
        discipline: params.data.discipline,
        scope: query.data.scope ?? null,
        segment: ladderKey.segment ?? null,
        class_codes: ladderKey.classCodes ?? null,
        organization_id: organization ? organization.organizationId : null,
        since: query.data.since,
        players: movers.items.map((player) => ({
          player_id: player.playerId,
          display_name: player.displayName,
          short_name: player.shortName ?? null,
          given_name: player.givenName ?? null,
          family_name: player.familyName ?? null,
          country_code: player.countryCode ?? null,
          region_id: player.regionId ?? null,
          mu: player.mu,
          mu_raw: player.muRaw ?? null,
          sigma: player.sigma,
          matches: player.matches,
          change: player.change,
          events: player.events,
          last_event_at: player.lastEventAt ?? null,
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

  app.get('/v1/ratings/:sport/:discipline/players/:player_id', async (req, res) => {
    const params = parsePathParams(req.params);
    if (!params.success) {
      return res.status(400).send({ error: 'validation_error', details: params.error });
    }

    const query = PlayerSummaryQuerySchema.safeParse(req.query);
    if (!query.success) {
      return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
    }

    try {
      let organization: OrganizationRecord | null = null;
      if (query.data.organization_id) {
        organization = await resolveOrganization({ organization_id: query.data.organization_id });
      }

      const ladderKey = withScopeOnKey(
        buildLadderKey({
          sport: params.data.sport,
          discipline: params.data.discipline,
          segment: query.data.segment ?? null,
          classCodes: query.data.class_codes ?? null,
        }),
        query.data.scope
      );

      const rating = await store.getPlayerRating(req.params.player_id, ladderKey);
      if (!rating) {
        return res.status(404).send({ error: 'player_not_found' });
      }

      const bias = rating.sexBias ?? 0;
      return res.send({
        player_id: rating.playerId,
        sport: params.data.sport,
        discipline: params.data.discipline,
        scope: query.data.scope ?? null,
        segment: ladderKey.segment ?? null,
        class_codes: ladderKey.classCodes ?? null,
        organization_id: organization ? organization.organizationId : null,
        mu: rating.mu + bias,
        mu_raw: rating.mu,
        sex_bias: bias,
        sigma: rating.sigma,
        matches: rating.matchesCount,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(404).send({ error: 'organization_not_found', message: err.message });
      }
      console.error('rating_summary_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get(
    '/v1/ratings/:sport/:discipline/players/:player_id/snapshot',
    requireAuth,
    async (req, res) => {
      const params = parsePathParams(req.params);
      if (!params.success) {
        return res.status(400).send({ error: 'validation_error', details: params.error });
      }

      const query = RatingSnapshotQuerySchema.safeParse(req.query);
      if (!query.success) {
        return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
      }

      try {
        let organization: OrganizationRecord | null = null;
        if (query.data.organization_id) {
          organization = await resolveOrganization({ organization_id: query.data.organization_id });
          await ensureScopedAccess(req, organization, params.data.sport);
        }

        const ladderKey = withScopeOnKey(
          buildLadderKey({
            sport: params.data.sport,
            discipline: params.data.discipline,
            segment: query.data.segment ?? null,
            classCodes: query.data.class_codes ?? null,
          }),
          query.data.scope
        );

        const snapshot = await store.getRatingSnapshot({
          playerId: req.params.player_id,
          ladderKey,
          asOf: query.data.as_of,
          organizationId: organization?.organizationId ?? undefined,
        });

        if (!snapshot) {
          return res.status(404).send({ error: 'player_not_found' });
        }

        return res.send({
          player_id: snapshot.playerId,
          sport: params.data.sport,
          discipline: params.data.discipline,
          scope: snapshot.scope ?? null,
          segment: ladderKey.segment ?? null,
          class_codes: ladderKey.classCodes ?? null,
          organization_id: snapshot.organizationId ?? null,
          as_of: snapshot.asOf,
          mu: snapshot.mu,
          mu_raw: snapshot.muRaw ?? snapshot.mu,
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
    }
  );

  app.get(
    '/v1/ratings/:sport/:discipline/players/:player_id/events',
    requireAuth,
    async (req, res) => {
      if (
        !(
          hasScope(req, 'ratings:read') ||
          hasScope(req, 'matches:read') ||
          hasScope(req, 'matches:write')
        )
      ) {
        return res.status(403).send({ error: 'insufficient_scope', required: 'ratings:read|matches:read|matches:write' });
      }

      const params = parsePathParams(req.params);
      if (!params.success) {
        return res.status(400).send({ error: 'validation_error', details: params.error });
      }

      const query = RatingEventsQuerySchema.safeParse(req.query);
      if (!query.success) {
        return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
      }

      try {
        let organization: OrganizationRecord | null = null;
        if (query.data.organization_id) {
          organization = await resolveOrganization({ organization_id: query.data.organization_id });
          await ensureScopedAccess(req, organization, params.data.sport);
        }

        const ladderKey = withScopeOnKey(
          buildLadderKey({
            sport: params.data.sport,
            discipline: params.data.discipline,
            segment: query.data.segment ?? null,
            classCodes: query.data.class_codes ?? null,
          }),
          query.data.scope
        );

        const result = await store.listRatingEvents({
          playerId: req.params.player_id,
          ladderKey,
          organizationId: organization?.organizationId ?? undefined,
          matchId: query.data.match_id ?? undefined,
          since: query.data.since ?? undefined,
          until: query.data.until ?? undefined,
          limit: query.data.limit ?? undefined,
          cursor: query.data.cursor ?? undefined,
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
        console.error('rating_events_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

  app.get(
    '/v1/ratings/:sport/:discipline/players/:player_id/events/:rating_event_id',
    requireAuth,
    async (req, res) => {
      if (
        !(
          hasScope(req, 'ratings:read') ||
          hasScope(req, 'matches:read') ||
          hasScope(req, 'matches:write')
        )
      ) {
        return res.status(403).send({ error: 'insufficient_scope', required: 'ratings:read|matches:read|matches:write' });
      }

      const params = parsePathParams(req.params);
      if (!params.success) {
        return res.status(400).send({ error: 'validation_error', details: params.error });
      }

      const query = BaseFilterSchema.safeParse(req.query);
      if (!query.success) {
        return res.status(400).send({ error: 'validation_error', details: query.error.flatten() });
      }

      try {
        let organization: OrganizationRecord | null = null;
        if (query.data.organization_id) {
          organization = await resolveOrganization({ organization_id: query.data.organization_id });
          await ensureScopedAccess(req, organization, params.data.sport);
        }

        const ladderKey = withScopeOnKey(
          buildLadderKey({
            sport: params.data.sport,
            discipline: params.data.discipline,
            segment: query.data.segment ?? null,
            classCodes: query.data.class_codes ?? null,
          }),
          query.data.scope
        );

        const event = await store.getRatingEvent({
          ladderKey,
          playerId: req.params.player_id,
          ratingEventId: req.params.rating_event_id,
          organizationId: organization?.organizationId ?? undefined,
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
};
