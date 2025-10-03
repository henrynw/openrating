import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  requireAuth,
  requireScope,
} from '../auth.js';
import type {
  PlayerUpdateInput,
  RatingStore,
  OrganizationRecord,
  PlayerCompetitiveProfile,
  PlayerAttributes,
  PlayerRankingSnapshot,
} from '../store/index.js';
import { OrganizationLookupError, PlayerLookupError } from '../store/index.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toPlayerResponse } from './helpers/responders.js';

const LooseRecordSchema = z.record(z.unknown());

const PlayerRankingSnapshotSchema = z.object({
  source: z.string(),
  discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).nullable().optional(),
  position: z.number().int().min(1).nullable().optional(),
  points: z.number().nullable().optional(),
  as_of: z.string().datetime().nullable().optional(),
  metadata: LooseRecordSchema.nullish(),
});

const PlayerCompetitiveProfileSchema = z.object({
  discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).nullable().optional(),
  ranking_points: z.number().nullable().optional(),
  ranking_position: z.number().int().min(1).nullable().optional(),
  total_matches: z.number().int().min(0).nullable().optional(),
  as_of: z.string().datetime().nullable().optional(),
  external_rankings: z.array(PlayerRankingSnapshotSchema).nullable().optional(),
});

const PlayerAttributesSchema = z.object({
  handedness: z.enum(['LEFT', 'RIGHT', 'AMBIDEXTROUS', 'OTHER']).nullable().optional(),
  dominant_side: z.enum(['DEUCE', 'AD', 'LEFT', 'RIGHT', 'BOTH', 'OTHER']).nullable().optional(),
  height_cm: z.number().nullable().optional(),
  weight_kg: z.number().nullable().optional(),
  birth_date: z.string().date().nullable().optional(),
  residence: z.string().nullable().optional(),
  metadata: LooseRecordSchema.nullish(),
});

const PlayerUpsertSchema = z
  .object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    display_name: z.string().min(1),
    short_name: z.string().optional(),
    native_name: z.string().optional(),
    external_ref: z.string().optional(),
    given_name: z.string().optional(),
    family_name: z.string().optional(),
    sex: z.enum(['M', 'F', 'X']).optional(),
    birth_year: z.number().int().optional(),
    country_code: z.string().optional(),
    region_id: z.string().optional(),
    competitive_profile: PlayerCompetitiveProfileSchema.nullable().optional(),
    attributes: PlayerAttributesSchema.nullable().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const PlayerListQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
    cursor: z.string().optional(),
    q: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const PlayerGetQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const PlayerUpdateSchema = z
  .object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    display_name: z.string().min(1).optional(),
    short_name: z.string().nullable().optional(),
    native_name: z.string().nullable().optional(),
    external_ref: z.string().nullable().optional(),
    given_name: z.string().nullable().optional(),
    family_name: z.string().nullable().optional(),
    sex: z.enum(['M', 'F', 'X']).nullable().optional(),
    birth_year: z.number().int().nullable().optional(),
    country_code: z.string().nullable().optional(),
    region_id: z.string().nullable().optional(),
    competitive_profile: PlayerCompetitiveProfileSchema.nullable().optional(),
    attributes: PlayerAttributesSchema.nullable().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  })
  .refine(
    (data) =>
      data.display_name !== undefined ||
      data.short_name !== undefined ||
      data.native_name !== undefined ||
      data.external_ref !== undefined ||
      data.given_name !== undefined ||
      data.family_name !== undefined ||
      data.sex !== undefined ||
      data.birth_year !== undefined ||
      data.country_code !== undefined ||
      data.region_id !== undefined ||
      data.competitive_profile !== undefined ||
      data.attributes !== undefined,
    {
      message: 'At least one field is required',
      path: ['display_name'],
    }
  );

type PlayerRankingSnapshotInput = z.infer<typeof PlayerRankingSnapshotSchema>;
type PlayerCompetitiveProfileInput = z.infer<typeof PlayerCompetitiveProfileSchema>;
type PlayerAttributesInput = z.infer<typeof PlayerAttributesSchema>;

const mapRankingSnapshotInput = (snapshot: PlayerRankingSnapshotInput): PlayerRankingSnapshot => ({
  source: snapshot.source,
  discipline: snapshot.discipline ?? null,
  position: snapshot.position ?? null,
  points: snapshot.points ?? null,
  asOf: snapshot.as_of ?? null,
  metadata: snapshot.metadata ?? null,
});

const mapCompetitiveProfileInput = (
  input: PlayerCompetitiveProfileInput | null | undefined
): PlayerCompetitiveProfile | null | undefined => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  const externalRankings = input.external_rankings
    ? input.external_rankings.map(mapRankingSnapshotInput)
    : null;
  return {
    discipline: input.discipline ?? null,
    rankingPoints: input.ranking_points ?? null,
    rankingPosition: input.ranking_position ?? null,
    totalMatches: input.total_matches ?? null,
    asOf: input.as_of ?? null,
    externalRankings,
  };
};

const mapPlayerAttributesInput = (
  input: PlayerAttributesInput | null | undefined
): PlayerAttributes | null | undefined => {
  if (input === undefined) return undefined;
  if (input === null) return null;
  return {
    handedness: input.handedness ?? null,
    dominantSide: input.dominant_side ?? null,
    heightCm: input.height_cm ?? null,
    weightKg: input.weight_kg ?? null,
    birthDate: input.birth_date ?? null,
    residence: input.residence ?? null,
    metadata: input.metadata ?? null,
  };
};

interface PlayerRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

export const registerPlayerRoutes = (app: Express, deps: PlayerRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.post('/v1/players', async (req, res) => {
    const parsed = PlayerUpsertSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      const competitiveProfile = mapCompetitiveProfileInput(parsed.data.competitive_profile);
      const attributes = mapPlayerAttributesInput(parsed.data.attributes);

      const created = await store.createPlayer({
        organizationId: organization.organizationId,
        displayName: parsed.data.display_name,
        shortName: parsed.data.short_name,
        nativeName: parsed.data.native_name,
        externalRef: parsed.data.external_ref,
        givenName: parsed.data.given_name,
        familyName: parsed.data.family_name,
        sex: parsed.data.sex,
        birthYear: parsed.data.birth_year,
        countryCode: parsed.data.country_code,
        regionId: parsed.data.region_id,
        ...(competitiveProfile !== undefined ? { competitiveProfile } : {}),
        ...(attributes !== undefined ? { attributes } : {}),
      });

      return res.send(toPlayerResponse(created, organization.slug));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      console.error('player_create_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/players', requireAuth, async (req, res) => {
    const parsed = PlayerListQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const { organization_id, organization_slug, limit, cursor, q } = parsed.data;

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['players:read', 'matches:write'],
        errorCode: 'players_read_denied',
        errorMessage: 'Insufficient grants for players:read',
      });

      const result = await store.listPlayers({ organizationId: organization.organizationId, limit, cursor, q });

      return res.send({
        players: result.items.map((player) => toPlayerResponse(player, organization.slug)),
        next_cursor: result.nextCursor ?? null,
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('players_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.get('/v1/players/:player_id', requireAuth, async (req, res) => {
    const parsed = PlayerGetQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['players:read', 'matches:write', 'matches:read', 'ratings:read'],
        errorCode: 'players_read_denied',
        errorMessage: 'Insufficient grants for players:read',
      });

      const player = await store.getPlayer(req.params.player_id, organization.organizationId);
      if (!player) {
        return res.status(404).send({ error: 'player_not_found' });
      }

      return res.send(toPlayerResponse(player, organization.slug, { forceNullDefaults: true }));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('player_get_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.patch('/v1/players/:player_id', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = PlayerUpdateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['players:write', 'matches:write'],
        errorCode: 'players_update_denied',
        errorMessage: 'Insufficient grants for players:write',
      });

      const updateInput: PlayerUpdateInput = {};
      if (parsed.data.display_name !== undefined) updateInput.displayName = parsed.data.display_name;
      if (parsed.data.short_name !== undefined) updateInput.shortName = parsed.data.short_name;
      if (parsed.data.native_name !== undefined) updateInput.nativeName = parsed.data.native_name;
      if (parsed.data.external_ref !== undefined) updateInput.externalRef = parsed.data.external_ref;
      if (parsed.data.given_name !== undefined) updateInput.givenName = parsed.data.given_name;
      if (parsed.data.family_name !== undefined) updateInput.familyName = parsed.data.family_name;
      if (parsed.data.sex !== undefined) updateInput.sex = parsed.data.sex;
      if (parsed.data.birth_year !== undefined) updateInput.birthYear = parsed.data.birth_year;
      if (parsed.data.country_code !== undefined) updateInput.countryCode = parsed.data.country_code;
      if (parsed.data.region_id !== undefined) updateInput.regionId = parsed.data.region_id;
      if (parsed.data.competitive_profile !== undefined) {
        updateInput.competitiveProfile = mapCompetitiveProfileInput(parsed.data.competitive_profile) ?? null;
      }
      if (parsed.data.attributes !== undefined) {
        updateInput.attributes = mapPlayerAttributesInput(parsed.data.attributes) ?? null;
      }

      const updated = await store.updatePlayer(req.params.player_id, organization.organizationId, updateInput);

      return res.send(toPlayerResponse(updated, organization.slug, { forceNullDefaults: true }));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof PlayerLookupError) {
        if (err.context.missing?.length) {
          return res.status(404).send({ error: 'player_not_found', message: err.message });
        }
        if (err.context.wrongOrganization?.length) {
          return res.status(403).send({ error: 'players_update_denied', message: err.message });
        }
        return res.status(400).send({ error: 'invalid_player', message: err.message });
      }
      console.error('player_update_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
