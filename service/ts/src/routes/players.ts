import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  getSubjectId,
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
  PlayerInsightsQuery,
  PlayerRecord,
  PlayerInsightAiData,
} from '../store/index.js';
import { OrganizationLookupError, PlayerLookupError, InvalidBirthInputError } from '../store/index.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toPlayerResponse, toPlayerInsightsResponse, toPlayerSportTotals } from './helpers/responders.js';
import type { ProfilePhotoService } from '../services/profile-photos.js';
import {
  ProfilePhotoServiceDisabledError,
  ProfilePhotoNotReadyError,
  CloudflareImagesError,
} from '../services/profile-photos.js';

const LooseRecordSchema = z.record(z.unknown());
const SportEnum = z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']);
const DisciplineEnum = z.enum(['SINGLES', 'DOUBLES', 'MIXED']);

const PlayerRankingSnapshotSchema = z.object({
  source: z.string(),
  discipline: DisciplineEnum.nullable().optional(),
  position: z.number().int().min(1).nullable().optional(),
  points: z.number().nullable().optional(),
  as_of: z.string().datetime().nullable().optional(),
  metadata: LooseRecordSchema.nullish(),
});

const PlayerCompetitiveProfileSchema = z.object({
  discipline: DisciplineEnum.nullable().optional(),
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

const ProfilePhotoContentTypeEnum = z.enum(['image/jpeg', 'image/png', 'image/webp', 'image/avif'] as const);

const requireOrganizationIdentifier = <T extends { organization_id?: string; organization_slug?: string }>(
  schema: z.ZodType<T>
) =>
  schema.refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const PlayerPhotoUploadSchema = requireOrganizationIdentifier(
  z.object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    content_type: ProfilePhotoContentTypeEnum.optional(),
  })
);

const PlayerPhotoDeleteSchema = requireOrganizationIdentifier(
  z.object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
  })
);

const PlayerPhotoFinalizeSchema = requireOrganizationIdentifier(
  z.object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    image_id: z.string().min(1),
  })
);

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
    birth_date: z.string().date().optional(),
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

const PlayerSportTotalsQuerySchema = requireOrganizationIdentifier(
  z
    .object({
      organization_id: z.string().optional(),
      organization_slug: z.string().optional(),
      sport: SportEnum.optional(),
      discipline: DisciplineEnum.optional(),
    })
    .refine((data) => !data.discipline || data.sport, {
      message: 'sport is required when discipline provided',
      path: ['sport'],
    })
);

const PlayerGetQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const PlayerInsightsQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: SportEnum.optional(),
    discipline: DisciplineEnum.optional(),
    force_refresh: z.union([z.string(), z.boolean()]).optional(),
    include_ai: z.union([z.string(), z.boolean()]).optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  })
  .refine((data) => !data.discipline || data.sport, {
    message: 'sport is required when discipline provided',
    path: ['sport'],
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
    birth_date: z.string().date().nullable().optional(),
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
      data.birth_date !== undefined ||
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

const parseBooleanParam = (value: unknown): boolean | null => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  }
  return null;
};

const AI_PROMPT_VERSION = 'v1';

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
  photoService: ProfilePhotoService;
}

export const registerPlayerRoutes = (app: Express, deps: PlayerRouteDeps) => {
  const { store, resolveOrganization, photoService } = deps;

  const withProfilePhoto = (player: PlayerRecord) => {
    const url = photoService.getPublicUrl(player.profilePhotoId);
    return {
      ...player,
      profilePhotoUrl: url ?? undefined,
    };
  };

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
        birthDate: parsed.data.birth_date ?? undefined,
        countryCode: parsed.data.country_code,
        regionId: parsed.data.region_id,
        ...(competitiveProfile !== undefined ? { competitiveProfile } : {}),
        ...(attributes !== undefined ? { attributes } : {}),
      });

      return res.send(toPlayerResponse(withProfilePhoto(created), organization.slug));
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof InvalidBirthInputError) {
        return res.status(400).send({ error: err.code, message: err.message });
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
        players: result.items.map((player) => toPlayerResponse(withProfilePhoto(player), organization.slug)),
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

  app.get('/v1/players/totals/by-sport', requireAuth, async (req, res) => {
    const parsed = PlayerSportTotalsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const { organization_id, organization_slug, sport, discipline } = parsed.data;

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['players:read', 'matches:read', 'ratings:read'],
        ...(sport ? { sport } : {}),
        errorCode: 'players_read_denied',
        errorMessage: 'Insufficient grants for players:read',
      });

      const result = await store.countPlayersBySport({
        organizationId: organization.organizationId,
        sport: sport ?? undefined,
        discipline: discipline ?? undefined,
      });

      return res.send({
        organization_id: organization.organizationId,
        organization_slug: organization.slug ?? null,
        sport_totals: toPlayerSportTotals(result.totals),
      });
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      console.error('player_totals_error', err);
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

      return res.send(toPlayerResponse(withProfilePhoto(player), organization.slug, { forceNullDefaults: true }));
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

  app.get('/v1/players/:player_id/insights', requireAuth, async (req, res) => {
    const parsed = PlayerInsightsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const { organization_id, organization_slug, sport, discipline, force_refresh, include_ai } = parsed.data;
    const forceRefresh = parseBooleanParam(force_refresh) ?? false;
    const includeAi = parseBooleanParam(include_ai) ?? false;

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

      await authorizeOrgAccess(req, organization.organizationId, {
        permissions: ['players:read', 'matches:read', 'ratings:read'],
        ...(sport ? { sport } : {}),
        errorCode: 'players_read_denied',
        errorMessage: 'Insufficient grants for players:read',
      });

      const insightsQuery = {
        organizationId: organization.organizationId,
        playerId: req.params.player_id,
        sport: sport ?? null,
        discipline: discipline ?? null,
      } satisfies PlayerInsightsQuery;

      const ifNoneMatch = req.header('if-none-match');

      let snapshot = forceRefresh ? null : await store.getPlayerInsights(insightsQuery);
      let etag = snapshot?.cacheKeys?.etag ?? null;

      if (snapshot && ifNoneMatch && etag && ifNoneMatch === etag) {
        res.status(304).end();
        return;
      }

      if (!snapshot || forceRefresh) {
        const built = await store.buildPlayerInsightsSnapshot(insightsQuery);
        const upserted = await store.upsertPlayerInsightsSnapshot(insightsQuery, built);
        snapshot = upserted.snapshot;
        etag = upserted.etag;
      }

      if (!snapshot) {
        return res.status(404).send({ error: 'insights_not_found' });
      }

      let aiState: PlayerInsightAiData | null = null;
      let aiJobId: string | null = null;

      if (includeAi) {
        const digest = snapshot.cacheKeys?.digest ?? null;
        if (digest) {
          let requestedBy: string | null = null;
          try {
            requestedBy = await getSubjectId(req);
          } catch (err) {
            requestedBy = null;
          }

          const ensureResult = await store.ensurePlayerInsightAiState({
            organizationId: organization.organizationId,
            playerId: req.params.player_id,
            sport: sport ?? null,
            discipline: discipline ?? null,
            snapshotDigest: digest,
            promptVersion: AI_PROMPT_VERSION,
            requestedAt: new Date(),
            enqueue: true,
            payload: {
              requested_by: requestedBy,
              source: 'api.players.insights',
            },
          });
          aiState = ensureResult.state;
          aiJobId = ensureResult.jobId ?? null;
        } else {
          aiState = {
            snapshotDigest: 'unavailable',
            promptVersion: AI_PROMPT_VERSION,
            status: 'DISABLED',
            narrative: null,
            model: null,
            generatedAt: null,
            tokens: null,
            expiresAt: null,
            lastRequestedAt: null,
            pollAfterMs: null,
            errorCode: 'digest_missing',
            errorMessage: 'Snapshot digest unavailable for AI generation',
          };
        }
      }

      if (etag) {
        res.setHeader('ETag', etag);
      }

      return res.send(
        toPlayerInsightsResponse(snapshot, {
          ai: includeAi ? aiState : null,
          aiJobId,
        })
      );
    } catch (err) {
      if (err instanceof OrganizationLookupError) {
        return res.status(400).send({ error: 'invalid_organization', message: err.message });
      }
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      if (err instanceof PlayerLookupError) {
        return res.status(404).send({ error: 'player_not_found', message: err.message });
      }
      console.error('player_insights_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.post(
    '/v1/players/:player_id/profile-photo/upload',
    requireAuth,
    requireScope('matches:write'),
    async (req, res) => {
      const parsed = PlayerPhotoUploadSchema.safeParse(req.body);
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

        const player = await store.getPlayer(req.params.player_id, organization.organizationId);
        if (!player) {
          return res.status(404).send({ error: 'player_not_found' });
        }

        if (!photoService.isEnabled()) {
          return res.status(503).send({
            error: 'profile_photo_service_disabled',
            message: 'Profile photo uploads are not configured',
          });
        }

        const uploadRequest = await photoService.createDirectUpload({
          organizationId: organization.organizationId,
          playerId: player.playerId,
          contentType: parsed.data.content_type,
        });

        return res.send({
          upload: {
            url: uploadRequest.uploadUrl,
            method: 'POST',
            headers: {},
            expires_at: uploadRequest.expiresAt,
          },
          profile_photo: {
            image_id: uploadRequest.imageId,
            url: null,
            requires_finalize: true,
          },
          previous_photo: player.profilePhotoId
            ? {
                image_id: player.profilePhotoId,
                url: photoService.getPublicUrl(player.profilePhotoId),
              }
            : null,
          player: toPlayerResponse(withProfilePhoto(player), organization.slug, { forceNullDefaults: true }),
        });
      } catch (err) {
        if (err instanceof OrganizationLookupError) {
          return res.status(400).send({ error: 'invalid_organization', message: err.message });
        }
        if (err instanceof AuthorizationError) {
          return res.status(err.status).send({ error: err.code, message: err.message });
        }
        if (err instanceof PlayerLookupError) {
          return res.status(404).send({ error: 'player_not_found', message: err.message });
        }
        if (err instanceof ProfilePhotoServiceDisabledError) {
          return res.status(503).send({
            error: 'profile_photo_service_disabled',
            message: err.message,
          });
        }
        if (err instanceof CloudflareImagesError && err.status === 404) {
          return res.status(404).send({ error: 'profile_photo_not_found', message: err.message });
        }
        console.error('player_profile_photo_upload_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

  app.delete(
    '/v1/players/:player_id/profile-photo',
    requireAuth,
    requireScope('matches:write'),
    async (req, res) => {
      const parsed = PlayerPhotoDeleteSchema.safeParse(req.body);
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

        const player = await store.getPlayer(req.params.player_id, organization.organizationId);
        if (!player) {
          return res.status(404).send({ error: 'player_not_found' });
        }

        const previousId = player.profilePhotoId ?? null;
        await photoService.delete(previousId);

        const updated = await store.updatePlayer(player.playerId, organization.organizationId, {
          profilePhotoId: null,
          profilePhotoUploadedAt: null,
        });

        return res.send({
          deleted_image_id: previousId,
          player: toPlayerResponse(withProfilePhoto(updated), organization.slug, { forceNullDefaults: true }),
        });
      } catch (err) {
        if (err instanceof OrganizationLookupError) {
          return res.status(400).send({ error: 'invalid_organization', message: err.message });
        }
        if (err instanceof AuthorizationError) {
          return res.status(err.status).send({ error: err.code, message: err.message });
        }
        if (err instanceof PlayerLookupError) {
          return res.status(404).send({ error: 'player_not_found', message: err.message });
        }
        console.error('player_profile_photo_delete_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

  app.post(
    '/v1/players/:player_id/profile-photo/finalize',
    requireAuth,
    requireScope('matches:write'),
    async (req, res) => {
      const parsed = PlayerPhotoFinalizeSchema.safeParse(req.body);
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

        const player = await store.getPlayer(req.params.player_id, organization.organizationId);
        if (!player) {
          return res.status(404).send({ error: 'player_not_found' });
        }

        if (!photoService.isEnabled()) {
          return res.status(503).send({
            error: 'profile_photo_service_disabled',
            message: 'Profile photo uploads are not configured',
          });
        }

        const finalizeResult = await photoService.finalize(parsed.data.image_id);

        const previousId = player.profilePhotoId ?? null;
        const uploadedAt = finalizeResult.uploadedAt ?? new Date().toISOString();

        const updated = await store.updatePlayer(player.playerId, organization.organizationId, {
          profilePhotoId: finalizeResult.imageId,
          profilePhotoUploadedAt: uploadedAt,
        });

        if (previousId && previousId !== finalizeResult.imageId) {
          await photoService.delete(previousId);
        }

        return res.send({
          profile_photo: {
            image_id: finalizeResult.imageId,
            url: finalizeResult.variants.default ?? photoService.getPublicUrl(finalizeResult.imageId),
            uploaded_at: updated.profilePhotoUploadedAt ?? uploadedAt,
            variants: finalizeResult.variants,
          },
          player: toPlayerResponse(withProfilePhoto(updated), organization.slug, { forceNullDefaults: true }),
        });
      } catch (err) {
        if (err instanceof OrganizationLookupError) {
          return res.status(400).send({ error: 'invalid_organization', message: err.message });
        }
        if (err instanceof AuthorizationError) {
          return res.status(err.status).send({ error: err.code, message: err.message });
        }
        if (err instanceof PlayerLookupError) {
          return res.status(404).send({ error: 'player_not_found', message: err.message });
        }
        if (err instanceof ProfilePhotoServiceDisabledError) {
          return res.status(503).send({
            error: 'profile_photo_service_disabled',
            message: err.message,
          });
        }
        if (err instanceof ProfilePhotoNotReadyError) {
          return res.status(409).send({
            error: 'profile_photo_not_ready',
            message: 'Uploaded object is not yet processed. Try again shortly.',
          });
        }
        if (err instanceof CloudflareImagesError && err.status === 404) {
          return res.status(404).send({
            error: 'profile_photo_not_found',
            message: 'Cloudflare Images could not locate the uploaded asset',
          });
        }
        console.error('player_profile_photo_finalize_error', err);
        return res.status(500).send({ error: 'internal_error' });
      }
    }
  );

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
      if (parsed.data.birth_date !== undefined) updateInput.birthDate = parsed.data.birth_date;
      if (parsed.data.country_code !== undefined) updateInput.countryCode = parsed.data.country_code;
      if (parsed.data.region_id !== undefined) updateInput.regionId = parsed.data.region_id;
      if (parsed.data.competitive_profile !== undefined) {
        updateInput.competitiveProfile = mapCompetitiveProfileInput(parsed.data.competitive_profile) ?? null;
      }
      if (parsed.data.attributes !== undefined) {
        updateInput.attributes = mapPlayerAttributesInput(parsed.data.attributes) ?? null;
      }

      const updated = await store.updatePlayer(req.params.player_id, organization.organizationId, updateInput);

      return res.send(toPlayerResponse(withProfilePhoto(updated), organization.slug, { forceNullDefaults: true }));
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
      if (err instanceof InvalidBirthInputError) {
        return res.status(400).send({ error: err.code, message: err.message });
      }
      console.error('player_update_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });
};
