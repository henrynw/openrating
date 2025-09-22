import express from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';

// ---- engine imports ----
import { updateMatch } from './engine/rating.js';
import { normalizeMatchSubmission } from './formats/index.js';
import { getStore } from './store/index.js';
import type { LadderKey } from './store/index.js';
import { PlayerLookupError, OrganizationLookupError } from './store/index.js';
import { normalizeRegion, normalizeTier } from './store/helpers.js';
import { AuthorizationError, authorizeOrgAccess, enforceMatchWrite, hasScope, requireAuth, requireScope } from './auth.js';

dotenv.config();

const app = express();
app.use(express.json());

const store = getStore();

const normalizeSlug = (value: string) => value.trim().toLowerCase();

async function resolveOrganizationIdentifier(input: { organization_id?: string; organization_slug?: string }) {
  const { organization_id, organization_slug } = input;
  if (organization_id) {
    const org = await store.getOrganizationById(organization_id);
    if (org) return org;
  }
  if (organization_slug) {
    const slug = normalizeSlug(organization_slug);
    const org = await store.getOrganizationBySlug(slug);
    if (org) return org;
  }
  if (!organization_id && !organization_slug) {
    throw new OrganizationLookupError('organization_id or organization_slug is required');
  }
  throw new OrganizationLookupError('Organization not found');
}


// ---- health ----
app.get('/health', (_req, res) => res.status(200).send({ ok: true }));

// ---- organizations ----
const OrganizationCreate = z.object({
  name: z.string().min(1),
  slug: z.string().optional(),
  description: z.string().optional(),
});

const OrganizationListQuery = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional(),
  cursor: z.string().optional(),
  q: z.string().optional(),
});

app.post('/v1/organizations', requireAuth, requireScope('organizations:write'), async (req, res) => {
  const parsed = OrganizationCreate.safeParse(req.body);
  if (!parsed.success) {
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });
  }

  try {
    const created = await store.createOrganization(parsed.data);
    return res.status(201).send({
      organization_id: created.organizationId,
      name: created.name,
      slug: created.slug,
      description: created.description ?? null,
    });
  } catch (err) {
    if (err instanceof OrganizationLookupError) {
      return res.status(409).send({ error: 'duplicate_slug', message: err.message });
    }
    console.error('organization_create_error', err);
    return res.status(500).send({ error: 'internal_error' });
  }
});

app.get('/v1/organizations', requireAuth, async (req, res) => {
  if (!(hasScope(req, 'organizations:read') || hasScope(req, 'organizations:write'))) {
    return res.status(403).send({ error: 'insufficient_scope', required: 'organizations:read|organizations:write' });
  }

  const parsed = OrganizationListQuery.safeParse(req.query);
  if (!parsed.success) {
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });
  }

  try {
    const result = await store.listOrganizations(parsed.data);
    return res.send({
      organizations: result.items.map((org) => ({
        organization_id: org.organizationId,
        name: org.name,
        slug: org.slug,
        description: org.description ?? null,
        created_at: org.createdAt ?? null,
      })),
      next_cursor: result.nextCursor ?? null,
    });
  } catch (err) {
    console.error('organizations_list_error', err);
    return res.status(500).send({ error: 'internal_error' });
  }
});

// ---- players ----
const PlayerUpsert = z
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
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

app.post('/v1/players', async (req, res) => {
  const parsed = PlayerUpsert.safeParse(req.body);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  try {
    const organization = await resolveOrganizationIdentifier({
      organization_id: parsed.data.organization_id,
      organization_slug: parsed.data.organization_slug,
    });

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
    });

    return res.send({
      player_id: created.playerId,
      organization_id: created.organizationId,
      organization_slug: organization.slug,
      display_name: created.displayName,
      short_name: created.shortName,
      native_name: created.nativeName,
      given_name: created.givenName,
      family_name: created.familyName,
      sex: created.sex,
      birth_year: created.birthYear,
      country_code: created.countryCode,
      region_id: created.regionId,
      external_ref: created.externalRef,
    });
  } catch (err) {
    if (err instanceof OrganizationLookupError) {
      return res.status(400).send({ error: 'invalid_organization', message: err.message });
    }
    console.error('player_create_error', err);
    return res.status(500).send({ error: 'internal_error' });
  }
});

const PlayerListQuery = z
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

app.get('/v1/players', requireAuth, async (req, res) => {
  const parsed = PlayerListQuery.safeParse(req.query);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  const { organization_id, organization_slug, limit, cursor, q } = parsed.data;

  try {
    const organization = await resolveOrganizationIdentifier({ organization_id, organization_slug });

    await authorizeOrgAccess(req, organization.organizationId, {
      permissions: ['players:read', 'matches:write'],
      errorCode: 'players_read_denied',
      errorMessage: 'Insufficient grants for players:read',
    });

    const result = await store.listPlayers({ organizationId: organization.organizationId, limit, cursor, q });

    return res.send({
      players: result.items.map((player) => ({
        player_id: player.playerId,
        organization_id: player.organizationId,
        organization_slug: organization.slug,
        display_name: player.displayName,
        short_name: player.shortName,
        native_name: player.nativeName,
        given_name: player.givenName,
        family_name: player.familyName,
        sex: player.sex,
        birth_year: player.birthYear,
        country_code: player.countryCode,
        region_id: player.regionId,
        external_ref: player.externalRef,
      })),
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

// ---- matches ----
const MatchSubmit = z
  .object({
    provider_id: z.string(),
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']),
    discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']),
    format: z.string(),
    start_time: z.string(),
    venue_id: z.string().optional(),
    venue_region_id: z.string().optional(),
    tier: z.enum(['SANCTIONED', 'LEAGUE', 'SOCIAL', 'EXHIBITION']).optional(),
    sides: z.object({
      A: z.object({ players: z.array(z.string()).min(1).max(4) }),
      B: z.object({ players: z.array(z.string()).min(1).max(4) }),
    }),
    games: z.array(
      z.object({ game_no: z.number().int(), a: z.number().int(), b: z.number().int() })
    ),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

app.post('/v1/matches', requireAuth, requireScope('matches:write'), async (req, res) => {
  const parsed = MatchSubmit.safeParse(req.body);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  const normalization = normalizeMatchSubmission({
    sport: parsed.data.sport,
    discipline: parsed.data.discipline,
    format: parsed.data.format,
    tier: parsed.data.tier,
    sides: parsed.data.sides,
    games: parsed.data.games,
  });

  if (!normalization.ok) {
    return res.status(400).send({
      error: normalization.error,
      message: normalization.message,
      issues: normalization.issues,
    });
  }

  const organization = await resolveOrganizationIdentifier({
    organization_id: parsed.data.organization_id,
    organization_slug: parsed.data.organization_slug,
  });

  const ladderKey: LadderKey = {
    organizationId: organization.organizationId,
    sport: normalization.match.sport,
    discipline: normalization.match.discipline,
    format: normalization.match.format,
    tier: normalizeTier(parsed.data.tier),
    regionId: normalizeRegion(parsed.data.venue_region_id),
  };

  const uniquePlayerIds = Array.from(
    new Set([
      ...normalization.match.sides.A.players,
      ...normalization.match.sides.B.players,
    ])
  );

  try {
    try {
      await enforceMatchWrite(req, {
        organizationId: organization.organizationId,
        sport: normalization.match.sport,
        regionId: ladderKey.regionId,
      });
    } catch (err) {
      if (err instanceof AuthorizationError) {
        return res.status(err.status).send({ error: err.code, message: err.message });
      }
      throw err;
    }

    const { ladderId, players } = await store.ensurePlayers(uniquePlayerIds, ladderKey);

    const result = updateMatch(normalization.match, (id) => {
      const state = players.get(id);
      if (!state) throw new Error(`missing player state for ${id}`);
      return state;
    });

    const { matchId } = await store.recordMatch({
      ladderId,
      ladderKey,
      match: normalization.match,
      result,
      playerStates: players,
      submissionMeta: {
        providerId: parsed.data.provider_id,
        organizationId: organization.organizationId,
        startTime: parsed.data.start_time,
        rawPayload: req.body,
        venueId: parsed.data.venue_id ?? null,
        regionId: parsed.data.venue_region_id ?? null,
      },
    });

    return res.send({
      match_id: matchId,
      organization_id: organization.organizationId,
      organization_slug: organization.slug,
      ratings: result.perPlayer.map((p) => ({
        player_id: p.playerId,
        mu_before: p.muBefore,
        mu_after: p.muAfter,
        delta: p.delta,
        sigma_after: p.sigmaAfter,
        win_probability_pre: p.winProbPre,
      })),
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

const MatchListQuery = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: z.string().optional(),
    player_id: z.string().optional(),
    cursor: z.string().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
    start_after: z.string().datetime().optional(),
    start_before: z.string().datetime().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

app.get('/v1/matches', requireAuth, async (req, res) => {
  const parsed = MatchListQuery.safeParse(req.query);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  const { organization_id, organization_slug, sport, player_id, cursor, limit, start_after, start_before } = parsed.data;

  if (!(
    hasScope(req, 'matches:write') ||
    hasScope(req, 'matches:read') ||
    hasScope(req, 'ratings:read')
  )) {
    return res.status(403).send({ error: 'insufficient_scope', required: 'matches:read|matches:write|ratings:read' });
  }

  try {
    const organization = await resolveOrganizationIdentifier({ organization_id, organization_slug });

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
      cursor,
      limit,
      startAfter: start_after ?? undefined,
      startBefore: start_before ?? undefined,
    });

    return res.send({
      matches: result.items.map((match) => ({
        match_id: match.matchId,
        organization_id: match.organizationId,
        organization_slug: organization.slug,
        sport: match.sport,
        discipline: match.discipline,
        format: match.format,
        tier: match.tier,
        start_time: match.startTime,
        venue_id: match.venueId,
        region_id: match.regionId,
        sides: match.sides.reduce((acc, side) => {
          acc[side.side] = { players: side.players };
          return acc;
        }, {} as Record<'A' | 'B', { players: string[] }>),
        games: match.games.map((game) => ({ game_no: game.gameNo, a: game.a, b: game.b })),
      })),
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

const RatingQuery = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']).optional(),
    discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']).optional(),
    format: z.string().optional(),
    tier: z.string().optional(),
    region_id: z.string().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

app.get('/v1/ratings/:player_id', async (req, res) => {
  const queryParse = RatingQuery.safeParse({
    organization_id: req.query.organization_id,
    sport: req.query.sport,
    discipline: req.query.discipline,
    format: req.query.format,
    tier: req.query.tier,
    region_id: req.query.region_id,
  });

  if (!queryParse.success) {
    return res.status(400).send({
      error: 'validation_error',
      details: queryParse.error.flatten(),
    });
  }

  try {
    const organization = await resolveOrganizationIdentifier({
      organization_id: queryParse.data.organization_id,
      organization_slug: queryParse.data.organization_slug,
    });

    const ladderKey: LadderKey = {
      organizationId: organization.organizationId,
      sport: (queryParse.data.sport ?? 'BADMINTON') as LadderKey['sport'],
      discipline: (queryParse.data.discipline ?? 'SINGLES') as LadderKey['discipline'],
      format: queryParse.data.format ?? 'BO3_21RALLY',
      tier: normalizeTier(queryParse.data.tier),
      regionId: normalizeRegion(queryParse.data.region_id),
    };

    const rating = await store.getPlayerRating(req.params.player_id, ladderKey);
    if (!rating) return res.status(404).send({ error: 'not_found' });

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

// ---- start server ----
const port = process.env.PORT ? Number(process.env.PORT) : 8080;
app.listen(port, () => console.log(`OpenRating listening on :${port}`));
