import express from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';

// ---- engine imports ----
import { updateMatch } from './engine/rating.js';
import { normalizeMatchSubmission } from './formats/index.js';
import { getStore } from './store/index.js';
import type { LadderKey, OrganizationUpdateInput, PlayerUpdateInput, MatchUpdateInput } from './store/index.js';
import { PlayerLookupError, OrganizationLookupError, MatchLookupError } from './store/index.js';
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

const OrganizationUpdate = z
  .object({
    name: z.string().min(1).optional(),
    slug: z.string().min(1).optional(),
    description: z.string().nullable().optional(),
  })
  .refine((data) => data.name !== undefined || data.slug !== undefined || data.description !== undefined, {
    message: 'At least one field is required',
    path: ['name'],
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

app.patch('/v1/organizations/:organization_id', requireAuth, requireScope('organizations:write'), async (req, res) => {
  const parsed = OrganizationUpdate.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
  }

  const organizationId = req.params.organization_id;

  try {
    const existing = await store.getOrganizationById(organizationId);
    if (!existing) {
      return res.status(404).send({ error: 'organization_not_found', message: 'Organization not found' });
    }

    await authorizeOrgAccess(req, organizationId, {
      permissions: ['organizations:write'],
      errorCode: 'organizations_update_denied',
      errorMessage: 'Insufficient grants for organizations:write',
    });

    const updateInput: OrganizationUpdateInput = {};
    if (parsed.data.name !== undefined) updateInput.name = parsed.data.name;
    if (parsed.data.slug !== undefined) updateInput.slug = parsed.data.slug;
    if (parsed.data.description !== undefined) updateInput.description = parsed.data.description;

    const updated = await store.updateOrganization(organizationId, updateInput);

    return res.send({
      organization_id: updated.organizationId,
      name: updated.name,
      slug: updated.slug,
      description: updated.description ?? null,
    });
  } catch (err) {
    if (err instanceof AuthorizationError) {
      return res.status(err.status).send({ error: err.code, message: err.message });
    }
    if (err instanceof OrganizationLookupError) {
      if (err.message.startsWith('Slug already in use')) {
        return res.status(409).send({ error: 'duplicate_slug', message: err.message });
      }
      return res.status(404).send({ error: 'organization_not_found', message: err.message });
    }
    console.error('organization_update_error', err);
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

const PlayerUpdate = z
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
      data.region_id !== undefined,
    {
      message: 'At least one field is required',
      path: ['display_name'],
    }
  );

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

app.patch('/v1/players/:player_id', requireAuth, requireScope('matches:write'), async (req, res) => {
  const parsed = PlayerUpdate.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
  }

  try {
    const organization = await resolveOrganizationIdentifier({
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

    const updated = await store.updatePlayer(req.params.player_id, organization.organizationId, updateInput);

    return res.send({
      player_id: updated.playerId,
      organization_id: updated.organizationId,
      organization_slug: organization.slug,
      display_name: updated.displayName,
      short_name: updated.shortName ?? null,
      native_name: updated.nativeName ?? null,
      given_name: updated.givenName ?? null,
      family_name: updated.familyName ?? null,
      sex: updated.sex ?? null,
      birth_year: updated.birthYear ?? null,
      country_code: updated.countryCode ?? null,
      region_id: updated.regionId ?? null,
      external_ref: updated.externalRef ?? null,
    });
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

const MatchUpdate = z
  .object({
    organization_id: z.string().uuid().optional(),
    organization_slug: z.string().optional(),
    start_time: z.string().optional(),
    venue_id: z.string().nullable().optional(),
    venue_region_id: z.string().nullable().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  })
  .refine(
    (data) =>
      data.start_time !== undefined ||
      data.venue_id !== undefined ||
      data.venue_region_id !== undefined,
    {
      message: 'At least one field is required',
      path: ['start_time'],
    }
  );

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

app.patch('/v1/matches/:match_id', requireAuth, requireScope('matches:write'), async (req, res) => {
  const parsed = MatchUpdate.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
  }

  try {
    const organization = await resolveOrganizationIdentifier({
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
        ? normalizeRegion(null)
        : normalizeRegion(parsed.data.venue_region_id);
    }

    const updated = await store.updateMatch(req.params.match_id, organization.organizationId, updateInput);

    return res.send({
      match_id: updated.matchId,
      organization_id: updated.organizationId,
      organization_slug: organization.slug,
      sport: updated.sport,
      discipline: updated.discipline,
      format: updated.format,
      tier: updated.tier,
      start_time: updated.startTime,
      venue_id: updated.venueId,
      region_id: updated.regionId,
      sides: updated.sides.reduce((acc, side) => {
        acc[side.side] = { players: side.players };
        return acc;
      }, {} as Record<'A' | 'B', { players: string[] }>),
      games: updated.games.map((game) => ({ game_no: game.gameNo, a: game.a, b: game.b })),
    });
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
