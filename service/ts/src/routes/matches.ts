import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  enforceMatchWrite,
  hasScope,
  requireAuth,
  requireScope,
} from '../auth.js';
import { updateMatch } from '../engine/rating.js';
import type { MatchInput } from '../engine/types.js';
import type { LadderKey, MatchUpdateInput, RatingStore, OrganizationRecord } from '../store/index.js';
import { MatchLookupError, OrganizationLookupError, PlayerLookupError } from '../store/index.js';
import { normalizeMatchSubmission } from '../formats/index.js';
import { normalizeRegion, normalizeTier, isDefaultRegion } from '../store/helpers.js';
import type { OrganizationIdentifierInput } from './helpers/organization-resolver.js';
import { toMatchSummaryResponse } from './helpers/responders.js';
import { buildPairKey, sortPairPlayers } from '../store/helpers.js';
import type { PairState } from '../engine/types.js';

const MatchSubmitSchema = z
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
    event_id: z.string().uuid().optional(),
    sides: z.object({
      A: z.object({ players: z.array(z.string()).min(1).max(4) }),
      B: z.object({ players: z.array(z.string()).min(1).max(4) }),
    }),
    games: z.array(z.object({ game_no: z.number().int(), a: z.number().int(), b: z.number().int() })),
    winner: z.enum(['A', 'B']).optional(),
    mov_weight: z.number().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const MatchListQuerySchema = z
  .object({
    organization_id: z.string().optional(),
    organization_slug: z.string().optional(),
    sport: z.string().optional(),
    player_id: z.string().optional(),
    event_id: z.string().optional(),
    cursor: z.string().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional(),
    start_after: z.string().datetime().optional(),
    start_before: z.string().datetime().optional(),
  })
  .refine((data) => data.organization_id || data.organization_slug, {
    message: 'organization_id or organization_slug is required',
    path: ['organization_id'],
  });

const MatchUpdateSchema = z.object({
  organization_id: z.string().uuid().optional(),
  organization_slug: z.string().optional(),
  start_time: z.string().datetime().optional(),
  venue_id: z.string().nullable().optional(),
  venue_region_id: z.string().nullable().optional(),
  event_id: z.string().uuid().nullable().optional(),
});

interface MatchRouteDeps {
  store: RatingStore;
  resolveOrganization: (input: OrganizationIdentifierInput) => Promise<OrganizationRecord>;
}

const buildLadderKey = (
  organizationId: string,
  match: MatchInput,
  options: { tier?: string; regionId?: string | null }
): LadderKey => ({
  organizationId,
  sport: match.sport,
  discipline: match.discipline,
  format: match.format,
  tier: normalizeTier(options.tier),
  regionId: normalizeRegion(options.regionId ?? null),
});

export const registerMatchRoutes = (app: Express, deps: MatchRouteDeps) => {
  const { store, resolveOrganization } = deps;

  app.post('/v1/matches', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = MatchSubmitSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

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

    try {
      const organization = await resolveOrganization({
        organization_id: parsed.data.organization_id,
        organization_slug: parsed.data.organization_slug,
      });

      const ladderKey = buildLadderKey(organization.organizationId, normalization.match, {
        tier: parsed.data.tier,
        regionId: parsed.data.venue_region_id ?? null,
      });

      const uniquePlayerIds = Array.from(
        new Set([
          ...normalization.match.sides.A.players,
          ...normalization.match.sides.B.players,
        ])
      );

      await enforceMatchWrite(req, {
        organizationId: organization.organizationId,
        sport: normalization.match.sport,
        regionId: ladderKey.regionId,
      });

      const { ladderId, players } = await store.ensurePlayers(uniquePlayerIds, ladderKey);

      const pairDescriptors: Array<{ pairId: string; players: string[] }> = [];
      const collectPair = (sidePlayers: string[]) => {
        if (sidePlayers.length < 2) return;
        const sorted = sortPairPlayers(sidePlayers);
        pairDescriptors.push({ pairId: buildPairKey(sorted), players: sorted });
      };

      collectPair(normalization.match.sides.A.players);
      collectPair(normalization.match.sides.B.players);

      let pairStates: Map<string, PairState> = new Map();
      if (pairDescriptors.length) {
        pairStates = await store.ensurePairSynergies({ ladderId, ladderKey, pairs: pairDescriptors });
      }

      const result = updateMatch(normalization.match, {
        getPlayer: (id) => {
          const state = players.get(id);
          if (!state) throw new Error(`missing player state for ${id}`);
          return state;
        },
        getPair: pairDescriptors.length
          ? (sidePlayers) => pairStates.get(buildPairKey(sidePlayers))
          : undefined,
      });

      const { matchId, ratingEvents } = await store.recordMatch({
        ladderId,
        ladderKey,
        match: normalization.match,
        result,
        eventId: parsed.data.event_id ?? null,
        playerStates: players,
        submissionMeta: {
          providerId: parsed.data.provider_id,
          organizationId: organization.organizationId,
          startTime: parsed.data.start_time,
          rawPayload: req.body,
          venueId: parsed.data.venue_id ?? null,
          regionId: parsed.data.venue_region_id ?? null,
        },
        pairUpdates: result.pairUpdates,
      });

      const ratingEventByPlayer = new Map(
        ratingEvents.map((event) => [event.playerId, event.ratingEventId])
      );

      return res.send({
        match_id: matchId,
        organization_id: organization.organizationId,
        organization_slug: organization.slug,
        event_id: parsed.data.event_id ?? null,
        ratings: result.perPlayer.map((p) => {
          const ratingEventId = ratingEventByPlayer.get(p.playerId);
          if (!ratingEventId) {
            throw new Error(`missing rating event for player ${p.playerId} in match ${matchId}`);
          }
          return {
            player_id: p.playerId,
            rating_event_id: ratingEventId,
            mu_before: p.muBefore,
            mu_after: p.muAfter,
            delta: p.delta,
            sigma_after: p.sigmaAfter,
            win_probability_pre: p.winProbPre,
          };
        }),
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

  app.get('/v1/matches', requireAuth, async (req, res) => {
    const parsed = MatchListQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    const { organization_id, organization_slug, sport, player_id, event_id, cursor, limit, start_after, start_before } =
      parsed.data;

    if (!(
      hasScope(req, 'matches:write') ||
      hasScope(req, 'matches:read') ||
      hasScope(req, 'ratings:read')
    )) {
      return res.status(403).send({ error: 'insufficient_scope', required: 'matches:read|matches:write|ratings:read' });
    }

    try {
      const organization = await resolveOrganization({ organization_id, organization_slug });

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
        eventId: event_id ?? undefined,
        cursor,
        limit,
        startAfter: start_after ?? undefined,
        startBefore: start_before ?? undefined,
      });

      return res.send({
        matches: result.items.map((match) => toMatchSummaryResponse(match, organization.slug)),
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

  app.patch('/v1/matches/:match_id', requireAuth, requireScope('matches:write'), async (req, res) => {
    const parsed = MatchUpdateSchema.safeParse(req.body);
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
        errorCode: 'matches_update_denied',
        errorMessage: 'Insufficient grants for matches:write',
      });

      const updateInput: MatchUpdateInput = {};
      if (parsed.data.start_time !== undefined) updateInput.startTime = parsed.data.start_time;
      if (parsed.data.venue_id !== undefined) updateInput.venueId = parsed.data.venue_id;
      if (parsed.data.venue_region_id !== undefined) {
        updateInput.regionId = parsed.data.venue_region_id === null
          ? null
          : normalizeRegion(parsed.data.venue_region_id);
      }
      if (parsed.data.event_id !== undefined) {
        updateInput.eventId = parsed.data.event_id;
      }

      const updated = await store.updateMatch(req.params.match_id, organization.organizationId, updateInput);

      return res.send(toMatchSummaryResponse(updated, organization.slug));
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
};
