import { and, eq } from 'drizzle-orm';

import { getDb } from '../../db/client.js';
import {
  organizations,
  sports,
  providers,
  regions,
  venues,
  ratingLadders,
  players,
} from '../../db/schema.js';
import { OrganizationLookupError, PlayerLookupError } from '../types.js';
import { buildLadderId, isDefaultRegion } from '../helpers.js';
import type { LadderKey } from '../types.js';
import type { LadderAgePolicy, AgeBandDefinition } from '../age.js';

export type DbClient = ReturnType<typeof getDb>;

export interface PostgresStoreContext {
  db: DbClient;
  now: () => Date;
  assertOrganizationExists: (organizationId: string, client?: DbClient) => Promise<void>;
  ensureSport: (sportId: string, client?: DbClient) => Promise<void>;
  ensureProvider: (providerId: string, client?: DbClient) => Promise<void>;
  ensureRegion: (
    regionId: string | null | undefined,
    organizationId: string,
    client?: DbClient
  ) => Promise<string | null>;
  ensureVenue: (
    venueId: string | null | undefined,
    organizationId: string,
    regionId: string | null,
    client?: DbClient
  ) => Promise<string | null>;
  ensureLadder: (key: LadderKey, client?: DbClient) => Promise<string>;
  getLadderAgePolicy: (ladderId: string) => Promise<LadderAgePolicy | null>;
  assertPlayerInOrganization: (playerId: string, organizationId: string, client?: DbClient) => Promise<void>;
}

const now = () => new Date();

const createAssertOrganizationExists = (db: DbClient) =>
  async (organizationId: string, client: DbClient = db) => {
    const rows = await client
      .select({ organizationId: organizations.organizationId })
      .from(organizations)
      .where(eq(organizations.organizationId, organizationId))
      .limit(1);

    if (!rows.length) {
      throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
    }
  };

const createEnsureSport = (db: DbClient, nowFn: () => Date) =>
  async (sportId: string, client: DbClient = db) => {
    await client
      .insert(sports)
      .values({
        sportId,
        name: sportId,
        createdAt: nowFn(),
        updatedAt: nowFn(),
      })
      .onConflictDoNothing({ target: sports.sportId });
  };

const createEnsureProvider = (db: DbClient, nowFn: () => Date) =>
  async (providerId: string, client: DbClient = db) => {
    await client
      .insert(providers)
      .values({
        providerId,
        name: providerId,
        createdAt: nowFn(),
        updatedAt: nowFn(),
      })
      .onConflictDoNothing({ target: providers.providerId });
  };

const createEnsureRegion = (db: DbClient, nowFn: () => Date, assertOrg: PostgresStoreContext['assertOrganizationExists']) =>
  async (
    regionId: string | null | undefined,
    organizationId: string,
    client: DbClient = db
  ): Promise<string | null> => {
    if (!regionId || isDefaultRegion(regionId)) return null;
    await assertOrg(organizationId, client);
    await client
      .insert(regions)
      .values({
        regionId,
        organizationId,
        parentRegionId: null,
        type: 'CUSTOM',
        name: regionId,
        countryCode: null,
        createdAt: nowFn(),
        updatedAt: nowFn(),
      })
      .onConflictDoNothing({ target: regions.regionId });
    return regionId;
  };

const createEnsureVenue = (
  db: DbClient,
  nowFn: () => Date,
  assertOrg: PostgresStoreContext['assertOrganizationExists']
) =>
  async (
    venueId: string | null | undefined,
    organizationId: string,
    regionId: string | null,
    client: DbClient = db
  ): Promise<string | null> => {
    if (!venueId) return null;
    await assertOrg(organizationId, client);
    await client
      .insert(venues)
      .values({
        venueId,
        organizationId,
        regionId,
        name: venueId,
        address: null,
        createdAt: nowFn(),
        updatedAt: nowFn(),
      })
      .onConflictDoNothing({ target: venues.venueId });
    return venueId;
  };

const createEnsureLadder = (
  db: DbClient,
  nowFn: () => Date,
  ensureSport: PostgresStoreContext['ensureSport']
) =>
  async (key: LadderKey, client: DbClient = db): Promise<string> => {
    const ladderId = buildLadderId(key);
    await ensureSport(key.sport, client);

    await client
      .insert(ratingLadders)
      .values({
        ladderId,
        sport: key.sport,
        discipline: key.discipline,
        createdAt: nowFn(),
        updatedAt: nowFn(),
      })
      .onConflictDoNothing({ target: ratingLadders.ladderId });

    return ladderId;
  };

const createGetLadderAgePolicy = (db: DbClient) =>
  async (ladderId: string): Promise<LadderAgePolicy | null> => {
    const rows = await db
      .select({
        cutoff: ratingLadders.defaultAgeCutoff,
        groups: ratingLadders.ageBands,
      })
      .from(ratingLadders)
      .where(eq(ratingLadders.ladderId, ladderId))
      .limit(1);

    const row = rows.at(0);
    if (!row) return null;
    const groups = row.groups as Record<string, AgeBandDefinition> | null;
    return {
      cutoff: row.cutoff ? row.cutoff.toISOString().slice(0, 10) : null,
      groups,
    };
  };

const createAssertPlayerInOrganization = (db: DbClient) =>
  async (playerId: string, organizationId: string, client: DbClient = db) => {
    const rows = await client
      .select({ playerId: players.playerId })
      .from(players)
      .where(and(eq(players.playerId, playerId), eq(players.organizationId, organizationId)))
      .limit(1);

    if (!rows.length) {
      throw new PlayerLookupError(`Player not found in organization: ${playerId}`, {
        missing: [playerId],
      });
    }
  };

export const createPostgresContext = (db: DbClient): PostgresStoreContext => {
  const nowFn = now;
  const assertOrganizationExists = createAssertOrganizationExists(db);
  const ensureSport = createEnsureSport(db, nowFn);
  const ensureProvider = createEnsureProvider(db, nowFn);
  const ensureRegion = createEnsureRegion(db, nowFn, assertOrganizationExists);
  const ensureVenue = createEnsureVenue(db, nowFn, assertOrganizationExists);
  const ensureLadder = createEnsureLadder(db, nowFn, ensureSport);
  const getLadderAgePolicy = createGetLadderAgePolicy(db);
  const assertPlayerInOrganization = createAssertPlayerInOrganization(db);

  return {
    db,
    now: nowFn,
    assertOrganizationExists,
    ensureSport,
    ensureProvider,
    ensureRegion,
    ensureVenue,
    ensureLadder,
    getLadderAgePolicy,
    assertPlayerInOrganization,
  } satisfies PostgresStoreContext;
};
