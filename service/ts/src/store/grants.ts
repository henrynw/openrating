import { eq } from 'drizzle-orm';
import { getDb } from '../db/client.js';
import { subjectGrants, subjects } from '../db/schema.js';

export interface SubjectGrant {
  subjectId: string;
  organizationId: string;
  sport: string | null;
  regionId: string | null;
  permission: string;
}

const AUTH_PROVIDER = process.env.AUTH_PROVIDER ?? 'AUTH0';

export async function ensureSubject(subjectId: string, displayName?: string | null) {
  const db = getDb();
  await db
    .insert(subjects)
    .values({
      subjectId,
      authProvider: AUTH_PROVIDER,
      displayName: displayName ?? null,
      createdAt: new Date(),
    })
    .onConflictDoNothing({ target: subjects.subjectId });
}

export async function loadGrants(subjectId: string): Promise<SubjectGrant[]> {
  const db = getDb();
  const rows = await db
    .select({
      subjectId: subjectGrants.subjectId,
      organizationId: subjectGrants.organizationId,
      sport: subjectGrants.sport,
      regionId: subjectGrants.regionId,
      permission: subjectGrants.permission,
    })
    .from(subjectGrants)
    .where(eq(subjectGrants.subjectId, subjectId));

  return rows.map((row: {
    subjectId: string;
    organizationId: string;
    sport: string | null;
    regionId: string | null;
    permission: string;
  }): SubjectGrant => ({
    subjectId: row.subjectId,
    organizationId: row.organizationId,
    sport: row.sport ?? null,
    regionId: row.regionId ?? null,
    permission: row.permission,
  }));
}

export function grantMatches(grants: SubjectGrant[], organizationId: string, sport: string, regionId: string) {
  return grants.some((grant) =>
    grant.permission === 'matches:write' &&
    grant.organizationId === organizationId &&
    (grant.sport === null || grant.sport === sport) &&
    (grant.regionId === null || grant.regionId === regionId)
  );
}

export function grantRatingsRead(grants: SubjectGrant[], organizationId: string, sport: string | null, regionId: string) {
  return grants.some((grant) =>
    grant.permission === 'ratings:read' &&
    grant.organizationId === organizationId &&
    (sport === null || grant.sport === null || grant.sport === sport) &&
    (grant.regionId === null || grant.regionId === regionId)
  );
}
