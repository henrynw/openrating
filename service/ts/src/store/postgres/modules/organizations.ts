import { randomUUID } from 'crypto';
import { eq, sql } from 'drizzle-orm';

import { organizations } from '../../../db/schema.js';
import type { PostgresStoreContext } from '../context.js';
import { slugify } from '../../util/slug.js';
import { clampLimit } from '../../util/pagination.js';
import { combineFilters, type SqlFilter } from '../sql-helpers.js';
import {
  OrganizationLookupError,
  type OrganizationCreateInput,
  type OrganizationListQuery,
  type OrganizationListResult,
  type OrganizationRecord,
  type OrganizationUpdateInput,
} from '../../types.js';

type OrganizationRow = {
  organizationId: string;
  name: string;
  slug: string;
  description: string | null;
  createdAt: Date | null;
};

const toOrganizationRecord = (row: OrganizationRow): OrganizationRecord => ({
  organizationId: row.organizationId,
  name: row.name,
  slug: row.slug,
  description: row.description,
  createdAt: row.createdAt?.toISOString(),
});

const selectOrganizationRow = () => ({
  organizationId: organizations.organizationId,
  name: organizations.name,
  slug: organizations.slug,
  description: organizations.description,
  createdAt: organizations.createdAt,
});

const getOrganizationRowById = async (ctx: PostgresStoreContext, id: string): Promise<OrganizationRow | null> => {
  const rows = await ctx.db
    .select(selectOrganizationRow())
    .from(organizations)
    .where(eq(organizations.organizationId, id))
    .limit(1);

  return (rows as OrganizationRow[]).at(0) ?? null;
};

const getOrganizationRowBySlug = async (ctx: PostgresStoreContext, slug: string): Promise<OrganizationRow | null> => {
  const rows = await ctx.db
    .select(selectOrganizationRow())
    .from(organizations)
    .where(eq(organizations.slug, slug))
    .limit(1);

  return (rows as OrganizationRow[]).at(0) ?? null;
};

const create = (ctx: PostgresStoreContext) =>
  async (input: OrganizationCreateInput): Promise<OrganizationRecord> => {
    const organizationId = randomUUID();
    const candidateSlug = (input.slug ?? slugify(input.name)).toLowerCase();

    try {
      const [row] = await ctx.db
        .insert(organizations)
        .values({
          organizationId,
          name: input.name,
          slug: candidateSlug,
          description: input.description ?? null,
          createdAt: ctx.now(),
          updatedAt: ctx.now(),
        })
        .returning(selectOrganizationRow());

      return toOrganizationRecord(row as OrganizationRow);
    } catch (err: any) {
      if (err && typeof err === 'object' && 'code' in err && err.code === '23505') {
        throw new OrganizationLookupError(`Slug already in use: ${candidateSlug}`);
      }
      throw err;
    }
  };

const update = (ctx: PostgresStoreContext) =>
  async (organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord> => {
    const updates: Record<string, unknown> = {};

    if (input.name !== undefined) updates.name = input.name;
    if (input.description !== undefined) updates.description = input.description ?? null;
    if (input.slug !== undefined) updates.slug = input.slug.toLowerCase();

    if (!Object.keys(updates).length) {
      const existing = await getOrganizationRowById(ctx, organizationId);
      if (!existing) {
        throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
      }
      return toOrganizationRecord(existing);
    }

    updates.updatedAt = ctx.now();

    try {
      const [row] = await ctx.db
        .update(organizations)
        .set(updates)
        .where(eq(organizations.organizationId, organizationId))
        .returning(selectOrganizationRow());

      if (!row) {
        throw new OrganizationLookupError(`Organization not found: ${organizationId}`);
      }

      return toOrganizationRecord(row as OrganizationRow);
    } catch (err: any) {
      if (err && typeof err === 'object' && 'code' in err && err.code === '23505') {
        const slug = input.slug?.toLowerCase();
        throw new OrganizationLookupError(`Slug already in use: ${slug}`);
      }
      throw err;
    }
  };

const list = (ctx: PostgresStoreContext) =>
  async (query: OrganizationListQuery): Promise<OrganizationListResult> => {
    const limit = clampLimit(query.limit);
    const filters: SqlFilter[] = [];

    if (query.cursor) {
      filters.push(sql`${organizations.slug} > ${query.cursor}`);
    }

    if (query.q) {
      filters.push(sql`${organizations.name} ILIKE ${`%${query.q}%`}`);
    }

    const condition = combineFilters(filters);

    let orgQuery = ctx.db
      .select(selectOrganizationRow())
      .from(organizations);

    if (condition) {
      orgQuery = orgQuery.where(condition);
    }

    const rows = (await orgQuery.orderBy(organizations.slug).limit(limit + 1)) as OrganizationRow[];
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const nextCursor = hasMore && page.length ? page[page.length - 1].slug : undefined;

    return {
      items: page.map(toOrganizationRecord),
      nextCursor,
    } satisfies OrganizationListResult;
  };

const getBySlug = (ctx: PostgresStoreContext) =>
  async (slug: string): Promise<OrganizationRecord | null> => {
    const row = await getOrganizationRowBySlug(ctx, slug);
    return row ? toOrganizationRecord(row) : null;
  };

const getById = (ctx: PostgresStoreContext) =>
  async (id: string): Promise<OrganizationRecord | null> => {
    const row = await getOrganizationRowById(ctx, id);
    return row ? toOrganizationRecord(row) : null;
  };

export interface OrganizationsModule {
  createOrganization: ReturnType<typeof create>;
  updateOrganization: ReturnType<typeof update>;
  listOrganizations: ReturnType<typeof list>;
  getOrganizationBySlug: ReturnType<typeof getBySlug>;
  getOrganizationById: ReturnType<typeof getById>;
}

export const createOrganizationsModule = (ctx: PostgresStoreContext): OrganizationsModule => ({
  createOrganization: create(ctx),
  updateOrganization: update(ctx),
  listOrganizations: list(ctx),
  getOrganizationBySlug: getBySlug(ctx),
  getOrganizationById: getById(ctx),
});
