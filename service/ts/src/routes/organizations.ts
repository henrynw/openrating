import type { Express } from 'express';
import { z } from 'zod';

import {
  AuthorizationError,
  authorizeOrgAccess,
  hasScope,
  requireAuth,
  requireScope,
} from '../auth.js';
import type { RatingStore, OrganizationUpdateInput } from '../store/index.js';
import { OrganizationLookupError } from '../store/index.js';
import { toOrganizationResponse } from './helpers/responders.js';

const OrganizationCreateSchema = z.object({
  name: z.string().min(1),
  slug: z.string().optional(),
  description: z.string().optional(),
});

const OrganizationListQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional(),
  cursor: z.string().optional(),
  q: z.string().optional(),
});

const OrganizationUpdateSchema = z
  .object({
    name: z.string().min(1).optional(),
    slug: z.string().min(1).optional(),
    description: z.string().nullable().optional(),
  })
  .refine((data) => data.name !== undefined || data.slug !== undefined || data.description !== undefined, {
    message: 'At least one field is required',
    path: ['name'],
  });

export const registerOrganizationRoutes = (app: Express, store: RatingStore) => {
  app.post('/v1/organizations', requireAuth, requireScope('organizations:write'), async (req, res) => {
    const parsed = OrganizationCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const created = await store.createOrganization(parsed.data);
      return res.status(201).send(toOrganizationResponse(created));
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

    const parsed = OrganizationListQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
    }

    try {
      const result = await store.listOrganizations(parsed.data);
      return res.send({
        organizations: result.items.map((org) => toOrganizationResponse(org, { includeCreatedAt: true })),
        next_cursor: result.nextCursor ?? null,
      });
    } catch (err) {
      console.error('organizations_list_error', err);
      return res.status(500).send({ error: 'internal_error' });
    }
  });

  app.patch('/v1/organizations/:organization_id', requireAuth, requireScope('organizations:write'), async (req, res) => {
    const parsed = OrganizationUpdateSchema.safeParse(req.body);
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

      return res.send(toOrganizationResponse(updated));
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
};
