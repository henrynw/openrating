import type { RatingStore, OrganizationRecord } from '../../store/index.js';
import { OrganizationLookupError } from '../../store/index.js';

const normalizeSlug = (value: string) => value.trim().toLowerCase();

export interface OrganizationIdentifierInput {
  organization_id?: string;
  organization_slug?: string;
}

export const createOrganizationResolver = (store: RatingStore) =>
  async ({ organization_id, organization_slug }: OrganizationIdentifierInput): Promise<OrganizationRecord> => {
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
  };
