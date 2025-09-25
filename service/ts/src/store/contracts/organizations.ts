export interface OrganizationCreateInput {
  name: string;
  slug?: string;
  description?: string;
}

export interface OrganizationUpdateInput {
  name?: string;
  slug?: string;
  description?: string | null;
}

export interface OrganizationRecord {
  organizationId: string;
  name: string;
  slug: string;
  description?: string | null;
  createdAt?: string;
}

export interface OrganizationListQuery {
  cursor?: string;
  limit?: number;
  q?: string;
}

export interface OrganizationListResult {
  items: OrganizationRecord[];
  nextCursor?: string;
}

export type OrganizationIdentifier = { id?: string; slug?: string };
