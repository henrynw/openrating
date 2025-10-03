import type { MatchInput } from '../../engine/types.js';

export interface CompetitionRecord {
  competitionId: string;
  eventId: string;
  organizationId: string;
  name: string;
  slug: string;
  sport?: MatchInput['sport'] | null;
  discipline?: MatchInput['discipline'] | null;
  format?: string | null;
  tier?: string | null;
  status?: string | null;
  drawSize?: number | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface CompetitionCreateInput {
  eventId: string;
  organizationId: string;
  name: string;
  slug?: string | null;
  sport?: MatchInput['sport'] | null;
  discipline?: MatchInput['discipline'] | null;
  format?: string | null;
  tier?: string | null;
  status?: string | null;
  drawSize?: number | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface CompetitionUpdateInput {
  name?: string;
  slug?: string | null;
  sport?: MatchInput['sport'] | null;
  discipline?: MatchInput['discipline'] | null;
  format?: string | null;
  tier?: string | null;
  status?: string | null;
  drawSize?: number | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface CompetitionListQuery {
  eventId: string;
}

export interface CompetitionListResult {
  items: CompetitionRecord[];
}
