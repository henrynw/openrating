import type { MatchInput } from '../../engine/types.js';
import type { EventClassification, EventMediaLinks } from './events.js';

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
  classification?: EventClassification | null;
  purse?: number | null;
  purseCurrency?: string | null;
  mediaLinks?: EventMediaLinks | null;
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
  classification?: EventClassification | null;
  purse?: number | null;
  purseCurrency?: string | null;
  mediaLinks?: EventMediaLinks | null;
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
  classification?: EventClassification | null;
  purse?: number | null;
  purseCurrency?: string | null;
  mediaLinks?: EventMediaLinks | null;
  metadata?: Record<string, unknown> | null;
}

export interface CompetitionListQuery {
  eventId: string;
}

export interface CompetitionListResult {
  items: CompetitionRecord[];
}

export interface CompetitionParticipantRecord {
  competitionId: string;
  playerId: string;
  seed?: number | null;
  status?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface CompetitionParticipantUpsertInput {
  competitionId: string;
  playerId: string;
  seed?: number | null;
  status?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface CompetitionParticipantListResult {
  items: CompetitionParticipantRecord[];
}
