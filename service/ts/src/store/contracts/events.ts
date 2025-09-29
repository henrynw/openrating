export type EventType =
  | 'TOURNAMENT'
  | 'LEAGUE'
  | 'LADDER'
  | 'BOX_LEAGUE'
  | 'CHAMPIONSHIP'
  | 'SERIES'
  | 'EXHIBITION'
  | 'CUSTOM';

export interface EventRecord {
  eventId: string;
  organizationId: string;
  type: EventType;
  name: string;
  slug: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface EventCreateInput {
  organizationId: string;
  type: EventType;
  name: string;
  slug?: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface EventUpdateInput {
  name?: string;
  type?: EventType;
  slug?: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface EventListQuery {
  organizationId: string;
  types?: EventType[];
  cursor?: string;
  limit?: number;
  q?: string;
}

export interface EventListResult {
  items: EventRecord[];
  nextCursor?: string;
}

export interface EventParticipantRecord {
  eventId: string;
  playerId: string;
  seed?: number | null;
  status?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface EventParticipantUpsertInput {
  eventId: string;
  playerId: string;
  seed?: number | null;
  status?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface EventParticipantListResult {
  items: EventParticipantRecord[];
}
