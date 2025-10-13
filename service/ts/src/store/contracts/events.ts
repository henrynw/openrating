export type EventType =
  | 'TOURNAMENT'
  | 'LEAGUE'
  | 'LADDER'
  | 'BOX_LEAGUE'
  | 'CHAMPIONSHIP'
  | 'SERIES'
  | 'EXHIBITION'
  | 'CUSTOM';

export type EventLevel =
  | 'WORLD_TOUR'
  | 'CONTINENTAL'
  | 'NATIONAL'
  | 'REGIONAL'
  | 'CLUB'
  | 'SCHOOL'
  | 'COMMUNITY'
  | 'OTHER';

export type EventGradeFamily =
  | 'BWF'
  | 'ATP'
  | 'WTA'
  | 'ITF'
  | 'PSA'
  | 'PICKLEBALL_TOUR'
  | 'NATIONAL_FEDERATION'
  | 'OTHER';

export interface EventGrade {
  family: EventGradeFamily;
  code: string;
  name?: string | null;
}

export type EventAgeGroup =
  | 'U11'
  | 'U13'
  | 'U15'
  | 'U17'
  | 'U19'
  | 'U21'
  | 'SENIOR'
  | 'ADULT'
  | 'VETERAN'
  | 'MASTER'
  | 'OPEN'
  | 'OTHER';

export interface EventClassification {
  level?: EventLevel | null;
  grade?: EventGrade | null;
  ageGroup?: EventAgeGroup | null;
  tour?: string | null;
  category?: string | null;
}

export interface EventMediaLinks {
  website?: string | null;
  registration?: string | null;
  liveScoring?: string | null;
  streaming?: string | null;
  social?: Record<string, string> | null;
}

export interface EventRecord {
  eventId: string;
  organizationId: string;
  providerId?: string | null;
  externalRef?: string | null;
  type: EventType;
  name: string;
  slug: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  sanctioningBody?: string | null;
  season?: string | null;
  metadata?: Record<string, unknown> | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface EventCreateInput {
  organizationId: string;
  providerId: string;
  externalRef?: string | null;
  type: EventType;
  name: string;
  slug?: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  sanctioningBody?: string | null;
  season?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface EventUpdateInput {
  name?: string;
  type?: EventType;
  slug?: string;
  description?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  sanctioningBody?: string | null;
  season?: string | null;
  metadata?: Record<string, unknown> | null;
}

export type EventScheduleStatus = 'UPCOMING' | 'IN_PROGRESS' | 'COMPLETED';

export type EventListSortField = 'start_date' | 'created_at' | 'name' | 'slug';

export type EventListSortDirection = 'asc' | 'desc';

export type EventListInclude = 'competitions';

export interface EventListQuery {
  organizationId: string;
  types?: EventType[];
  sport?: string | null;
  discipline?: string | null;
  statuses?: EventScheduleStatus[];
  season?: string | null;
  sanctioningBody?: string | null;
  startDateFrom?: string | null;
  startDateTo?: string | null;
  endDateFrom?: string | null;
  endDateTo?: string | null;
  cursor?: string;
  limit?: number;
  q?: string;
  sortField?: EventListSortField;
  sortDirection?: EventListSortDirection;
  includes?: EventListInclude[];
}

export interface EventListResult {
  items: EventRecord[];
  nextCursor?: string;
}
