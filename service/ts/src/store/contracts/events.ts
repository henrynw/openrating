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

export type EventGrade =
  | 'SUPER_1000'
  | 'SUPER_750'
  | 'SUPER_500'
  | 'SUPER_300'
  | 'GOLD'
  | 'SILVER'
  | 'BRONZE'
  | 'MAJOR'
  | 'DIVISION_1'
  | 'DIVISION_2'
  | 'OPEN'
  | 'OTHER';

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
