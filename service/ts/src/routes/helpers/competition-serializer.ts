import type { CompetitionRecord, EventRecord } from '../../store/index.js';

export const toCompetitionResponse = (competition: CompetitionRecord, event: EventRecord) => ({
  competition_id: competition.competitionId,
  event_id: competition.eventId,
  event_slug: event.slug,
  organization_id: event.organizationId,
  name: competition.name,
  slug: competition.slug,
  sport: competition.sport ?? null,
  discipline: competition.discipline ?? null,
  format: competition.format ?? null,
  tier: competition.tier ?? null,
  status: competition.status ?? null,
  draw_size: competition.drawSize ?? null,
  start_date: competition.startDate ?? null,
  end_date: competition.endDate ?? null,
  metadata: competition.metadata ?? null,
  created_at: competition.createdAt ?? null,
  updated_at: competition.updatedAt ?? null,
});
