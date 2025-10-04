import type { CompetitionRecord, EventRecord, EventClassification, EventMediaLinks } from '../../store/index.js';

const serializeClassification = (classification?: EventClassification | null) => {
  if (!classification) return undefined;
  return {
    level: classification.level ?? null,
    grade: classification.grade ?? null,
    age_group: classification.ageGroup ?? null,
    tour: classification.tour ?? null,
    category: classification.category ?? null,
  };
};

const serializeMediaLinks = (links?: EventMediaLinks | null) => {
  if (!links) return undefined;
  return {
    website: links.website ?? null,
    registration: links.registration ?? null,
    live_scoring: links.liveScoring ?? null,
    streaming: links.streaming ?? null,
    social: links.social ?? null,
  };
};

export const toCompetitionResponse = (competition: CompetitionRecord, event: EventRecord) => ({
  competition_id: competition.competitionId,
  event_id: competition.eventId,
  event_slug: event.slug,
  organization_id: event.organizationId,
  provider_id: competition.providerId ?? null,
  external_ref: competition.externalRef ?? null,
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
  classification: serializeClassification(competition.classification) ?? null,
  purse: competition.purse ?? null,
  purse_currency: competition.purseCurrency ?? null,
  media_links: serializeMediaLinks(competition.mediaLinks) ?? null,
  metadata: competition.metadata ?? null,
  created_at: competition.createdAt ?? null,
  updated_at: competition.updatedAt ?? null,
});
