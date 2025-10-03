import {
  pgTable,
  text,
  timestamp,
  integer,
  jsonb,
  doublePrecision,
  primaryKey,
  serial,
  uniqueIndex,
} from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const organizations = pgTable('organizations', {
  organizationId: text('organization_id').primaryKey(),
  name: text('name').notNull(),
  slug: text('slug').notNull(),
  description: text('description'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const providers = pgTable('providers', {
  providerId: text('provider_id').primaryKey(),
  name: text('name').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const subjects = pgTable('subjects', {
  subjectId: text('subject_id').primaryKey(),
  authProvider: text('auth_provider').notNull(),
  displayName: text('display_name'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

export const subjectGrants = pgTable('subject_grants', {
  id: serial('id').primaryKey(),
  subjectId: text('subject_id').references(() => subjects.subjectId, {
    onDelete: 'cascade',
  }).notNull(),
  organizationId: text('organization_id').notNull(),
  sport: text('sport'),
  regionId: text('region_id'),
  permission: text('permission').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

export const sports = pgTable('sports', {
  sportId: text('sport_id').primaryKey(),
  name: text('name').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const regions = pgTable('regions', {
  regionId: text('region_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }),
  parentRegionId: text('parent_region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
  type: text('type').notNull(),
  name: text('name').notNull(),
  countryCode: text('country_code'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const venues = pgTable('venues', {
  venueId: text('venue_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }),
  regionId: text('region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
  name: text('name').notNull(),
  address: text('address'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const events = pgTable('events', {
  eventId: text('event_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  type: text('type').notNull(),
  name: text('name').notNull(),
  slug: text('slug').notNull(),
  description: text('description'),
  startDate: timestamp('start_date', { withTimezone: true }),
  endDate: timestamp('end_date', { withTimezone: true }),
  sanctioningBody: text('sanctioning_body'),
  season: text('season'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  organizationSlugIdx: uniqueIndex('events_org_slug_idx').on(table.organizationId, table.slug),
}));

export const competitions = pgTable('competitions', {
  competitionId: text('competition_id').primaryKey(),
  eventId: text('event_id').references(() => events.eventId, {
    onDelete: 'cascade',
  }).notNull(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  name: text('name').notNull(),
  slug: text('slug').notNull(),
  sport: text('sport'),
  discipline: text('discipline'),
  format: text('format'),
  tier: text('tier'),
  status: text('status'),
  drawSize: integer('draw_size'),
  startDate: timestamp('start_date', { withTimezone: true }),
  endDate: timestamp('end_date', { withTimezone: true }),
  classification: jsonb('classification'),
  purse: doublePrecision('purse'),
  purseCurrency: text('purse_currency'),
  mediaLinks: jsonb('media_links'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  eventSlugIdx: uniqueIndex('competitions_event_slug_idx').on(table.eventId, table.slug),
}));

export const players = pgTable('players', {
  playerId: text('player_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  displayName: text('display_name').notNull(),
  shortName: text('short_name'),
  nativeName: text('native_name'),
  externalRef: text('external_ref'),
  givenName: text('given_name'),
  familyName: text('family_name'),
  sex: text('sex'),
  birthYear: integer('birth_year'),
  countryCode: text('country_code'),
  regionId: text('region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
  competitiveProfile: jsonb('competitive_profile'),
  attributes: jsonb('attributes'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const ratingLadders = pgTable('rating_ladders', {
  ladderId: text('ladder_id').primaryKey(),
  sport: text('sport').references(() => sports.sportId, {
    onDelete: 'restrict',
  }).notNull(),
  discipline: text('discipline').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const matches = pgTable('matches', {
  matchId: text('match_id').primaryKey(),
  ladderId: text('ladder_id').references(() => ratingLadders.ladderId, {
    onDelete: 'cascade',
  }).notNull(),
  providerId: text('provider_id').references(() => providers.providerId, {
    onDelete: 'restrict',
  }).notNull(),
  externalRef: text('external_ref'),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  sport: text('sport').references(() => sports.sportId, {
    onDelete: 'restrict',
  }).notNull(),
  discipline: text('discipline').notNull(),
  format: text('format').notNull(),
  tier: text('tier').notNull(),
  venueId: text('venue_id').references(() => venues.venueId, {
    onDelete: 'set null',
  }),
  regionId: text('region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
  eventId: text('event_id').references(() => events.eventId, {
    onDelete: 'set null',
  }),
  competitionId: text('competition_id').references(() => competitions.competitionId),
  startTime: timestamp('start_time', { withTimezone: true }).notNull(),
  timing: jsonb('timing'),
  statistics: jsonb('statistics'),
  segments: jsonb('segments'),
  sideParticipants: jsonb('side_participants'),
  rawPayload: jsonb('raw_payload').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  providerRefIdx: uniqueIndex('matches_provider_ref_idx').on(table.providerId, table.externalRef).where(sql`${table.externalRef} IS NOT NULL`),
}));

export const matchSides = pgTable('match_sides', {
  id: serial('id').primaryKey(),
  matchId: text('match_id').references(() => matches.matchId, {
    onDelete: 'cascade',
  }).notNull(),
  side: text('side').notNull(),
  playersCount: integer('players_count').notNull(),
});

export const matchSidePlayers = pgTable('match_side_players', {
  id: serial('id').primaryKey(),
  matchSideId: integer('match_side_id').references(() => matchSides.id, {
    onDelete: 'cascade',
  }).notNull(),
  playerId: text('player_id').references(() => players.playerId, {
    onDelete: 'restrict',
  }).notNull(),
  position: integer('position').notNull(),
});

export const matchGames = pgTable('match_games', {
  id: serial('id').primaryKey(),
  matchId: text('match_id').references(() => matches.matchId, {
    onDelete: 'cascade',
  }).notNull(),
  gameNo: integer('game_no').notNull(),
  scoreA: integer('score_a').notNull(),
  scoreB: integer('score_b').notNull(),
  statistics: jsonb('statistics'),
  segments: jsonb('segments'),
});

export const playerRatings = pgTable('player_ratings', {
  playerId: text('player_id').references(() => players.playerId, {
    onDelete: 'cascade',
  }).notNull(),
  ladderId: text('ladder_id').references(() => ratingLadders.ladderId, {
    onDelete: 'cascade',
  }).notNull(),
  mu: doublePrecision('mu').notNull(),
  sigma: doublePrecision('sigma').notNull(),
  matchesCount: integer('matches_count').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  pk: primaryKey(table.playerId, table.ladderId),
}));

export const playerRatingHistory = pgTable('player_rating_history', {
  id: serial('id').primaryKey(),
  playerId: text('player_id').references(() => players.playerId, {
    onDelete: 'cascade',
  }).notNull(),
  ladderId: text('ladder_id').references(() => ratingLadders.ladderId, {
    onDelete: 'cascade',
  }).notNull(),
  matchId: text('match_id').references(() => matches.matchId, {
    onDelete: 'cascade',
  }).notNull(),
  muBefore: doublePrecision('mu_before').notNull(),
  muAfter: doublePrecision('mu_after').notNull(),
  sigmaBefore: doublePrecision('sigma_before'),
  sigmaAfter: doublePrecision('sigma_after').notNull(),
  delta: doublePrecision('delta').notNull(),
  winProbPre: doublePrecision('win_prob_pre'),
  movWeight: doublePrecision('mov_weight'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

export const competitionParticipants = pgTable('competition_participants', {
  competitionId: text('competition_id').references(() => competitions.competitionId, {
    onDelete: 'cascade',
  }).notNull(),
  playerId: text('player_id').references(() => players.playerId, {
    onDelete: 'cascade',
  }).notNull(),
  seed: integer('seed'),
  status: text('status'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  pk: primaryKey(table.competitionId, table.playerId),
}));

export const pairSynergies = pgTable('pair_synergies', {
  ladderId: text('ladder_id').references(() => ratingLadders.ladderId, {
    onDelete: 'cascade',
  }).notNull(),
  pairKey: text('pair_key').notNull(),
  players: jsonb('players').notNull(),
  gamma: doublePrecision('gamma').default(0).notNull(),
  matches: integer('matches').default(0).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table: any) => ({
  pk: primaryKey(table.ladderId, table.pairKey),
}));

export const pairSynergyHistory = pgTable('pair_synergy_history', {
  id: serial('id').primaryKey(),
  ladderId: text('ladder_id').references(() => ratingLadders.ladderId, {
    onDelete: 'cascade',
  }).notNull(),
  pairKey: text('pair_key').notNull(),
  matchId: text('match_id').references(() => matches.matchId, {
    onDelete: 'cascade',
  }).notNull(),
  gammaBefore: doublePrecision('gamma_before').notNull(),
  gammaAfter: doublePrecision('gamma_after').notNull(),
  delta: doublePrecision('delta').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});
