import {
  pgTable,
  text,
  timestamp,
  integer,
  jsonb,
  doublePrecision,
  primaryKey,
  serial,
} from 'drizzle-orm/pg-core';

export const organizations = pgTable('organizations', {
  organizationId: text('organization_id').primaryKey(),
  name: text('name').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const providers = pgTable('providers', {
  providerId: text('provider_id').primaryKey(),
  name: text('name').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
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

export const players = pgTable('players', {
  playerId: text('player_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  externalRef: text('external_ref'),
  givenName: text('given_name'),
  familyName: text('family_name'),
  sex: text('sex'),
  birthYear: integer('birth_year'),
  countryCode: text('country_code'),
  regionId: text('region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const ratingLadders = pgTable('rating_ladders', {
  ladderId: text('ladder_id').primaryKey(),
  organizationId: text('organization_id').references(() => organizations.organizationId, {
    onDelete: 'cascade',
  }).notNull(),
  sport: text('sport').references(() => sports.sportId, {
    onDelete: 'restrict',
  }).notNull(),
  discipline: text('discipline').notNull(),
  format: text('format').notNull(),
  tier: text('tier').notNull().default('UNSPECIFIED'),
  regionId: text('region_id').references(() => regions.regionId, {
    onDelete: 'set null',
  }),
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
  startTime: timestamp('start_time', { withTimezone: true }).notNull(),
  rawPayload: jsonb('raw_payload').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

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
  sigmaAfter: doublePrecision('sigma_after').notNull(),
  delta: doublePrecision('delta').notNull(),
  winProbPre: doublePrecision('win_prob_pre'),
  movWeight: doublePrecision('mov_weight'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});
