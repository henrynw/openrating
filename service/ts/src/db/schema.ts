import {
  pgTable,
  text,
  timestamp,
  integer,
  jsonb,
  doublePrecision,
  primaryKey,
  uniqueIndex,
} from 'drizzle-orm/pg-core';

export const players = pgTable('players', {
  playerId: text('player_id').primaryKey(),
  organizationId: text('organization_id').notNull(),
  externalRef: text('external_ref'),
  givenName: text('given_name'),
  familyName: text('family_name'),
  sex: text('sex'),
  birthYear: integer('birth_year'),
  countryCode: text('country_code'),
  regionId: text('region_id'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true })
    .defaultNow()
    .notNull(),
});

export const ratingLadders = pgTable(
  'rating_ladders',
  {
    ladderId: text('ladder_id').primaryKey(),
    organizationId: text('organization_id').notNull(),
    sport: text('sport').notNull(),
    discipline: text('discipline').notNull(),
    format: text('format').notNull(),
    tier: text('tier').notNull().default('UNSPECIFIED'),
    regionId: text('region_id').notNull().default('GLOBAL'),
    createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (table: any) => ({
    uniq: uniqueIndex('rating_ladders_org_format_idx').on(
      table.organizationId,
      table.sport,
      table.discipline,
      table.format,
      table.tier,
      table.regionId
    ),
  })
);

export const matches = pgTable('matches', {
  matchId: text('match_id').primaryKey(),
  ladderId: text('ladder_id')
    .notNull()
    .references(() => ratingLadders.ladderId, { onDelete: 'cascade' }),
  providerId: text('provider_id').notNull(),
  organizationId: text('organization_id').notNull(),
  sport: text('sport').notNull(),
  discipline: text('discipline').notNull(),
  format: text('format').notNull(),
  tier: text('tier').notNull(),
  startTime: timestamp('start_time', { withTimezone: true }).notNull(),
  rawPayload: jsonb('raw_payload').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});

export const matchSides = pgTable(
  'match_sides',
  {
    id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
    matchId: text('match_id')
      .notNull()
      .references(() => matches.matchId, { onDelete: 'cascade' }),
    side: text('side').notNull(),
    playersCount: integer('players_count').notNull(),
  },
  (table: any) => ({
    uniq: uniqueIndex('match_sides_match_side_idx').on(table.matchId, table.side),
  })
);

export const matchSidePlayers = pgTable('match_side_players', {
  id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
  matchSideId: integer('match_side_id')
    .notNull()
    .references(() => matchSides.id, { onDelete: 'cascade' }),
  playerId: text('player_id')
    .notNull()
    .references(() => players.playerId, { onDelete: 'restrict' }),
  position: integer('position').notNull(),
});

export const matchGames = pgTable(
  'match_games',
  {
    id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
    matchId: text('match_id')
      .notNull()
      .references(() => matches.matchId, { onDelete: 'cascade' }),
    gameNo: integer('game_no').notNull(),
    scoreA: integer('score_a').notNull(),
    scoreB: integer('score_b').notNull(),
  },
  (table: any) => ({
    uniq: uniqueIndex('match_games_match_game_idx').on(table.matchId, table.gameNo),
  })
);

export const playerRatings = pgTable(
  'player_ratings',
  {
    playerId: text('player_id')
      .notNull()
      .references(() => players.playerId, { onDelete: 'cascade' }),
    ladderId: text('ladder_id')
      .notNull()
      .references(() => ratingLadders.ladderId, { onDelete: 'cascade' }),
    mu: doublePrecision('mu').notNull(),
    sigma: doublePrecision('sigma').notNull(),
    matchesCount: integer('matches_count').notNull(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
  },
  (table: any) => ({
    pk: primaryKey(table.playerId, table.ladderId),
  })
);

export const playerRatingHistory = pgTable('player_rating_history', {
  id: integer('id').generatedAlwaysAsIdentity().primaryKey(),
  playerId: text('player_id')
    .notNull()
    .references(() => players.playerId, { onDelete: 'cascade' }),
  ladderId: text('ladder_id')
    .notNull()
    .references(() => ratingLadders.ladderId, { onDelete: 'cascade' }),
  matchId: text('match_id')
    .notNull()
    .references(() => matches.matchId, { onDelete: 'cascade' }),
  muBefore: doublePrecision('mu_before').notNull(),
  muAfter: doublePrecision('mu_after').notNull(),
  sigmaAfter: doublePrecision('sigma_after').notNull(),
  delta: doublePrecision('delta').notNull(),
  winProbPre: doublePrecision('win_prob_pre'),
  movWeight: doublePrecision('mov_weight'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
});
