type RatingEventRow = {
  id: number;
  playerId: string;
  ladderId: string;
  matchId: string | null;
  createdAt: Date;
  muBefore: number;
  muAfter: number;
  delta: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  winProbPre: number | null;
  movWeight: number | null;
  organizationId: string;
};

type PlayerInsightEventRow = {
  id: number;
  createdAt: Date;
  sport: string;
  discipline: string;
  muBefore: number;
  muAfter: number;
  sigmaBefore: number | null;
  sigmaAfter: number;
  delta: number;
  winProbPre: number | null;
  matchId: string | null;
};

type PlayerInsightRatingRow = {
  sport: string;
  discipline: string;
  mu: number;
  sigma: number;
  matchesCount: number;
  updatedAt: Date;
};

type PlayerLeaderboardRow = {
  playerId: string;
  mu: number;
  sigma: number;
  matchesCount: number;
  displayName: string;
  shortName: string | null;
  givenName: string | null;
  familyName: string | null;
  countryCode: string | null;
  playerRegionId: string | null;
};

type EventRow = {
  eventId: string;
  organizationId: string;
  providerId: string | null;
  externalRef: string | null;
  type: string;
  name: string;
  slug: string;
  description: string | null;
  startDate: Date | null;
  endDate: Date | null;
  sanctioningBody: string | null;
  season: string | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
};

type CompetitionRow = {
  competitionId: string;
  eventId: string;
  organizationId: string;
  providerId: string | null;
  externalRef: string | null;
  name: string;
  slug: string;
  sport: string | null;
  discipline: string | null;
  format: string | null;
  tier: string | null;
  status: string | null;
  drawSize: number | null;
  startDate: Date | null;
  endDate: Date | null;
  classification: unknown | null;
  purse: number | null;
  purseCurrency: string | null;
  mediaLinks: unknown | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
};

type CompetitionParticipantRow = {
  competitionId: string;
  playerId: string;
  seed: number | null;
  status: string | null;
  metadata: unknown | null;
  createdAt: Date;
  updatedAt: Date;
};

export type {
  RatingEventRow,
  PlayerInsightEventRow,
  PlayerInsightRatingRow,
  PlayerLeaderboardRow,
  EventRow,
  CompetitionRow,
  CompetitionParticipantRow,
};
