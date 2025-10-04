import type { PlayerCreateInput, PlayerListQuery, PlayerListResult, PlayerRecord, PlayerUpdateInput } from './players.js';
import type {
  OrganizationCreateInput,
  OrganizationListQuery,
  OrganizationListResult,
  OrganizationRecord,
  OrganizationUpdateInput,
} from './organizations.js';
import type { MatchListQuery, MatchListResult, MatchSummary, MatchUpdateInput } from './matches.js';
import type {
  LadderKey,
  EnsurePlayersResult,
  EnsurePairSynergiesParams,
  EnsurePairSynergiesResult,
  RecordMatchParams,
  RecordMatchResult,
  NightlyStabilizationOptions,
  RatingReplayOptions,
  RatingReplayReport,
  RatingReplayQueueOptions,
} from './common.js';
import type {
  RatingEventListQuery,
  RatingEventListResult,
  RatingEventRecord,
  RatingSnapshot,
  LeaderboardQuery,
  LeaderboardResult,
  LeaderboardMoversQuery,
  LeaderboardMoversResult,
} from './ratings.js';
import type { PlayerState } from '../../engine/types.js';
import type {
  EventCreateInput,
  EventUpdateInput,
  EventRecord,
  EventListQuery,
  EventListResult,
} from './events.js';
import type {
  CompetitionCreateInput,
  CompetitionUpdateInput,
  CompetitionRecord,
  CompetitionListQuery,
  CompetitionListResult,
  CompetitionParticipantUpsertInput,
  CompetitionParticipantRecord,
  CompetitionParticipantListResult,
} from './competitions.js';
import type {
  PlayerInsightsSnapshot,
  PlayerInsightsQuery,
  PlayerInsightsEnqueueInput,
  PlayerInsightsJob,
  PlayerInsightsJobClaimOptions,
  PlayerInsightsJobCompletion,
  PlayerInsightsUpsertResult,
  PlayerInsightsBuildOptions,
} from './insights.js';

export interface RatingStore {
  createPlayer(input: PlayerCreateInput): Promise<PlayerRecord>;
  updatePlayer(playerId: string, organizationId: string, input: PlayerUpdateInput): Promise<PlayerRecord>;
  getPlayer(playerId: string, organizationId: string): Promise<PlayerRecord | null>;
  ensurePlayers(ids: string[], ladderKey: LadderKey, options: { organizationId: string }): Promise<EnsurePlayersResult>;
  ensurePairSynergies(params: EnsurePairSynergiesParams): Promise<EnsurePairSynergiesResult>;
  recordMatch(params: RecordMatchParams): Promise<RecordMatchResult>;
  updateMatch(matchId: string, organizationId: string, input: MatchUpdateInput): Promise<MatchSummary>;
  getMatch(matchId: string, organizationId: string): Promise<MatchSummary | null>;
  getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null>;
  listPlayers(query: PlayerListQuery): Promise<PlayerListResult>;
  listMatches(query: MatchListQuery): Promise<MatchListResult>;
  listRatingEvents(query: RatingEventListQuery): Promise<RatingEventListResult>;
  getRatingEvent(
    identifiers: { ladderKey: LadderKey; playerId: string; ratingEventId: string; organizationId?: string | null }
  ): Promise<RatingEventRecord | null>;
  getRatingSnapshot(
    params: { playerId: string; ladderKey: LadderKey; asOf?: string; organizationId?: string | null }
  ): Promise<RatingSnapshot | null>;
  listLeaderboard(params: LeaderboardQuery): Promise<LeaderboardResult>;
  listLeaderboardMovers(params: LeaderboardMoversQuery): Promise<LeaderboardMoversResult>;
  createOrganization(input: OrganizationCreateInput): Promise<OrganizationRecord>;
  updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord>;
  listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult>;
  getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null>;
  getOrganizationById(id: string): Promise<OrganizationRecord | null>;
  createEvent(input: EventCreateInput): Promise<EventRecord>;
  updateEvent(eventId: string, input: EventUpdateInput): Promise<EventRecord>;
  listEvents(query: EventListQuery): Promise<EventListResult>;
  getEventById(eventId: string): Promise<EventRecord | null>;
  getEventBySlug(organizationId: string, slug: string): Promise<EventRecord | null>;
  createCompetition(input: CompetitionCreateInput): Promise<CompetitionRecord>;
  updateCompetition(competitionId: string, input: CompetitionUpdateInput): Promise<CompetitionRecord>;
  listCompetitions(query: CompetitionListQuery): Promise<CompetitionListResult>;
  getCompetitionById(competitionId: string): Promise<CompetitionRecord | null>;
  getCompetitionBySlug(eventId: string, slug: string): Promise<CompetitionRecord | null>;
  upsertCompetitionParticipant(input: CompetitionParticipantUpsertInput): Promise<CompetitionParticipantRecord>;
  listCompetitionParticipants(competitionId: string): Promise<CompetitionParticipantListResult>;
  ensureCompetitionParticipants(competitionId: string, playerIds: string[]): Promise<void>;
  runNightlyStabilization(options?: NightlyStabilizationOptions): Promise<void>;
  processRatingReplayQueue(options?: RatingReplayQueueOptions): Promise<RatingReplayReport>;
  replayRatings(options: RatingReplayOptions): Promise<RatingReplayReport>;
  getPlayerInsights(query: PlayerInsightsQuery): Promise<PlayerInsightsSnapshot | null>;
  buildPlayerInsightsSnapshot(
    query: PlayerInsightsQuery,
    options?: PlayerInsightsBuildOptions
  ): Promise<PlayerInsightsSnapshot>;
  upsertPlayerInsightsSnapshot(
    query: PlayerInsightsQuery,
    snapshot: PlayerInsightsSnapshot
  ): Promise<PlayerInsightsUpsertResult>;
  enqueuePlayerInsightsRefresh(
    input: PlayerInsightsEnqueueInput
  ): Promise<{ jobId: string; enqueued: boolean }>;
  claimPlayerInsightsJob(options: PlayerInsightsJobClaimOptions): Promise<PlayerInsightsJob | null>;
  completePlayerInsightsJob(result: PlayerInsightsJobCompletion): Promise<void>;
}
