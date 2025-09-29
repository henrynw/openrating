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
  EventParticipantUpsertInput,
  EventParticipantRecord,
  EventParticipantListResult,
} from './events.js';

export interface RatingStore {
  createPlayer(input: PlayerCreateInput): Promise<PlayerRecord>;
  updatePlayer(playerId: string, organizationId: string, input: PlayerUpdateInput): Promise<PlayerRecord>;
  ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult>;
  ensurePairSynergies(params: EnsurePairSynergiesParams): Promise<EnsurePairSynergiesResult>;
  recordMatch(params: RecordMatchParams): Promise<RecordMatchResult>;
  updateMatch(matchId: string, organizationId: string, input: MatchUpdateInput): Promise<MatchSummary>;
  getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null>;
  listPlayers(query: PlayerListQuery): Promise<PlayerListResult>;
  listMatches(query: MatchListQuery): Promise<MatchListResult>;
  listRatingEvents(query: RatingEventListQuery): Promise<RatingEventListResult>;
  getRatingEvent(
    identifiers: { ladderKey: LadderKey; playerId: string; ratingEventId: string }
  ): Promise<RatingEventRecord | null>;
  getRatingSnapshot(
    params: { playerId: string; ladderKey: LadderKey; asOf?: string }
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
  upsertEventParticipant(input: EventParticipantUpsertInput): Promise<EventParticipantRecord>;
  listEventParticipants(eventId: string): Promise<EventParticipantListResult>;
  ensureEventParticipants(eventId: string, playerIds: string[]): Promise<void>;
  runNightlyStabilization(options?: NightlyStabilizationOptions): Promise<void>;
}
