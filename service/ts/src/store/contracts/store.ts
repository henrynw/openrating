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
import type { RatingEventListQuery, RatingEventListResult, RatingEventRecord, RatingSnapshot } from './ratings.js';
import type { PlayerState } from '../../engine/types.js';

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
  createOrganization(input: OrganizationCreateInput): Promise<OrganizationRecord>;
  updateOrganization(organizationId: string, input: OrganizationUpdateInput): Promise<OrganizationRecord>;
  listOrganizations(query: OrganizationListQuery): Promise<OrganizationListResult>;
  getOrganizationBySlug(slug: string): Promise<OrganizationRecord | null>;
  getOrganizationById(id: string): Promise<OrganizationRecord | null>;
  runNightlyStabilization(options?: NightlyStabilizationOptions): Promise<void>;
}
