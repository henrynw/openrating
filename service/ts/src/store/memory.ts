import { randomUUID } from 'crypto';
import { P } from '../engine/params.js';
import type { MatchInput, PlayerState, UpdateResult } from '../engine/types.js';
import type {
  EnsurePlayersResult,
  LadderKey,
  PlayerCreateInput,
  PlayerRecord,
  RatingStore,
  RecordMatchParams,
} from './types.js';
import { PlayerLookupError } from './types.js';
import { buildLadderId } from './helpers.js';

interface MemoryPlayerRecord extends PlayerRecord {
  ratings: Map<string, PlayerState>;
}

export class MemoryStore implements RatingStore {
  private players = new Map<string, MemoryPlayerRecord>();
  private matches: Array<{
    matchId: string;
    match: MatchInput;
    result: UpdateResult;
  }> = [];

  async createPlayer(input: PlayerCreateInput): Promise<PlayerRecord> {
    const playerId = randomUUID();
    const record: MemoryPlayerRecord = {
      playerId,
      organizationId: input.organizationId,
      givenName: input.givenName,
      familyName: input.familyName,
      sex: input.sex,
      birthYear: input.birthYear,
      countryCode: input.countryCode,
      regionId: input.regionId,
      externalRef: input.externalRef,
      ratings: new Map(),
    };
    this.players.set(playerId, record);
    return record;
  }

  async ensurePlayers(ids: string[], ladderKey: LadderKey): Promise<EnsurePlayersResult> {
    const ladderId = buildLadderId(ladderKey);
    const playersMap = new Map<string, PlayerState>();

    const missing: string[] = [];
    const wrongOrg: string[] = [];

    for (const id of ids) {
      const player = this.players.get(id);
      if (!player) {
        missing.push(id);
        continue;
      }
      if (player.organizationId !== ladderKey.organizationId) {
        wrongOrg.push(id);
        continue;
      }

      let rating = player.ratings.get(ladderId);
      if (!rating) {
        rating = {
          playerId: id,
          mu: P.baseMu,
          sigma: P.baseSigma,
          matchesCount: 0,
        };
        player.ratings.set(ladderId, rating);
      }
      playersMap.set(id, rating);
    }

    if (missing.length || wrongOrg.length) {
      throw new PlayerLookupError(
        missing.length
          ? `Players not found: ${missing.join(', ')}`
          : `Players not registered to organization ${ladderKey.organizationId}: ${wrongOrg.join(', ')}`,
        { missing: missing.length ? missing : undefined, wrongOrganization: wrongOrg.length ? wrongOrg : undefined }
      );
    }

    return { ladderId, players: playersMap };
  }

  async recordMatch(params: RecordMatchParams): Promise<{ matchId: string }> {
    const matchId = randomUUID();
    this.matches.push({ matchId, match: params.match, result: params.result });
    return { matchId };
  }

  async getPlayerRating(playerId: string, ladderKey: LadderKey): Promise<PlayerState | null> {
    const ladderId = buildLadderId(ladderKey);
    const player = this.players.get(playerId);
    if (!player) return null;
    return player.ratings.get(ladderId) ?? null;
  }
}
