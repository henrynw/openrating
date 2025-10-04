import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { MemoryStore } from '../../src/store/memory.js';
import { updateMatch } from '../../src/engine/rating.js';
import type { MatchInput } from '../../src/engine/types.js';
import type { LadderKey, RatingReplayReport } from '../../src/store/index.js';

const ladderKey: LadderKey = { sport: 'BADMINTON', discipline: 'SINGLES' };

interface MatchDef {
  start: string;
  winner: 'A' | 'B';
}

const MATCHES: MatchDef[] = [
  { start: '2022-01-01T00:00:00.000Z', winner: 'A' },
  { start: '2023-01-01T00:00:00.000Z', winner: 'B' },
  { start: '2024-01-01T00:00:00.000Z', winner: 'A' },
];

const buildMatchInput = (winner: 'A' | 'B'): MatchInput => ({
  sport: ladderKey.sport,
  discipline: ladderKey.discipline,
  format: 'BEST_OF_3',
  sides: {
    A: { players: [] },
    B: { players: [] },
  },
  games: [
    {
      game_no: 1,
      a: winner === 'A' ? 21 : 12,
      b: winner === 'A' ? 12 : 21,
    },
  ],
  winner,
});

async function buildStore(order: MatchDef[]) {
  const store = new MemoryStore();
  const organization = await store.createOrganization({ name: 'Replay Org' });
  const playerA = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'Player A' });
  const playerB = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'Player B' });

  let ladderId: string | null = null;

  for (const def of order) {
    const matchInput = buildMatchInput(def.winner);
    matchInput.sides.A.players = [playerA.playerId];
    matchInput.sides.B.players = [playerB.playerId];

    const { ladderId: ensuredLadderId, players } = await store.ensurePlayers(
      [playerA.playerId, playerB.playerId],
      ladderKey,
      { organizationId: organization.organizationId }
    );
    ladderId = ensuredLadderId;

    const result = updateMatch(matchInput, {
      getPlayer: (id) => {
        const state = players.get(id);
        if (!state) throw new Error(`missing state for player ${id}`);
        return state;
      },
    });

    await store.recordMatch({
      ladderId,
      ladderKey,
      match: matchInput,
      result,
      playerStates: players,
      pairUpdates: result.pairUpdates,
      submissionMeta: {
        providerId: 'test-provider',
        organizationId: organization.organizationId,
        startTime: def.start,
        rawPayload: { winner: def.winner },
      },
      timing: { completedAt: def.start },
    });
  }

  if (!ladderId) {
    throw new Error('missing ladder');
  }

  return {
    store,
    ladderId,
    organizationId: organization.organizationId,
    playerA: playerA.playerId,
    playerB: playerB.playerId,
  };
}

describe('rating replay queue', () => {
  it('detects out of order ingestion and reports earliest start time', async () => {
    const ingestionOrder = [MATCHES[1], MATCHES[2], MATCHES[0]];
    const { store, ladderId } = await buildStore(ingestionOrder);

    const report: RatingReplayReport = await store.processRatingReplayQueue({ dryRun: true });
    assert.equal(report.laddersProcessed, 1);
    assert.equal(report.entries.length, 1);
    const entry = report.entries[0];
    assert.equal(entry.ladderId, ladderId);
    assert.equal(entry.replayFrom, MATCHES[0].start);
    assert.equal(entry.matchesProcessed, MATCHES.length);

    // dry-run should not clear the queue
    const second = await store.processRatingReplayQueue({ dryRun: true });
    assert.equal(second.entries.length, 1);
  });

  it('replays ladder ratings to chronological order', async () => {
    const ingestionOrder = [MATCHES[1], MATCHES[2], MATCHES[0]];
    const expectedOrder = [...MATCHES].sort((a, b) => a.start.localeCompare(b.start));

    const actual = await buildStore(ingestionOrder);
    const expected = await buildStore(expectedOrder);

    const rebuildReport = await actual.store.processRatingReplayQueue();
    assert.equal(rebuildReport.laddersProcessed, 1);
    assert.equal(rebuildReport.entries[0]?.matchesProcessed, MATCHES.length);

    const postQueueReport = await actual.store.processRatingReplayQueue({ dryRun: true });
    assert.equal(postQueueReport.entries.length, 0);

    const actualRatingA = await actual.store.getPlayerRating(actual.playerA, ladderKey);
    const actualRatingB = await actual.store.getPlayerRating(actual.playerB, ladderKey);
    const expectedRatingA = await expected.store.getPlayerRating(expected.playerA, ladderKey);
    const expectedRatingB = await expected.store.getPlayerRating(expected.playerB, ladderKey);

    assert(actualRatingA && expectedRatingA);
    assert(actualRatingB && expectedRatingB);

    assert.equal(actualRatingA.matchesCount, expectedRatingA.matchesCount);
    assert.equal(actualRatingB.matchesCount, expectedRatingB.matchesCount);
    assert.equal(actualRatingA.mu.toFixed(6), expectedRatingA.mu.toFixed(6));
    assert.equal(actualRatingB.mu.toFixed(6), expectedRatingB.mu.toFixed(6));
    assert.equal(actualRatingA.sigma.toFixed(6), expectedRatingA.sigma.toFixed(6));
    assert.equal(actualRatingB.sigma.toFixed(6), expectedRatingB.sigma.toFixed(6));
  });
});
