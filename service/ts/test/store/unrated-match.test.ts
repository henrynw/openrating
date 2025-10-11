import assert from 'node:assert/strict';
import { test } from 'node:test';

import { MemoryStore } from '../../src/store/memory.js';
import type { MatchInput } from '../../src/engine/types.js';
import type { LadderKey } from '../../src/store/index.js';

test('records unrated match with skip reason and exposes it in summaries', async () => {
  const store = new MemoryStore();

  const organization = await store.createOrganization({ name: 'Skip Org' });
  const playerA = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'A' });
  const playerB = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'B' });
  const playerC = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'C' });
  const playerD = await store.createPlayer({ organizationId: organization.organizationId, displayName: 'D' });

  const ladderKey: LadderKey = {
    organizationId: organization.organizationId,
    sport: 'BADMINTON',
    discipline: 'DOUBLES',
  };

  const matchInput: MatchInput = {
    sport: 'BADMINTON',
    discipline: 'DOUBLES',
    format: 'MD',
    sides: {
      A: { players: [playerA.playerId, playerB.playerId] },
      B: { players: [playerC.playerId, playerD.playerId] },
    },
    games: [],
    winner: 'A',
  };

  const { ladderId, players } = await store.ensurePlayers(
    [...matchInput.sides.A.players, ...matchInput.sides.B.players],
    ladderKey,
    { organizationId: organization.organizationId }
  );

  const { matchId, ratingEvents } = await store.recordMatch({
    ladderId,
    ladderKey,
    match: matchInput,
    result: null,
    playerStates: players,
    submissionMeta: {
      providerId: 'test-provider',
      organizationId: organization.organizationId,
      startTime: '2024-05-01T10:00:00Z',
      rawPayload: { winner: 'A' },
    },
    pairUpdates: [],
    ratingStatus: 'UNRATED',
    ratingSkipReason: 'MISSING_SCORES',
  });

  assert.equal(ratingEvents.length, 0, 'unrated match should not emit rating events');

  const summary = await store.getMatch(matchId, organization.organizationId);
  assert.ok(summary, 'match summary should be retrievable');
  assert.equal(summary.ratingStatus, 'UNRATED');
  assert.equal(summary.ratingSkipReason, 'MISSING_SCORES');
  assert.equal(summary.winnerSide, 'A');
  assert.equal(summary.games.length, 0);

  const list = await store.listMatches({ organizationId: organization.organizationId, limit: 10 });
  assert.equal(list.items[0]?.ratingStatus, 'UNRATED');
  assert.equal(list.items[0]?.ratingSkipReason, 'MISSING_SCORES');
});
