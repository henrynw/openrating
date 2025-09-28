import { test } from 'node:test';
import assert from 'node:assert/strict';

import { updateMatch } from '../../src/engine/rating.js';
import type { MatchInput, PlayerState, PairState } from '../../src/engine/types.js';

const pairKey = (players: string[]) => [...players].sort((a, b) => a.localeCompare(b)).join('|');

const createPlayerState = (playerId: string, overrides: Partial<PlayerState> = {}): PlayerState => ({
  playerId,
  mu: 1500,
  sigma: 120,
  matchesCount: 5,
  ...overrides,
});

const sampleMatch: MatchInput = {
  sport: 'BADMINTON',
  discipline: 'DOUBLES',
  format: 'MD',
  sides: {
    A: { players: ['A1', 'A2'] },
    B: { players: ['B1', 'B2'] },
  },
  games: [
    { game_no: 1, a: 21, b: 18 },
    { game_no: 2, a: 19, b: 21 },
    { game_no: 3, a: 21, b: 17 },
  ],
  winner: 'A',
};

const buildContext = (players: Map<string, PlayerState>, pairs: Map<string, PairState>) => ({
  getPlayer: (id: string) => {
    const state = players.get(id);
    if (!state) throw new Error(`missing player ${id}`);
    return state;
  },
  getPair: (playerIds: string[]) => pairs.get(pairKey(playerIds)),
});

test('pair synergy remains inactive until activation threshold', () => {
  const players = new Map<string, PlayerState>([
    ['A1', createPlayerState('A1')],
    ['A2', createPlayerState('A2')],
    ['B1', createPlayerState('B1')],
    ['B2', createPlayerState('B2')],
  ]);

  const pairA: PairState = { pairId: pairKey(['A1', 'A2']), players: ['A1', 'A2'], gamma: 0, matches: 1 };
  const pairB: PairState = { pairId: pairKey(['B1', 'B2']), players: ['B1', 'B2'], gamma: 0, matches: 1 };
  const pairs = new Map<string, PairState>([
    [pairA.pairId, pairA],
    [pairB.pairId, pairB],
  ]);

  const result = updateMatch(sampleMatch, buildContext(players, pairs));
  const updateA = result.pairUpdates.find((u) => u.pairId === pairA.pairId);
  const updateB = result.pairUpdates.find((u) => u.pairId === pairB.pairId);

  assert.ok(updateA);
  assert.ok(updateB);
  assert.equal(updateA.activated, false);
  assert.equal(updateB.activated, false);
  assert.equal(updateA.delta, 0);
  assert.equal(updateB.delta, 0);
  assert.equal(pairA.gamma, 0);
  assert.equal(pairB.gamma, 0);
});

test('pair synergy updates once activation threshold reached', () => {
  const players = new Map<string, PlayerState>([
    ['A1', createPlayerState('A1')],
    ['A2', createPlayerState('A2')],
    ['B1', createPlayerState('B1')],
    ['B2', createPlayerState('B2')],
  ]);

  const pairA: PairState = { pairId: pairKey(['A1', 'A2']), players: ['A1', 'A2'], gamma: 5, matches: 3 };
  const pairB: PairState = { pairId: pairKey(['B1', 'B2']), players: ['B1', 'B2'], gamma: -3, matches: 4 };
  const pairs = new Map<string, PairState>([
    [pairA.pairId, pairA],
    [pairB.pairId, pairB],
  ]);

  const result = updateMatch(sampleMatch, buildContext(players, pairs));
  const updateA = result.pairUpdates.find((u) => u.pairId === pairA.pairId);
  const updateB = result.pairUpdates.find((u) => u.pairId === pairB.pairId);

  assert.ok(updateA);
  assert.ok(updateB);
  assert.equal(updateA.activated, true);
  assert.equal(updateB.activated, true);
  assert.notEqual(updateA.delta, 0);
  assert.notEqual(updateB.delta, 0);
  assert.ok(pairA.gamma > 5, 'winning pair synergy should increase');
  assert.ok(pairB.gamma < -3, 'losing pair synergy should decrease');
});
