import { test } from 'node:test';
import assert from 'node:assert/strict';

const { normalizeMatchStatistics, isMatchMetricRecord } = await import('../../src/routes/helpers/statistics.js');

test('normalizeMatchStatistics flattens BWF-style team statistics', () => {
  const stats = {
    team1: {
      other: 0,
      gamePoints: 3,
      ralliesWon: 42,
      challengeUsed: '0',
      ralliesWonInPercent: 72.41,
    },
    team2: {
      other: 0,
      gamePoints: 0,
      ralliesWon: 16,
      challengeUsed: '0',
      ralliesWonInPercent: 27.59,
    },
  };

  const normalized = normalizeMatchStatistics(stats);
  assert.ok(normalized, 'expected stats to normalize');
  if (!normalized) return;

  assert.equal(normalized.team1_game_points?.value, 3);
  assert.equal(normalized.team1_game_points?.metadata?.source_path, 'team1.gamePoints');

  assert.equal(normalized.team2_rallies_won_in_percent?.value, 27.59);
  assert.equal(
    normalized.team2_rallies_won_in_percent?.metadata?.source_path,
    'team2.ralliesWonInPercent'
  );

  assert.equal(normalized.team1_challenge_used?.value, 0);
  assert.equal(normalized.team2_rallies_won?.value, 16);
});

test('normalizeMatchStatistics returns null when no numeric values are present', () => {
  const stats = {
    team1: {
      notes: 'missing',
    },
    extra: {
      nested: {
        value: 'N/A',
      },
    },
  };

  const normalized = normalizeMatchStatistics(stats);
  assert.equal(normalized, null);
});

test('isMatchMetricRecord only accepts canonical metric objects', () => {
  assert.equal(
    isMatchMetricRecord({
      rallies_won: { value: 12, unit: 'count' },
      rally_pct: { value: 54.2, metadata: { source_path: 'team1.percent' } },
    }),
    true
  );

  assert.equal(
    isMatchMetricRecord({
      raw: 10,
    }),
    false
  );
});
