import { test } from 'node:test';
import assert from 'node:assert/strict';

const { normalizeMatchSubmission } = await import('../../src/formats/index.js');

const buildPlayers = (count: number, prefix: string) =>
  Array.from({ length: count }, (_, idx) => `${prefix}${idx}`);

const games = [
  { game_no: 2, a: 18, b: 21 },
  { game_no: 1, a: 21, b: 17 },
  { game_no: 3, a: 21, b: 19 },
];

const cases: Array<{
  name: string;
  format: string;
  discipline: 'SINGLES' | 'DOUBLES' | 'MIXED';
  playersPerSide: number;
}> = [
  { name: 'men singles', format: 'MS', discipline: 'SINGLES', playersPerSide: 1 },
  { name: 'women singles', format: 'WS', discipline: 'SINGLES', playersPerSide: 1 },
  { name: 'boys singles', format: 'BS', discipline: 'SINGLES', playersPerSide: 1 },
  { name: 'girls singles', format: 'GS', discipline: 'SINGLES', playersPerSide: 1 },
  { name: 'men doubles', format: 'MD', discipline: 'DOUBLES', playersPerSide: 2 },
  { name: 'women doubles', format: 'WD', discipline: 'DOUBLES', playersPerSide: 2 },
  { name: 'boys doubles', format: 'BD', discipline: 'DOUBLES', playersPerSide: 2 },
  { name: 'girls doubles', format: 'GD', discipline: 'DOUBLES', playersPerSide: 2 },
  { name: 'mixed doubles', format: 'XD', discipline: 'MIXED', playersPerSide: 2 },
];

for (const testCase of cases) {
  test(`normalizes badminton submission for ${testCase.name}`, () => {
    const result = normalizeMatchSubmission({
      sport: 'BADMINTON',
      discipline: testCase.discipline,
      format: testCase.format,
      sides: {
        A: { players: buildPlayers(testCase.playersPerSide, 'A') },
        B: { players: buildPlayers(testCase.playersPerSide, 'B') },
      },
      games,
    });

    assert.ok(result.ok, `expected ${testCase.format} to normalize`);
    if (!result.ok) return;

    const { match } = result;
    assert.equal(match.format, testCase.format);
    assert.equal(match.discipline, testCase.discipline);
    assert.equal(match.sides.A.players.length, testCase.playersPerSide);
    assert.equal(match.sides.B.players.length, testCase.playersPerSide);
    assert.deepEqual(
      match.games.map((g) => g.game_no),
      [1, 2, 3],
      'games should be returned in ascending order'
    );
  });
}

test('rejects unsupported badminton format code', () => {
  const result = normalizeMatchSubmission({
    sport: 'BADMINTON',
    discipline: 'SINGLES',
    format: 'UNKNOWN',
    sides: {
      A: { players: ['A'] },
      B: { players: ['B'] },
    },
    games,
  });

  assert.equal(result.ok, false, 'expected unsupported format');
  if (result.ok) return;

  assert.equal(result.error, 'unsupported_format');
  assert.match(result.message, /unsupported format BADMINTON\/SINGLES\/UNKNOWN/);
});
