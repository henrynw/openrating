import type { MatchInput, WinnerSide } from '../engine/types.js';
import type {
  MatchSubmission,
  NormalizationFailure,
  NormalizationResult,
  RegisteredFormat,
} from './types.js';

const ensureSortedGames = (games: MatchSubmission['games']) =>
  [...games].sort((a, b) => a.game_no - b.game_no);

// Validates a single badminton game scored to 21 with rally scoring and 30 cap.
const isValid21PointGame = (a: number, b: number) => {
  if (a === b) return false;
  const high = Math.max(a, b);
  const low = Math.min(a, b);
  if (high < 21 || high > 30) return false;
  if (high === 30) return low >= 29 ? true : high - low >= 2;
  return high - low >= 2;
};

const winnerFromGames = (games: MatchSubmission['games']): WinnerSide | undefined => {
  let winsA = 0;
  let winsB = 0;
  for (const g of games) {
    if (g.a === g.b) return undefined;
    if (!isValid21PointGame(g.a, g.b)) return undefined;
    if (g.a > g.b) winsA += 1;
    else winsB += 1;
  }
  if (winsA === winsB) return undefined;
  return winsA > winsB ? 'A' : 'B';
};

const error = (message: string, issues?: unknown): NormalizationFailure => ({
  ok: false,
  error: 'validation_failed',
  message,
  issues,
});

const createNormalizeBo3 = (
  expectedPlayersPerSide: number,
  label: string
) =>
  (submission: MatchSubmission): NormalizationResult => {
    if (
      submission.sides.A.players.length !== expectedPlayersPerSide ||
      submission.sides.B.players.length !== expectedPlayersPerSide
    ) {
      return error(`${label} requires exactly ${expectedPlayersPerSide} players per side`);
    }

    if (submission.games.length < 2 || submission.games.length > 3) {
      return error('badminton best-of-three must contain two or three games');
    }

    const sortedGames = ensureSortedGames(submission.games);
    const winner = winnerFromGames(sortedGames);
    if (!winner) {
      return error('unable to determine winner from scores');
    }

    const match: MatchInput = {
      sport: 'BADMINTON',
      discipline: submission.discipline,
      format: submission.format,
      tier: submission.tier,
      sides: submission.sides,
      games: sortedGames,
      winner,
    };

    return { ok: true, match };
  };

const normalizeSinglesBo3 = createNormalizeBo3(1, 'badminton singles');
const normalizeDoublesBo3 = createNormalizeBo3(2, 'badminton doubles');
const normalizeMixedBo3 = createNormalizeBo3(2, 'badminton mixed doubles');

const singlesFormats = ['BO3_21RALLY', 'MS', 'WS', 'BS', 'GS'];
const doublesFormats = ['MD', 'WD', 'BD', 'GD'];
const mixedFormats = ['XD'];

export const badmintonFormats: RegisteredFormat[] = [
  ...singlesFormats.map((format) => ({
    sport: 'BADMINTON',
    discipline: 'SINGLES',
    format,
    normalize: normalizeSinglesBo3,
  })),
  ...doublesFormats.map((format) => ({
    sport: 'BADMINTON',
    discipline: 'DOUBLES',
    format,
    normalize: normalizeDoublesBo3,
  })),
  ...mixedFormats.map((format) => ({
    sport: 'BADMINTON',
    discipline: 'MIXED',
    format,
    normalize: normalizeMixedBo3,
  })),
];
