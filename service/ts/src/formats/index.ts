import type { MatchSubmission, NormalizationResult } from './types.js';
import { badmintonFormats } from './badminton.js';

const registry = [...badmintonFormats];

export const listSupportedFormats = () =>
  registry.map((entry) => ({
    sport: entry.sport,
    discipline: entry.discipline,
    format: entry.format,
  }));

export const normalizeMatchSubmission = (
  submission: MatchSubmission
): NormalizationResult => {
  const handler = registry.find(
    (entry) =>
      entry.sport === submission.sport &&
      entry.discipline === submission.discipline &&
      entry.format === submission.format
  );

  if (handler) {
    return handler.normalize(submission);
  }

  const formatMatch = registry.find(
    (entry) => entry.sport === submission.sport && entry.format === submission.format
  );

  if (formatMatch) {
    return {
      ok: false,
      error: 'validation_failed',
      message: `format ${submission.format} requires discipline ${formatMatch.discipline}`,
    };
  }

  return {
    ok: false,
    error: 'unsupported_format',
    message: `unsupported format ${submission.sport}/${submission.discipline}/${submission.format}`,
  };
};
