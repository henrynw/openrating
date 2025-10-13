const ISO_SEPARATOR = '|';

const isValidDate = (value: Date): boolean => Number.isFinite(value.getTime());

export interface MatchCursor {
  startTime: Date;
  matchId: string;
}

export const buildMatchCursor = ({ startTime, matchId }: MatchCursor): string =>
  `${startTime.toISOString()}${ISO_SEPARATOR}${matchId}`;

export const parseMatchCursor = (cursor: string): MatchCursor | null => {
  if (!cursor || typeof cursor !== 'string') return null;
  const [iso, id] = cursor.split(ISO_SEPARATOR);
  if (!iso || !id) return null;
  const startTime = new Date(iso);
  if (!isValidDate(startTime)) return null;
  return { startTime, matchId: id };
};

export interface RatingEventCursorPayload<TId = string> {
  createdAt: Date;
  id: TId;
}

export type RatingEventCursorInput = { createdAt: Date; id: string | number };

export const buildRatingEventCursor = ({ createdAt, id }: RatingEventCursorInput): string =>
  `${createdAt.toISOString()}${ISO_SEPARATOR}${id}`;

export const parseRatingEventCursor = (cursor: string): RatingEventCursorPayload<string> | null => {
  if (!cursor || typeof cursor !== 'string') return null;
  const [iso, idRaw] = cursor.split(ISO_SEPARATOR);
  if (!iso || !idRaw) return null;
  const createdAt = new Date(iso);
  if (!isValidDate(createdAt)) return null;
  return { createdAt, id: idRaw };
};

export const parseNumericRatingEventCursor = (cursor: string): RatingEventCursorPayload<number> | null => {
  const parsed = parseRatingEventCursor(cursor);
  if (!parsed) return null;
  const numericId = Number(parsed.id);
  if (!Number.isFinite(numericId)) return null;
  return { createdAt: parsed.createdAt, id: numericId };
};
