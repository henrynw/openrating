export const DEFAULT_PAGE_SIZE = 50;
export const MAX_PAGE_SIZE = 200;

export const clampLimit = (limit?: number, min = 1): number => {
  if (!limit || limit < min) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
};

export const normalizeOffset = (offset?: number): number => {
  if (!offset || offset < 0) return 0;
  return Math.floor(offset);
};
