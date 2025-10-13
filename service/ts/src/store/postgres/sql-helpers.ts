import { and } from 'drizzle-orm';
import type { SQL } from 'drizzle-orm/sql';

export type SqlFilter = SQL | null | undefined;

const isSql = (value: SqlFilter): value is SQL => Boolean(value);

export const combineFilters = (filters: SqlFilter[]): SQL | undefined => {
  const active = filters.filter(isSql);
  if (active.length === 0) return undefined;
  return active.slice(1).reduce((acc, filter) => and(acc, filter), active[0]);
};
