import type { RatingStore } from './types';
import { MemoryStore } from './memory';
import { PostgresStore } from './postgres';

export * from './types';

let store: RatingStore | null = null;

export const getStore = (): RatingStore => {
  if (!store) {
    store = process.env.DATABASE_URL ? new PostgresStore() : new MemoryStore();
  }
  return store;
};
