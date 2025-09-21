import type { RatingStore } from './types.js';
import { MemoryStore } from './memory.js';
import { PostgresStore } from './postgres.js';

export * from './types.js';

let store: RatingStore | null = null;

export const getStore = (): RatingStore => {
  if (!store) {
    store = process.env.DATABASE_URL ? new PostgresStore() : new MemoryStore();
  }
  return store;
};
