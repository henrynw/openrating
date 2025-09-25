import { createApp } from '../../src/app.js';
import { MemoryStore } from '../../src/store/memory.js';

export const createTestApp = () => {
  const store = new MemoryStore();
  const app = createApp(store);
  return { app, store };
};
