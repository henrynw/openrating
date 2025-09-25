import dotenv from 'dotenv';

import { createApp } from './app.js';
import { getStore } from './store/index.js';

dotenv.config();

const store = getStore();
const app = createApp(store);

export { app };

if (process.env.NODE_ENV !== 'test') {
  const port = process.env.PORT ? Number(process.env.PORT) : 8080;
  app.listen(port, () => console.log(`OpenRating listening on :${port}`));
}

