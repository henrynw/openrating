import express from 'express';
import type { Express } from 'express';

import type { RatingStore } from './store/index.js';
import { registerHealthRoutes } from './routes/health.js';
import { registerOrganizationRoutes } from './routes/organizations.js';
import { registerPlayerRoutes } from './routes/players.js';
import { registerMatchRoutes } from './routes/matches.js';
import { registerRatingRoutes } from './routes/ratings.js';
import { createOrganizationResolver } from './routes/helpers/organization-resolver.js';
import { registerLeaderboardRoutes } from './routes/leaderboards.js';

export const createApp = (store: RatingStore): Express => {
  const app = express();
  app.use(express.json());

  registerHealthRoutes(app);

  const resolveOrganization = createOrganizationResolver(store);

  registerOrganizationRoutes(app, store);
  registerPlayerRoutes(app, { store, resolveOrganization });
  registerMatchRoutes(app, { store, resolveOrganization });
  registerRatingRoutes(app, { store, resolveOrganization });
  registerLeaderboardRoutes(app, { store, resolveOrganization });

  return app;
};
