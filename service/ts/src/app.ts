import express from 'express';
import type { Express, ErrorRequestHandler } from 'express';

import type { RatingStore } from './store/index.js';
import { registerHealthRoutes } from './routes/health.js';
import { registerOrganizationRoutes } from './routes/organizations.js';
import { registerPlayerRoutes } from './routes/players.js';
import { registerMatchRoutes } from './routes/matches.js';
import { registerRatingRoutes } from './routes/ratings.js';
import { createOrganizationResolver } from './routes/helpers/organization-resolver.js';
import { registerEventRoutes } from './routes/events.js';
import { registerCompetitionRoutes } from './routes/competitions.js';
import { profilePhotoService } from './services/profile-photos.js';
import {
  EventLookupError,
  MatchLookupError,
  OrganizationLookupError,
  PlayerLookupError,
  InvalidLeaderboardFilterError,
} from './store/errors.js';

const errorHandler: ErrorRequestHandler = (err, _req, res, next) => {
  if (res.headersSent) {
    return next(err);
  }

  const payload = serializeError(err);
  if (payload.log) {
    // eslint-disable-next-line no-console
    console.error(payload.log.context, payload.log.error);
  }

  res.status(payload.status).json(payload.body);
};

const serializeError = (err: unknown): {
  status: number;
  body: Record<string, unknown>;
  log?: { error: unknown; context: string };
} => {
  if (err instanceof PlayerLookupError) {
    return {
      status: 404,
      body: {
        error: 'player_not_found',
        message: err.message,
        ...(err.context && Object.keys(err.context).length ? { context: err.context } : {}),
      },
    };
  }

  if (err instanceof OrganizationLookupError) {
    return { status: 404, body: { error: 'organization_not_found', message: err.message } };
  }

  if (err instanceof EventLookupError) {
    return { status: 404, body: { error: 'event_not_found', message: err.message } };
  }

  if (err instanceof MatchLookupError) {
    return { status: 404, body: { error: 'match_not_found', message: err.message } };
  }

  if (err instanceof InvalidLeaderboardFilterError) {
    return {
      status: 400,
      body: { error: err.code, message: err.message },
    };
  }

  return {
    status: 500,
    body: { error: 'internal_error', message: 'Unexpected error' },
    log: { error: err, context: 'unhandled_error' },
  };
};

export const createApp = (store: RatingStore): Express => {
  const app = express();
  app.use(express.json());

  registerHealthRoutes(app);

  const resolveOrganization = createOrganizationResolver(store);

  registerOrganizationRoutes(app, store);
  registerPlayerRoutes(app, { store, resolveOrganization, photoService: profilePhotoService });
  registerMatchRoutes(app, { store, resolveOrganization });
  registerRatingRoutes(app, { store, resolveOrganization });
  registerEventRoutes(app, { store, resolveOrganization });
  registerCompetitionRoutes(app, { store });

  app.use(errorHandler);

  return app;
};
