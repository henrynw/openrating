import type { Express } from 'express';

export const registerHealthRoutes = (app: Express) => {
  app.get('/health', (_req, res) => res.status(200).send({ ok: true }));
};

