import type { Request, RequestHandler } from 'express';
import { auth } from 'express-oauth2-jwt-bearer';
import { ensureSubject, grantMatches, loadGrants } from './store/grants.js';

const audience = process.env.AUTH0_AUDIENCE;
const domain = process.env.AUTH0_DOMAIN;
const configured = Boolean(audience && domain);

const AUTH_DISABLED = process.env.AUTH_DISABLE === '1' || !configured;

const jwtMiddleware: RequestHandler = AUTH_DISABLED
  ? (_req, _res, next) => next()
  : auth({
      audience,
      issuerBaseURL: `https://${domain}`,
      algorithms: ['RS256'],
    });

export const requireAuth = jwtMiddleware;

export const requireScope = (scope: string): RequestHandler => (req, res, next) => {
  if (AUTH_DISABLED) return next();
  const payload = (req as any).auth?.payload as Record<string, any> | undefined;
  const scopes = (payload?.scope ?? '').toString().split(' ').filter(Boolean);
  if (!scopes.includes(scope)) {
    return res.status(403).send({ error: 'insufficient_scope', required: scope });
  }
  return next();
};

export class AuthorizationError extends Error {
  status = 403;
  code = 'forbidden';
}

export async function enforceMatchWrite(req: Request, params: { organizationId: string; sport: string; regionId: string }) {
  if (AUTH_DISABLED) return;
  const payload = (req as any).auth?.payload as Record<string, any> | undefined;
  const subject = payload?.sub as string | undefined;
  if (!subject) {
    const err = new AuthorizationError('Missing subject in access token');
    err.code = 'missing_subject';
    err.status = 401;
    throw err;
  }
  const displayName = (payload?.name as string | undefined) ?? (payload?.client_id as string | undefined) ?? subject;

  await ensureSubject(subject, displayName);
  const grants = await loadGrants(subject);

  if (!grantMatches(grants, params.organizationId, params.sport, params.regionId)) {
    const err = new AuthorizationError('Insufficient grants for matches:write');
    err.code = 'matches_write_denied';
    throw err;
  }
}
