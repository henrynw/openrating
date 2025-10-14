import type { Request, RequestHandler } from 'express';
import { auth } from 'express-oauth2-jwt-bearer';
import jwt from 'jsonwebtoken';
import { ensureSubject, hasOrgPermission, loadGrants } from './store/grants.js';

const audience = process.env.AUTH0_AUDIENCE;
const domain = process.env.AUTH0_DOMAIN;
const authProvider = (process.env.AUTH_PROVIDER ?? 'AUTH0').toUpperCase();

const devSharedSecret = process.env.AUTH_DEV_SHARED_SECRET;
const devAudience = process.env.AUTH_DEV_AUDIENCE ?? audience;
const devIssuer = process.env.AUTH_DEV_ISSUER;

const configuredAuth0 = Boolean(audience && domain);
const configuredDev = authProvider === 'DEV' && Boolean(devSharedSecret);

const AUTH_DISABLED = process.env.AUTH_DISABLE === '1' || (!configuredAuth0 && !configuredDev);

const createDevJwtMiddleware = (): RequestHandler => {
  const secret = devSharedSecret;
  if (!secret) {
    // Should not happen when CONFIGURED_DEV is true, fallback to no-auth passthrough.
    return (_req, _res, next) => next();
  }

  return (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).send({ error: 'missing_token', message: 'Authorization header missing bearer token.' });
    }

    const token = authHeader.slice('Bearer '.length);

    try {
      const verifyOptions: jwt.VerifyOptions = {
        algorithms: ['HS256'],
      };
      if (devAudience) verifyOptions.audience = devAudience;
      if (devIssuer) verifyOptions.issuer = devIssuer;

      const payload = jwt.verify(token, secret, verifyOptions);
      (req as any).auth = { payload };
      return next();
    } catch (err) {
      console.error('dev_auth_invalid_token', err instanceof Error ? err.message : err);
      return res.status(401).send({ error: 'invalid_token', message: 'Invalid or expired token.' });
    }
  };
};

const jwtMiddleware: RequestHandler = AUTH_DISABLED
  ? (_req, _res, next) => next()
  : configuredDev
    ? createDevJwtMiddleware()
    : auth({
        audience,
        issuerBaseURL: `https://${domain}`,
        algorithms: ['RS256'],
      });

export const requireAuth = jwtMiddleware;

export const requireScope = (scope: string): RequestHandler => (req, res, next) => {
  if (AUTH_DISABLED) return next();
  if (!hasScope(req, scope)) {
    return res.status(403).send({ error: 'insufficient_scope', required: scope });
  }
  return next();
};

export const hasScope = (req: Request, scope: string) => {
  if (AUTH_DISABLED) return true;
  const scopes = getScopes(req);
  return scopes.includes(scope);
};

export class AuthorizationError extends Error {
  status = 403;
  code = 'forbidden';
}

export async function authorizeOrgAccess(
  req: Request,
  organizationId: string,
  options: {
    permissions: string[];
    sport?: string | null;
    regionId?: string | null;
    errorCode?: string;
    errorMessage?: string;
  }
) {
  if (AUTH_DISABLED) return;
  const ctx = await getSubjectContext(req);
  if (
    !hasOrgPermission(ctx.grants, organizationId, options.permissions, {
      sport: options.sport ?? null,
      regionId: options.regionId ?? null,
    })
  ) {
    const err = new AuthorizationError(options.errorMessage ?? 'Insufficient grants');
    err.code = options.errorCode ?? 'insufficient_grants';
    throw err;
  }
}

export async function enforceMatchWrite(
  req: Request,
  params: { organizationId: string; sport: string; regionId: string }
) {
  try {
    await authorizeOrgAccess(req, params.organizationId, {
      permissions: ['matches:write'],
      sport: params.sport,
      regionId: params.regionId,
      errorCode: 'matches_write_denied',
      errorMessage: 'Insufficient grants for matches:write',
    });
  } catch (err) {
    if (err instanceof AuthorizationError) {
      const payload = getPayload(req) ?? {};
      const subject = (payload.sub as string | undefined) ?? 'unknown_sub';
      const clientId = (payload.azp as string | undefined) ?? payload.client_id ?? 'unknown_client';
      const tokenScopes = (payload.scope as string | undefined) ?? '';
      console.error('matches_write_denied', {
        subject,
        clientId,
        tokenScopes,
        organizationId: params.organizationId,
        sport: params.sport,
        regionId: params.regionId,
      });
    }
    throw err;
  }
}

export const getSubjectId = async (req: Request) => (await getSubjectContext(req)).subject;

type SubjectContext = {
  subject: string;
  scopes: string[];
  grants: Awaited<ReturnType<typeof loadGrants>>;
};

const SUBJECT_CONTEXT_KEY = Symbol('subjectContext');

const getPayload = (req: Request) => (req as any).auth?.payload as Record<string, any> | undefined;

const getScopes = (req: Request) => {
  const payload = getPayload(req);
  return (payload?.scope ?? '').toString().split(' ').filter(Boolean);
};

const getSubjectContext = async (req: Request): Promise<SubjectContext> => {
  if (AUTH_DISABLED) {
    return { subject: 'disabled', scopes: [], grants: [] };
  }
  const cached = (req as any)[SUBJECT_CONTEXT_KEY] as SubjectContext | undefined;
  if (cached) return cached;

  const payload = getPayload(req);
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
  const ctx: SubjectContext = {
    subject,
    scopes: getScopes(req),
    grants,
  };
  (req as any)[SUBJECT_CONTEXT_KEY] = ctx;
  return ctx;
};
