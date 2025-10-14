#!/usr/bin/env tsx
import 'dotenv/config';
import jwt from 'jsonwebtoken';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const main = async () => {
  const argv = await yargs(hideBin(process.argv))
    .scriptName('mint-dev-token')
    .usage('$0 [options]')
    .option('subject', {
      type: 'string',
      alias: 's',
      describe: 'Subject (sub) to embed in the token',
      demandOption: true,
    })
    .option('client-id', {
      type: 'string',
      alias: 'c',
      describe: 'Client identifier (mirrors Auth0 azp/client_id claims)',
      default: 'dev-cli',
    })
    .option('scope', {
      type: 'array',
      alias: 'S',
      describe: 'Space-separated scope list (repeat flag or provide quoted string)',
      default: ['matches:write'],
    })
    .option('expires-in', {
      type: 'number',
      alias: 'e',
      describe: 'Lifetime in seconds',
      default: 3600,
    })
    .option('audience', {
      type: 'string',
      alias: 'a',
      describe: 'Audience claim to embed (defaults to AUTH_DEV_AUDIENCE/AUTH0_AUDIENCE)',
    })
    .option('issuer', {
      type: 'string',
      alias: 'i',
      describe: 'Issuer claim to embed (defaults to AUTH_DEV_ISSUER or "openrating-dev")',
    })
    .help()
    .parseAsync();

  const sharedSecret = process.env.AUTH_DEV_SHARED_SECRET;
  if (!sharedSecret) {
    throw new Error('AUTH_DEV_SHARED_SECRET is not set. Please configure it in your environment.');
  }

  const audience = argv.audience ?? process.env.AUTH_DEV_AUDIENCE ?? process.env.AUTH0_AUDIENCE;
  const issuer = argv.issuer ?? process.env.AUTH_DEV_ISSUER ?? 'openrating-dev';
  const clientId = argv['client-id'];
  const scopes = (argv.scope as string[]).flatMap((entry) => entry.split(/[\s,]+/)).filter(Boolean);

  const now = Math.floor(Date.now() / 1000);
  const payload: jwt.JwtPayload = {
    sub: argv.subject,
    scope: scopes.join(' '),
    azp: clientId,
    client_id: clientId,
    iat: now,
    exp: now + argv['expires-in'],
  };

  if (audience) payload.aud = audience;
  if (issuer) payload.iss = issuer;

  const token = jwt.sign(payload, sharedSecret, {
    algorithm: 'HS256',
  });

  console.log(
    JSON.stringify(
      {
        token,
        payload,
        audience,
        issuer,
        expiresAt: new Date(payload.exp! * 1000).toISOString(),
      },
      null,
      2
    )
  );
};

main().catch((err) => {
  console.error('mint_dev_token_failed', err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
