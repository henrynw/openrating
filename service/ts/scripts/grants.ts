#!/usr/bin/env tsx
import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { eq } from 'drizzle-orm';
import { ensureSubject, loadGrants } from '../src/store/grants.js';
import { getDb, getPool } from '../src/db/client.js';
import { subjectGrants, organizations } from '../src/db/schema.js';

const DEFAULT_REGION = 'GLOBAL';

type CommandHandler = (argv: any) => Promise<void>;

const isUuid = (value: string) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);

const addHandler: CommandHandler = async (argv) => {
  const subjectId = argv.subject as string;
  let organizationId = argv.org as string;
  const permission = argv.permission as string;
  const sport = (argv.sport as string | undefined) ?? null;
  const rawRegion = argv.region as string | undefined;
  const region =
    rawRegion === undefined
      ? DEFAULT_REGION
      : ['*', 'ANY', 'any', 'ALL', 'all', 'NULL', 'null'].includes(rawRegion)
        ? null
        : rawRegion;
  const displayName = (argv.name as string | undefined) ?? subjectId;

  await ensureSubject(subjectId, displayName);

  const db = getDb();
  if (!isUuid(organizationId)) {
    const slug = organizationId.toLowerCase();
    const rows = await db
      .select({ organizationId: organizations.organizationId })
      .from(organizations)
      .where(eq(organizations.slug, slug))
      .limit(1);
    const row = rows.at(0);
    if (!row) {
      throw new Error(`Organization slug not found: ${slug}`);
    }
    organizationId = row.organizationId;
  }

  await db
    .insert(subjectGrants)
    .values({
      subjectId,
      organizationId,
      sport,
      regionId: region,
      permission,
      createdAt: new Date(),
    })
    .onConflictDoNothing();

  console.log(`Grant ensured for subject=${subjectId} org=${organizationId} sport=${sport ?? '*'} region=${region} permission=${permission}`);
};

const listHandler: CommandHandler = async (argv) => {
  const subjectId = argv.subject as string;
  const grants = await loadGrants(subjectId);
  if (argv.json) {
    console.log(JSON.stringify(grants, null, 2));
    return;
  }
  if (!grants.length) {
    console.log(`No grants found for subject ${subjectId}`);
    return;
  }
  for (const grant of grants) {
    console.log(
      `- ${grant.permission} :: org=${grant.organizationId} sport=${grant.sport ?? '*'} region=${grant.regionId ?? '*'} `
    );
  }
};

async function main() {
  const parser = yargs(hideBin(process.argv))
    .scriptName('grants')
    .command(
      'add',
      'Add or ensure a grant for a subject',
      (cmd) =>
        cmd
          .option('subject', { type: 'string', demandOption: true, desc: 'Auth0 subject/client ID' })
          .option('name', { type: 'string', desc: 'Display name for subject' })
          .option('org', { type: 'string', demandOption: true, desc: 'Organization ID' })
          .option('sport', { type: 'string', desc: 'Sport code (optional)' })
          .option('region', { type: 'string', default: DEFAULT_REGION, desc: 'Region ID (default GLOBAL)' })
          .option('permission', {
            type: 'string',
            choices: ['matches:write', 'matches:read', 'ratings:read', 'players:read', 'players:write', 'organizations:write', 'organizations:read'],
            demandOption: true,
            desc: 'Permission to grant',
          }),
      (argv) => addHandler(argv)
    )
    .command(
      'list',
      'List grants for a subject',
      (cmd) =>
        cmd
          .option('subject', { type: 'string', demandOption: true, desc: 'Auth0 subject/client ID' })
          .option('json', { type: 'boolean', default: false, desc: 'Output JSON' }),
      (argv) => listHandler(argv)
    )
    .demandCommand(1)
    .strict()
    .help();

  await parser.parseAsync();
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await getPool().end().catch(() => undefined);
  });
