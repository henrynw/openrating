import 'dotenv/config';
import { join } from 'node:path';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { getDb, getPool } from './client';

async function main() {
  const db = getDb();
  try {
    await migrate(db, {
      migrationsFolder: join(process.cwd(), 'drizzle'),
    });
  } finally {
    // Always release connections so CLI exits cleanly.
    await getPool().end().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error('migrate_failed', err);
  process.exit(1);
});
