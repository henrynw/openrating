import 'dotenv/config';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { getPool } from './client';

const MIGRATIONS_DIR = join(process.cwd(), 'drizzle');
const MIGRATIONS_TABLE = '__openrating_migrations';

const ensureTableSQL = `
CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

async function main() {
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query(ensureTableSQL);

    const applied = new Set<string>();
    const result = await client.query<{ name: string }>(`SELECT name FROM ${MIGRATIONS_TABLE} ORDER BY name`);
    for (const row of result.rows) applied.add(row.name);

    const migrations = readdirSync(MIGRATIONS_DIR)
      .filter((file) => file.endsWith('.sql'))
      .sort();

    for (const file of migrations) {
      if (applied.has(file)) continue;

      const sql = readFileSync(join(MIGRATIONS_DIR, file), 'utf8');
      console.log(`Applying migration ${file}`);
      await client.query(sql);
      await client.query(`INSERT INTO ${MIGRATIONS_TABLE} (name) VALUES ($1)`, [file]);
    }

    console.log('Migrations up to date');
  } finally {
    client.release();
    await pool.end().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error('migrate_failed', err);
  process.exit(1);
});
