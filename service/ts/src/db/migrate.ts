import 'dotenv/config';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { getPool } from './client.js';
import type { Pool, PoolClient } from 'pg';

const MIGRATIONS_DIR = join(process.cwd(), 'drizzle');
const MIGRATIONS_TABLE = '__openrating_migrations';

const ensureTableSQL = `
CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

const parsePositiveInteger = (value: string | undefined, fallback: number) => {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const RETRY_ATTEMPTS = parsePositiveInteger(process.env.DB_MIGRATE_RETRIES, 10);
const RETRY_DELAY_MS = parsePositiveInteger(process.env.DB_MIGRATE_RETRY_DELAY_MS, 5_000);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const connectWithRetry = async (pool: Pool): Promise<PoolClient> => {
  let lastError: unknown;
  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt += 1) {
    try {
      return await pool.connect();
    } catch (err) {
      lastError = err;
      if (attempt === RETRY_ATTEMPTS) break;
      const delay = RETRY_DELAY_MS * attempt;
      console.warn('db_connect_retry', {
        attempt,
        attempts: RETRY_ATTEMPTS,
        delayMs: delay,
        message: err instanceof Error ? err.message : String(err),
      });
      await sleep(delay);
    }
  }
  throw lastError instanceof Error ? lastError : new Error('Failed to acquire database connection');
};

async function main() {
  const pool = getPool();
  const client = await connectWithRetry(pool);
  try {
    await client.query(ensureTableSQL);

    const applied = new Set<string>();
    const result = await client.query(`SELECT name FROM ${MIGRATIONS_TABLE} ORDER BY name`);
    for (const row of result.rows as Array<{ name: string }>) applied.add(row.name);

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
