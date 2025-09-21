import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

let pool: Pool | undefined;

export const getPool = () => {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) throw new Error('DATABASE_URL is not set');
    pool = new Pool({ connectionString });
  }
  return pool;
};

export const getDb = () => drizzle(getPool());
