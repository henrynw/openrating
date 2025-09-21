import 'dotenv/config';
import type { Config } from 'drizzle-kit';

const connectionString = process.env.DATABASE_URL ?? '';

const config: Config = {
  schema: './src/db/schema.ts',
  out: './drizzle',
  driver: 'pg',
  dbCredentials: {
    connectionString,
  },
};

export default config;
