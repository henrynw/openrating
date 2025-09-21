declare module 'drizzle-orm' {
  export const and: (...args: any[]) => any;
  export const eq: (...args: any[]) => any;
  export const inArray: (...args: any[]) => any;
}

declare module 'drizzle-orm/pg-core' {
  export const pgTable: any;
  export const text: any;
  export const timestamp: any;
  export const integer: any;
  export const jsonb: any;
  export const doublePrecision: any;
  export const primaryKey: any;
  export const uniqueIndex: any;
  export const serial: any;
  export const generatedAlwaysAsIdentity: any;
}

declare module 'drizzle-orm/node-postgres' {
  import type { Pool } from 'pg';
  export function drizzle(pool: Pool): any;
}

declare module 'pg' {
  export interface PoolClient {
    query: (text: string, params?: unknown[]) => Promise<{ rows: any[] }>;
    release: () => void;
  }
  export class Pool {
    constructor(config: { connectionString: string });
    connect(): Promise<PoolClient>;
    query: PoolClient['query'];
    end(): Promise<void>;
  }
}
