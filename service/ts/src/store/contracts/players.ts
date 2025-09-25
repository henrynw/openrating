import type { LadderKey } from './common.js';

export interface PlayerCreateInput {
  organizationId: string;
  displayName: string;
  shortName?: string;
  nativeName?: string;
  externalRef?: string;
  givenName?: string;
  familyName?: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
}

export interface PlayerUpdateInput {
  displayName?: string;
  shortName?: string | null;
  nativeName?: string | null;
  externalRef?: string | null;
  givenName?: string | null;
  familyName?: string | null;
  sex?: 'M' | 'F' | 'X' | null;
  birthYear?: number | null;
  countryCode?: string | null;
  regionId?: string | null;
}

export interface PlayerRecord {
  playerId: string;
  organizationId: string;
  displayName: string;
  shortName?: string;
  nativeName?: string;
  givenName?: string;
  familyName?: string;
  sex?: 'M' | 'F' | 'X';
  birthYear?: number;
  countryCode?: string;
  regionId?: string;
  externalRef?: string;
}

export interface PlayerListQuery {
  organizationId: string;
  cursor?: string;
  limit?: number;
  q?: string;
}

export interface PlayerListResult {
  items: PlayerRecord[];
  nextCursor?: string;
}

export type PlayerLadderKey = LadderKey;
