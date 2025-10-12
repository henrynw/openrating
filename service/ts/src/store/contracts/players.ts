import type { LadderKey } from './common.js';

export type PlayerDiscipline = 'SINGLES' | 'DOUBLES' | 'MIXED';

export type PlayerHandedness = 'LEFT' | 'RIGHT' | 'AMBIDEXTROUS' | 'OTHER';

export type PlayerDominantSide = 'DEUCE' | 'AD' | 'LEFT' | 'RIGHT' | 'BOTH' | 'OTHER';

export interface PlayerRankingSnapshot {
  source: string;
  discipline?: PlayerDiscipline | null;
  position?: number | null;
  points?: number | null;
  asOf?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface PlayerCompetitiveProfile {
  discipline?: PlayerDiscipline | null;
  rankingPoints?: number | null;
  rankingPosition?: number | null;
  totalMatches?: number | null;
  asOf?: string | null;
  externalRankings?: PlayerRankingSnapshot[] | null;
}

export interface PlayerAttributes {
  handedness?: PlayerHandedness | null;
  dominantSide?: PlayerDominantSide | null;
  heightCm?: number | null;
  weightKg?: number | null;
  birthDate?: string | null;
  residence?: string | null;
  metadata?: Record<string, unknown> | null;
}

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
  birthDate?: string;
  countryCode?: string;
  regionId?: string;
  competitiveProfile?: PlayerCompetitiveProfile | null;
  attributes?: PlayerAttributes | null;
  profilePhotoId?: string | null;
  profilePhotoUploadedAt?: string | null;
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
  birthDate?: string | null;
  countryCode?: string | null;
  regionId?: string | null;
  competitiveProfile?: PlayerCompetitiveProfile | null;
  attributes?: PlayerAttributes | null;
  profilePhotoId?: string | null;
  profilePhotoUploadedAt?: string | null;
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
  birthDate?: string;
  countryCode?: string;
  regionId?: string;
  externalRef?: string;
  competitiveProfile?: PlayerCompetitiveProfile | null;
  attributes?: PlayerAttributes | null;
  profilePhotoId?: string;
  profilePhotoUrl?: string;
  profilePhotoUploadedAt?: string;
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

export interface PlayerSportTotal {
  sport: LadderKey['sport'];
  totalPlayers: number;
}

export interface PlayerSportTotalsQuery {
  organizationId: string;
  sport?: LadderKey['sport'];
  discipline?: PlayerDiscipline;
}

export interface PlayerSportTotalsResult {
  totals: PlayerSportTotal[];
}
