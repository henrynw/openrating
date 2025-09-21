export type Sport = 'BADMINTON' | 'TENNIS' | 'SQUASH' | 'PADEL' | 'PICKLEBALL';
export type Discipline = 'SINGLES' | 'DOUBLES' | 'MIXED';
export type WinnerSide = 'A' | 'B';

export interface PlayerState {
  playerId: string;
  mu: number;
  sigma: number;
  matchesCount: number;
  regionId?: string;
}

export interface GameScore { game_no: number; a: number; b: number; }

export interface MatchInput {
  sport: Sport;
  discipline: Discipline;
  format: string;
  tier?: 'SANCTIONED' | 'LEAGUE' | 'SOCIAL' | 'EXHIBITION';
  sides: { A: { players: string[] }, B: { players: string[] } };
  games: GameScore[];
  winner?: WinnerSide;
  movWeight?: number;
}

export interface UpdateResult {
  perPlayer: Array<{
    playerId: string;
    muBefore: number; muAfter: number; delta: number;
    sigmaAfter: number; winProbPre: number;
  }>;
}
