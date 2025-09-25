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

export interface PairState {
  pairId: string;
  players: string[];
  gamma: number;
  matches: number;
}

export interface PairUpdate {
  pairId: string;
  players: string[];
  gammaBefore: number;
  gammaAfter: number;
  delta: number;
  matchesBefore: number;
  matchesAfter: number;
  activated: boolean;
}

export interface MatchUpdateContext {
  getPlayer: (id: string) => PlayerState;
  getPair?: (players: string[]) => PairState | undefined;
}

export interface UpdateResult {
  perPlayer: Array<{
    playerId: string;
    muBefore: number;
    muAfter: number;
    delta: number;
    sigmaBefore: number;
    sigmaAfter: number;
    winProbPre: number;
  }>;
  pairUpdates: PairUpdate[];
  teamDelta: number;
  winProbability: number;
}
