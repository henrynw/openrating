export type MatchSubmit = {
  provider_id: string;
  organization_id: string;
  discipline: 'SINGLES' | 'DOUBLES' | 'MIXED';
  format: string;
  start_time: string;
  venue_region_id?: string;
  tier?: 'SANCTIONED' | 'LEAGUE' | 'SOCIAL' | 'EXHIBITION';
  sides: { A: { players: string[] }, B: { players: string[] } };
  games: { game_no: number; a: number; b: number }[];
};

export class OpenRatingClient {
  constructor(private baseUrl: string, private token?: string) {}
  async health() {
    const r = await fetch(`${this.baseUrl}/health`);
    return r.json();
  }
  async submitMatch(match: MatchSubmit) {
    const r = await fetch(`${this.baseUrl}/v1/matches`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}) },
      body: JSON.stringify(match)
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }
}
