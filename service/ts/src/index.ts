import express from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';

// ---- engine imports ----
import { updateMatch } from './engine/rating';
import type { PlayerState } from './engine/types';
import { normalizeMatchSubmission } from './formats';

dotenv.config();

const app = express();
app.use(express.json());

// ---- in-memory "DB" for now (swap with Aurora later) ----
const memory: Record<string, PlayerState> = {};
const getOrInit = (id: string): PlayerState => {
  if (!memory[id]) {
    memory[id] = { playerId: id, mu: 1500, sigma: 350, matchesCount: 0 };
  }
  return memory[id];
};

// ---- health ----
app.get('/health', (_req, res) => res.status(200).send({ ok: true }));

// ---- players ----
const PlayerUpsert = z.object({
  organization_id: z.string(),
  external_ref: z.string().optional(),
  given_name: z.string(),
  family_name: z.string(),
  sex: z.enum(['M', 'F', 'X']).optional(),
  birth_year: z.number().int().optional(),
  country_code: z.string().optional(),
  region_id: z.string().optional(),
});

app.post('/v1/players', (req, res) => {
  const parsed = PlayerUpsert.safeParse(req.body);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  // For now we just echo. Later: persist player & seed rating row(s)
  return res.send({ player_id: 'p_demo', ...parsed.data });
});

// ---- matches ----
const MatchSubmit = z.object({
  provider_id: z.string(),
  organization_id: z.string(),
  sport: z.enum(['BADMINTON', 'TENNIS', 'SQUASH', 'PADEL', 'PICKLEBALL']),
  discipline: z.enum(['SINGLES', 'DOUBLES', 'MIXED']),
  format: z.string(),
  start_time: z.string(),
  venue_region_id: z.string().optional(),
  tier: z.enum(['SANCTIONED', 'LEAGUE', 'SOCIAL', 'EXHIBITION']).optional(),
  sides: z.object({
    A: z.object({ players: z.array(z.string()).min(1).max(4) }),
    B: z.object({ players: z.array(z.string()).min(1).max(4) }),
  }),
  games: z.array(
    z.object({ game_no: z.number().int(), a: z.number().int(), b: z.number().int() })
  ),
});

app.post('/v1/matches', (req, res) => {
  const parsed = MatchSubmit.safeParse(req.body);
  if (!parsed.success)
    return res
      .status(400)
      .send({ error: 'validation_error', details: parsed.error.flatten() });

  const normalization = normalizeMatchSubmission({
    sport: parsed.data.sport,
    discipline: parsed.data.discipline,
    format: parsed.data.format,
    tier: parsed.data.tier,
    sides: parsed.data.sides,
    games: parsed.data.games,
  });

  if (!normalization.ok) {
    return res.status(400).send({
      error: normalization.error,
      message: normalization.message,
      issues: normalization.issues,
    });
  }

  // Run the rating update (in-memory for now)
  const result = updateMatch(normalization.match, getOrInit);

  return res.send({
    match_id: 'm_demo',
    ratings: result.perPlayer.map((p) => ({
      player_id: p.playerId,
      mu_before: p.muBefore,
      mu_after: p.muAfter,
      delta: p.delta,
      sigma_after: p.sigmaAfter,
      win_probability_pre: p.winProbPre,
    })),
  });
});

app.get('/v1/ratings/:player_id', (req, res) => {
  const p = memory[req.params.player_id];
  if (!p) return res.status(404).send({ error: 'not_found' });
  res.send({ player_id: p.playerId, mu: p.mu, sigma: p.sigma, matches: p.matchesCount });
});

// ---- start server ----
const port = process.env.PORT ? Number(process.env.PORT) : 8080;
app.listen(port, () => console.log(`OpenRating listening on :${port}`));
