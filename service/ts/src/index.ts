import express from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';
dotenv.config();

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.status(200).send({ ok: true }));

const PlayerUpsert = z.object({
  organization_id: z.string(),
  external_ref: z.string().optional(),
  given_name: z.string(),
  family_name: z.string(),
  sex: z.enum(['M','F','X']).optional(),
  birth_year: z.number().int().optional(),
  country_code: z.string().optional(),
  region_id: z.string().optional()
});

app.post('/v1/players', (req, res) => {
  const parsed = PlayerUpsert.safeParse(req.body);
  if (!parsed.success) return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
  return res.send({ player_id: 'p_demo', ...parsed.data });
});

const MatchSubmit = z.object({
  provider_id: z.string(),
  organization_id: z.string(),
  discipline: z.enum(['SINGLES','DOUBLES','MIXED']),
  format: z.string(),
  start_time: z.string(),
  venue_region_id: z.string().optional(),
  tier: z.enum(['SANCTIONED','LEAGUE','SOCIAL','EXHIBITION']).optional(),
  sides: z.object({
    A: z.object({ players: z.array(z.string()).min(1).max(2) }),
    B: z.object({ players: z.array(z.string()).min(1).max(2) }),
  }),
  games: z.array(z.object({ game_no: z.number().int(), a: z.number().int(), b: z.number().int() }))
});

app.post('/v1/matches', (req, res) => {
  const parsed = MatchSubmit.safeParse(req.body);
  if (!parsed.success) return res.status(400).send({ error: 'validation_error', details: parsed.error.flatten() });
  const match_id = 'm_demo';
  return res.send({ match_id, ratings: [] });
});

const port = process.env.PORT ? Number(process.env.PORT) : 8080;
app.listen(port, () => console.log(`OpenRating listening on :${port}`));
