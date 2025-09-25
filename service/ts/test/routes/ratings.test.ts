import { beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import { randomUUID } from 'node:crypto';

process.env.AUTH_DISABLE = '1';
process.env.NODE_ENV = 'test';
delete process.env.DATABASE_URL;

const { createTestApp } = await import('../helpers/app.js');

const ladderQuery = {
  sport: 'BADMINTON',
  discipline: 'SINGLES',
  format: 'BO3_21RALLY',
};

let agent: request.SuperTest<request.Test>;

beforeEach(() => {
  const { app } = createTestApp();
  agent = request(app);
});

const createOrganization = async () => {
  const res = await agent.post('/v1/organizations').send({ name: `Test Org ${randomUUID()}` });
  assert.equal(res.status, 201, res.text);
  return res.body as { organization_id: string; slug: string };
};

const createPlayer = async (organizationId: string, displayName: string) => {
  const res = await agent.post('/v1/players').send({
    organization_id: organizationId,
    display_name: displayName,
  });
  assert.equal(res.status, 200, res.text);
  return res.body as { player_id: string };
};

const submitSinglesMatch = async (
  organizationId: string,
  playerA: string,
  playerB: string
) => {
  const res = await agent
    .post('/v1/matches')
    .set('Idempotency-Key', randomUUID())
    .send({
      provider_id: 'provider-test',
      organization_id: organizationId,
      sport: ladderQuery.sport,
      discipline: ladderQuery.discipline,
      format: ladderQuery.format,
      start_time: new Date().toISOString(),
      sides: {
        A: { players: [playerA] },
        B: { players: [playerB] },
      },
      games: [
        { game_no: 1, a: 21, b: 15 },
        { game_no: 2, a: 21, b: 18 },
      ],
      winner: 'A',
    });

  assert.equal(res.status, 200, res.text);
  return res.body as {
    match_id: string;
    ratings: Array<{
      player_id: string;
      rating_event_id: string;
      mu_after: number;
    }>;
  };
};

test('match submission links rating events and history lookup works', async () => {
  const org = await createOrganization();
  const alice = await createPlayer(org.organization_id, 'Alice');
  const bob = await createPlayer(org.organization_id, 'Bob');

  const match = await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);
  assert.equal(match.ratings.length, 2);

  for (const rating of match.ratings) {
    assert.equal(typeof rating.rating_event_id, 'string');
    assert.ok(rating.rating_event_id.length > 0);
  }

  const aliceRatingRef = match.ratings.find((r) => r.player_id === alice.player_id);
  assert.ok(aliceRatingRef);

  const eventsRes = await agent
    .get(`/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-events`)
    .query(ladderQuery);
  assert.equal(eventsRes.status, 200, eventsRes.text);

  const eventsBody = eventsRes.body as {
    rating_events: Array<{
      rating_event_id: string;
      mu_after: number;
    }>;
  };
  assert.ok(Array.isArray(eventsBody.rating_events));
  assert.ok(eventsBody.rating_events.length >= 1);

  const listMatch = eventsBody.rating_events.find(
    (event) => event.rating_event_id === aliceRatingRef!.rating_event_id
  );
  assert.ok(listMatch, 'submitted rating event missing from history');

  const detailRes = await agent
    .get(
      `/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-events/${aliceRatingRef!.rating_event_id}`
    )
    .query(ladderQuery);
  assert.equal(detailRes.status, 200, detailRes.text);

  const detailBody = detailRes.body as { rating_event_id: string; mu_after: number };
  assert.equal(detailBody.rating_event_id, aliceRatingRef!.rating_event_id);

  const snapshotRes = await agent
    .get(`/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-snapshot`)
    .query(ladderQuery);
  assert.equal(snapshotRes.status, 200, snapshotRes.text);

  const snapshotBody = snapshotRes.body as {
    rating_event: { rating_event_id: string } | null;
    mu: number;
  };
  assert.ok(snapshotBody.rating_event, 'snapshot should include latest rating event');
  assert.equal(
    snapshotBody.rating_event!.rating_event_id,
    aliceRatingRef!.rating_event_id,
    'snapshot should reference latest event'
  );
});

test('rating snapshot honors as_of parameter', async () => {
  const org = await createOrganization();
  const alice = await createPlayer(org.organization_id, 'Alice');
  const bob = await createPlayer(org.organization_id, 'Bob');

  await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);
  await new Promise((resolve) => setTimeout(resolve, 5));
  await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);

  const eventsRes = await agent
    .get(`/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-events`)
    .query(ladderQuery);
  assert.equal(eventsRes.status, 200, eventsRes.text);

  const events = (eventsRes.body as {
    rating_events: Array<{
      rating_event_id: string;
      applied_at: string;
      mu_after: number;
    }>;
  }).rating_events;
  assert.ok(events.length >= 2, 'expected at least two rating events');

  const latest = events[0];
  const earliest = events[events.length - 1];

  const latestSnapshotRes = await agent
    .get(`/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-snapshot`)
    .query(ladderQuery);
  assert.equal(latestSnapshotRes.status, 200, latestSnapshotRes.text);

  const latestSnapshot = latestSnapshotRes.body as {
    rating_event: { rating_event_id: string } | null;
    mu: number;
  };
  assert.ok(latestSnapshot.rating_event);
  assert.equal(latestSnapshot.rating_event!.rating_event_id, latest.rating_event_id);
  assert.equal(latestSnapshot.mu, latest.mu_after);

  const asOfSnapshotRes = await agent
    .get(`/v1/organizations/${org.organization_id}/players/${alice.player_id}/rating-snapshot`)
    .query({ ...ladderQuery, as_of: earliest.applied_at });
  assert.equal(asOfSnapshotRes.status, 200, asOfSnapshotRes.text);

  const asOfSnapshot = asOfSnapshotRes.body as {
    rating_event: { rating_event_id: string } | null;
    mu: number;
  };
  assert.ok(asOfSnapshot.rating_event);
  assert.equal(asOfSnapshot.rating_event!.rating_event_id, earliest.rating_event_id);
  assert.equal(asOfSnapshot.mu, earliest.mu_after);
});
