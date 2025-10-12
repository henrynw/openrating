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
};

const matchFormat = {
  family: ladderQuery.sport,
  code: 'MS',
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
      provider_id: 'disabled',
      organization_id: organizationId,
      sport: ladderQuery.sport,
      discipline: ladderQuery.discipline,
      format: matchFormat,
      start_time: new Date().toISOString(),
      sides: {
        A: { players: [playerA] },
        B: { players: [playerB] },
      },
      games: [
        { game_no: 1, a: 21, b: 18 },
        { game_no: 2, a: 21, b: 17 },
      ],
      winner: 'A',
    });

  assert.equal(res.status, 200, res.text);
  return res.body as {
    match_id: string;
    ratings: Array<{
      player_id: string;
      rating_event_id: string;
    }>;
  };
};

test('match list includes rating events when requested', async () => {
  const org = await createOrganization();
  const alice = await createPlayer(org.organization_id, 'Alice');
  const bob = await createPlayer(org.organization_id, 'Bob');
  const submission = await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);

  const listWithoutInclude = await agent
    .get('/v1/matches')
    .query({ organization_id: org.organization_id });
  assert.equal(listWithoutInclude.status, 200, listWithoutInclude.text);

  const matchesWithout = (listWithoutInclude.body as { matches: Array<Record<string, unknown>> }).matches;
  assert.equal(matchesWithout.length, 1, 'expected one match in response');
  assert.equal('rating_events' in matchesWithout[0], false, 'rating_events should be omitted by default');

  const listWithInclude = await agent
    .get('/v1/matches')
    .query({ organization_id: org.organization_id, include: 'rating_events' });
  assert.equal(listWithInclude.status, 200, listWithInclude.text);

  const matchesWith = (listWithInclude.body as {
    matches: Array<{ rating_events: Array<{ rating_event_id: string }> }>;
  }).matches;
  assert.equal(matchesWith.length, 1, 'expected one match in response');

  const ratingEvents = matchesWith[0].rating_events;
  assert.ok(Array.isArray(ratingEvents), 'rating_events should be an array when requested');
  assert.equal(ratingEvents.length, submission.ratings.length, 'should return one rating event per player');

  const expectedEventIds = new Set(submission.ratings.map((item) => item.rating_event_id));
  for (const event of ratingEvents) {
    assert.ok(expectedEventIds.has(event.rating_event_id), 'unexpected rating event returned');
  }
});

test('match detail includes rating events when requested', async () => {
  const org = await createOrganization();
  const alice = await createPlayer(org.organization_id, 'Alice');
  const bob = await createPlayer(org.organization_id, 'Bob');
  const submission = await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);

  const detailWithoutInclude = await agent
    .get(`/v1/matches/${submission.match_id}`)
    .query({ organization_id: org.organization_id });
  assert.equal(detailWithoutInclude.status, 200, detailWithoutInclude.text);
  assert.equal(
    'rating_events' in detailWithoutInclude.body,
    false,
    'rating_events should be omitted by default'
  );

  const detailWithInclude = await agent
    .get(`/v1/matches/${submission.match_id}`)
    .query({ organization_id: org.organization_id, include: 'rating_events' });
  assert.equal(detailWithInclude.status, 200, detailWithInclude.text);

  const ratingEvents = (detailWithInclude.body as {
    rating_events: Array<{ rating_event_id: string; match_id: string }>;
  }).rating_events;
  assert.ok(Array.isArray(ratingEvents));
  assert.equal(ratingEvents.length, submission.ratings.length);
  for (const event of ratingEvents) {
    assert.equal(event.match_id, submission.match_id);
  }

  const eventIds = new Set(ratingEvents.map((event) => event.rating_event_id));
  for (const rating of submission.ratings) {
    assert.ok(eventIds.has(rating.rating_event_id));
  }
});

test('invalid include parameter returns validation error', async () => {
  const org = await createOrganization();
  const alice = await createPlayer(org.organization_id, 'Alice');
  const bob = await createPlayer(org.organization_id, 'Bob');
  await submitSinglesMatch(org.organization_id, alice.player_id, bob.player_id);

  const invalidList = await agent
    .get('/v1/matches')
    .query({ organization_id: org.organization_id, include: 'unknown' });
  assert.equal(invalidList.status, 400, invalidList.text);
  assert.equal(invalidList.body.error, 'validation_error');

  const invalidDetail = await agent
    .get('/v1/matches/invalid-id')
    .query({ organization_id: org.organization_id, include: 'unknown' });
  assert.equal(invalidDetail.status, 400, invalidDetail.text);
  assert.equal(invalidDetail.body.error, 'validation_error');
});
