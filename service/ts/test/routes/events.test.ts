import { beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import { randomUUID } from 'node:crypto';

process.env.AUTH_DISABLE = '1';
process.env.NODE_ENV = 'test';
delete process.env.DATABASE_URL;

const { createTestApp } = await import('../helpers/app.js');

let agent: request.SuperTest<request.Test>;

beforeEach(() => {
  const { app } = createTestApp();
  agent = request(app);
});

const createOrganization = async () => {
  const res = await agent.post('/v1/organizations').send({ name: `Org ${randomUUID()}` });
  assert.equal(res.status, 201, res.text);
  return res.body as { organization_id: string };
};

const createEvent = async (
  organizationId: string,
  overrides: {
    name?: string;
    startDate?: string | null;
    endDate?: string | null;
    season?: string | null;
    sanctioningBody?: string | null;
  } = {}
) => {
  const payload: Record<string, unknown> = {
    organization_id: organizationId,
    provider_id: 'disabled',
    type: 'TOURNAMENT',
    name: overrides.name ?? `Event ${randomUUID()}`,
  };

  if (overrides.startDate !== undefined) payload.start_date = overrides.startDate;
  if (overrides.endDate !== undefined) payload.end_date = overrides.endDate;
  if (overrides.season !== undefined) payload.season = overrides.season;
  if (overrides.sanctioningBody !== undefined) {
    payload.sanctioning_body = overrides.sanctioningBody;
  }

  const res = await agent.post('/v1/events').send(payload);
  assert.equal(res.status, 201, res.text);
  return res.body.event as { event_id: string };
};

const createCompetition = async (
  organizationId: string,
  eventId: string,
  overrides: { sport?: string | null; discipline?: string | null; name?: string } = {}
) => {
  const payload: Record<string, unknown> = {
    provider_id: 'disabled',
    name: overrides.name ?? `Competition ${randomUUID()}`,
  };

  if (overrides.sport !== undefined) payload.sport = overrides.sport;
  if (overrides.discipline !== undefined) payload.discipline = overrides.discipline;

  const res = await agent.post(`/v1/events/${eventId}/competitions`).send(payload);
  assert.equal(res.status, 201, res.text);
  return res.body.competition as { competition_id: string };
};

const addDays = (base: Date, offsetDays: number) => {
  const copy = new Date(base);
  copy.setUTCDate(copy.getUTCDate() + offsetDays);
  return copy.toISOString();
};

test('events list filters by schedule status combinations', async () => {
  const org = await createOrganization();
  const now = new Date();

  const completed = await createEvent(org.organization_id, {
    name: 'Completed Classic',
    startDate: addDays(now, -14),
    endDate: addDays(now, -7),
  });
  await createCompetition(org.organization_id, completed.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const inProgress = await createEvent(org.organization_id, {
    name: 'In Progress Invitational',
    startDate: addDays(now, -1),
    endDate: addDays(now, 1),
  });
  await createCompetition(org.organization_id, inProgress.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const upcoming = await createEvent(org.organization_id, {
    name: 'Future Open',
    startDate: addDays(now, 5),
    endDate: addDays(now, 6),
  });
  await createCompetition(org.organization_id, upcoming.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const upcomingRes = await agent
    .get('/v1/events')
    .query({ organization_id: org.organization_id, statuses: 'UPCOMING' });
  assert.equal(upcomingRes.status, 200, upcomingRes.text);
  const upcomingEvents = (upcomingRes.body as { events: Array<{ event_id: string }> }).events;
  assert.equal(upcomingEvents.length, 1);
  assert.equal(upcomingEvents[0].event_id, upcoming.event_id);

  const multiRes = await agent
    .get('/v1/events')
    .query({ organization_id: org.organization_id, statuses: ['IN_PROGRESS', 'UPCOMING'] });
  assert.equal(multiRes.status, 200, multiRes.text);
  const multiEvents = (multiRes.body as { events: Array<{ event_id: string }> }).events;
  const multiIds = new Set(multiEvents.map((event) => event.event_id));
  assert.ok(multiIds.has(inProgress.event_id));
  assert.ok(multiIds.has(upcoming.event_id));
  assert.equal(multiIds.has(completed.event_id), false);
});

test('events list filters by sport and discipline and includes competitions by default', async () => {
  const org = await createOrganization();

  const badmintonSingles = await createEvent(org.organization_id, {
    name: 'Badminton Singles',
    startDate: addDays(new Date(), 2),
    endDate: addDays(new Date(), 3),
  });
  await createCompetition(org.organization_id, badmintonSingles.event_id, {
    sport: 'BADMINTON',
    discipline: 'SINGLES',
  });

  const badmintonDoubles = await createEvent(org.organization_id, {
    name: 'Badminton Doubles',
    startDate: addDays(new Date(), 4),
    endDate: addDays(new Date(), 5),
  });
  await createCompetition(org.organization_id, badmintonDoubles.event_id, {
    sport: 'BADMINTON',
    discipline: 'DOUBLES',
  });

  const tennisSingles = await createEvent(org.organization_id, {
    name: 'Tennis Singles',
    startDate: addDays(new Date(), 6),
    endDate: addDays(new Date(), 7),
  });
  await createCompetition(org.organization_id, tennisSingles.event_id, {
    sport: 'TENNIS',
    discipline: 'SINGLES',
  });

  const res = await agent
    .get('/v1/events')
    .query({ organization_id: org.organization_id, sport: 'BADMINTON', discipline: 'SINGLES' });
  assert.equal(res.status, 200, res.text);

  const events = (res.body as {
    events: Array<{ event_id: string; competitions: Array<{ sport: string | null; discipline: string | null }> }>;
  }).events;
  assert.equal(events.length, 1);
  assert.equal(events[0].event_id, badmintonSingles.event_id);
  assert.equal(events[0].competitions.length, 1);
  assert.equal(events[0].competitions[0].sport, 'BADMINTON');
  assert.equal(events[0].competitions[0].discipline, 'SINGLES');
});

test('events list supports sorting with cursor pagination', async () => {
  const org = await createOrganization();
  const base = new Date();

  const early = await createEvent(org.organization_id, {
    name: 'Early Event',
    startDate: addDays(base, -10),
    endDate: addDays(base, -9),
  });
  await createCompetition(org.organization_id, early.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const middle = await createEvent(org.organization_id, {
    name: 'Middle Event',
    startDate: addDays(base, -2),
    endDate: addDays(base, -1),
  });
  await createCompetition(org.organization_id, middle.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const late = await createEvent(org.organization_id, {
    name: 'Late Event',
    startDate: addDays(base, 5),
    endDate: addDays(base, 6),
  });
  await createCompetition(org.organization_id, late.event_id, { sport: 'BADMINTON', discipline: 'SINGLES' });

  const firstPage = await agent
    .get('/v1/events')
    .query({
      organization_id: org.organization_id,
      sort: 'start_date',
      sort_direction: 'desc',
      limit: 2,
    });
  assert.equal(firstPage.status, 200, firstPage.text);
  const firstEvents = (firstPage.body as { events: Array<{ event_id: string; start_date: string }> }).events;
  assert.equal(firstEvents.length, 2);
  const firstStart = new Date(firstEvents[0].start_date!);
  const secondStart = new Date(firstEvents[1].start_date!);
  assert.ok(firstStart >= secondStart);

  assert.ok(firstPage.body.next_cursor, 'expected a cursor for additional results');
  assert.match(firstPage.body.next_cursor, /^[A-Za-z0-9_-]+$/);

  const secondPage = await agent
    .get('/v1/events')
    .query({
      organization_id: org.organization_id,
      sort: 'start_date',
      sort_direction: 'desc',
      limit: 2,
      cursor: firstPage.body.next_cursor,
    });
  assert.equal(secondPage.status, 200, secondPage.text);
  const secondEvents = (secondPage.body as { events: Array<{ event_id: string; start_date: string }> }).events;
  assert.equal(secondEvents.length, 1);
  assert.equal(secondEvents[0].event_id, early.event_id);
});
