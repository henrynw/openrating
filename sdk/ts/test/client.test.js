import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { OpenRatingClient, OpenRatingError } from '../dist/index.js';

const jsonResponse = (body, init = { status: 200 }) =>
  new Response(JSON.stringify(body), {
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(init.headers ?? {}),
    },
  });

describe('OpenRatingClient', () => {
  it('fetches health status', async () => {
    let called = false;
    const fetchMock = async (input, init) => {
      called = true;
      assert.equal(String(input), 'https://api.test/health');
      assert.equal(init?.method, 'GET');
      return jsonResponse({ ok: true, version: '1.0.0' });
    };

    const client = new OpenRatingClient({ baseUrl: 'https://api.test', fetchImpl: fetchMock });
    const result = await client.health();

    assert.deepEqual(result, { ok: true, version: '1.0.0' });
    assert.equal(called, true);
  });

  it('retries transient failures when submitting a match', async () => {
    const responses = [
      jsonResponse({ error: 'temporary' }, { status: 503 }),
      jsonResponse({ match_id: 'm-1', rating_status: 'QUEUED' }),
    ];

    const fetchMock = async () => {
      const response = responses.shift();
      if (!response) {
        throw new Error('Unexpected call');
      }
      return response;
    };

    const client = new OpenRatingClient({
      baseUrl: 'https://api.test',
      fetchImpl: fetchMock,
      retry: { attempts: 2, backoffMs: 0 },
    });

    const payload = {
      provider_id: 'p1',
      organization_id: 'org1',
      discipline: 'SINGLES',
      format: 'BEST_OF_THREE',
      start_time: new Date().toISOString(),
      sides: { A: { players: ['p1'] }, B: { players: ['p2'] } },
      games: [{ game_no: 1, a: 6, b: 4 }],
    };

    const result = await client.submitMatch(payload);

    assert.deepEqual(result, { match_id: 'm-1', rating_status: 'QUEUED' });
  });

  it('throws OpenRatingError for non-retriable failures', async () => {
    const fetchMock = async () => jsonResponse({ error: 'not_found' }, { status: 404 });

    const client = new OpenRatingClient({ baseUrl: 'https://api.test', fetchImpl: fetchMock });

    await assert.rejects(
      () =>
        client.submitMatch({
          provider_id: 'p1',
          organization_id: 'org1',
          discipline: 'SINGLES',
          format: 'BEST_OF_THREE',
          start_time: new Date().toISOString(),
          sides: { A: { players: ['p1'] }, B: { players: ['p2'] } },
          games: [{ game_no: 1, a: 6, b: 4 }],
        }),
      (error) => {
        assert.ok(error instanceof OpenRatingError);
        assert.equal(error.status, 404);
        assert.deepEqual(error.body, { error: 'not_found' });
        return true;
      }
    );
  });
});
