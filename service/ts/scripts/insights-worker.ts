import { randomUUID } from 'crypto';
import { setTimeout as sleep } from 'timers/promises';

import { getStore, type PlayerInsightsJobCompletion } from '../src/store/index.js';

const WORKER_ID = process.env.INSIGHTS_WORKER_ID ?? randomUUID();
const POLL_INTERVAL_MS = Number(process.env.INSIGHTS_WORKER_POLL_MS ?? 1000);

const gracefulSleep = async (ms: number) => {
  try {
    await sleep(ms);
  } catch (err) {
    if ((err as any)?.name !== 'AbortError') {
      throw err;
    }
  }
};

const main = async () => {
  const store = getStore();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const job = await store.claimPlayerInsightsJob({ workerId: WORKER_ID });
    if (!job) {
      await gracefulSleep(POLL_INTERVAL_MS);
      continue;
    }

    const completion: PlayerInsightsJobCompletion = {
      jobId: job.jobId,
      workerId: WORKER_ID,
      success: false,
    };

    try {
      const snapshot = await store.buildPlayerInsightsSnapshot({
        organizationId: job.organizationId,
        playerId: job.playerId,
        sport: job.sport ?? null,
        discipline: job.discipline ?? null,
      });

      await store.upsertPlayerInsightsSnapshot(
        {
          organizationId: job.organizationId,
          playerId: job.playerId,
          sport: job.sport ?? null,
          discipline: job.discipline ?? null,
        },
        snapshot
      );

      completion.success = true;
    } catch (err) {
      console.error('player_insights_job_failed', {
        jobId: job.jobId,
        playerId: job.playerId,
        organizationId: job.organizationId,
        error: err,
      });
      completion.error = err instanceof Error ? err.message : 'unknown_error';
    }

    await store.completePlayerInsightsJob(completion);
    await gracefulSleep(100);
  }
};

main().catch((err) => {
  console.error('insights_worker_error', err);
  process.exitCode = 1;
});
