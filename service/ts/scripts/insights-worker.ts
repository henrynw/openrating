import { randomUUID } from 'crypto';
import { setTimeout as sleep } from 'timers/promises';

import { getStore, type PlayerInsightsJobCompletion } from '../src/store/index.js';

const WORKER_ID = process.env.INSIGHTS_WORKER_ID ?? randomUUID();
const POLL_INTERVAL_MS = Number(process.env.INSIGHTS_WORKER_POLL_MS ?? 1000);
const RAW_BATCH_SIZE = Number(process.env.INSIGHTS_WORKER_BATCH_SIZE ?? 10);
const RAW_LOOP_COOLDOWN_MS = Number(process.env.INSIGHTS_WORKER_LOOP_COOLDOWN_MS ?? 50);
const RAW_LOG_EVERY = Number(process.env.INSIGHTS_WORKER_LOG_EVERY ?? 100);

const BATCH_SIZE = Number.isFinite(RAW_BATCH_SIZE) && RAW_BATCH_SIZE > 0 ? Math.min(Math.floor(RAW_BATCH_SIZE), 100) : 1;
const LOOP_COOLDOWN_MS = Number.isFinite(RAW_LOOP_COOLDOWN_MS) && RAW_LOOP_COOLDOWN_MS >= 0 ? RAW_LOOP_COOLDOWN_MS : 50;
const LOG_EVERY = Number.isFinite(RAW_LOG_EVERY) && RAW_LOG_EVERY > 0 ? Math.floor(RAW_LOG_EVERY) : 0;

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
  let processed = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const jobs = await store.claimPlayerInsightsJob({ workerId: WORKER_ID, batchSize: BATCH_SIZE });
    if (!jobs.length) {
      await gracefulSleep(POLL_INTERVAL_MS);
      continue;
    }

    for (const job of jobs) {
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
      processed += 1;
      if (LOG_EVERY && processed % LOG_EVERY === 0) {
        console.log(`[insights-worker:${WORKER_ID}] processed ${processed} jobs`);
      }
    }

    if (LOOP_COOLDOWN_MS > 0) {
      await gracefulSleep(LOOP_COOLDOWN_MS);
    }
  }
};

main().catch((err) => {
  console.error('insights_worker_error', err);
  process.exitCode = 1;
});
