import 'dotenv/config';
import { getStore } from '../src/store/index.js';

const BATCH_SIZE = Number(process.env.DEBUG_DRAIN_BATCH ?? 25);
const WORKER_ID = process.env.DEBUG_DRAIN_WORKER_ID ?? 'debug-drain';

const store = getStore();

const drain = async () => {
  let total = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const jobs = await store.claimPlayerInsightsJob({ workerId: WORKER_ID, batchSize: BATCH_SIZE });
    if (!jobs.length) {
      break;
    }

    for (const job of jobs) {
      await store.completePlayerInsightsJob({
        jobId: job.jobId,
        workerId: WORKER_ID,
        success: true,
      });
    }
    total += jobs.length;
    console.log(`drained ${total} jobs so far`);
  }
  console.log(`drain complete: ${total} jobs`);
};

await drain();
