#!/usr/bin/env tsx
import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

import { getStore } from '../src/store/index.js';
import { getPool } from '../src/db/client.js';
import type { RatingReplayReport } from '../src/store/index.js';
import type { Pool } from 'pg';

const formatNumber = (value: number) => value.toLocaleString('en-US', { maximumFractionDigits: 2 });

const printReport = (report: RatingReplayReport) => {
  if (!report.entries.length) {
    console.log(report.dryRun ? 'Dry-run: no ladders require replay.' : 'Replay queue empty.');
    return;
  }

  console.log(
    `${report.dryRun ? 'Dry-run replay for' : 'Replayed'} ${report.laddersProcessed} ladder(s), ${formatNumber(report.matchesProcessed)} match(es), ${formatNumber(report.playersTouched)} player(s).`
  );

  for (const entry of report.entries) {
    console.log(
      `- ${entry.ladderId} :: matches=${formatNumber(entry.matchesProcessed)} players=${formatNumber(entry.playersTouched)} pairs=${formatNumber(entry.pairUpdates)} from=${entry.replayFrom ?? 'n/a'} to=${entry.replayTo ?? 'n/a'}${entry.dryRun ? ' (dry-run)' : ''}`
    );
  }
};

const runQueue = async (argv: any) => {
  const store = getStore();
  const report = await store.processRatingReplayQueue({
    limit: argv.limit as number | undefined,
    dryRun: argv.dryRun as boolean,
  });
  printReport(report);
};

const runLadder = async (argv: any) => {
  const store = getStore();
  const report = await store.replayRatings({
    ladderId: argv.ladderId as string,
    from: (argv.from as string | undefined) ?? undefined,
    dryRun: argv.dryRun as boolean,
  });
  printReport(report);
};

const fetchAllLadderIds = async (pool: Pool): Promise<string[]> => {
  const client = await pool.connect();
  try {
    const result = await client.query<{ ladder_id: string }>(
      'SELECT ladder_id FROM rating_ladders ORDER BY ladder_id'
    );
    return result.rows.map((row) => row.ladder_id);
  } finally {
    client.release();
  }
};

const runAll = async (argv: any) => {
  const store = getStore();
  const pool = getPool();
  const ladderIds = await fetchAllLadderIds(pool);
  if (!ladderIds.length) {
    console.log('No ladders found in database.');
    return;
  }

  const dryRun = argv.dryRun as boolean;
  const from = (argv.from as string | undefined) ?? undefined;
  const stopOnError = argv.stopOnError as boolean;

  const summary: RatingReplayReport = {
    dryRun,
    laddersProcessed: 0,
    matchesProcessed: 0,
    playersTouched: 0,
    entries: [],
  };

  const failures: Array<{ ladderId: string; message: string }> = [];

  for (const [index, ladderId] of ladderIds.entries()) {
    console.log(`Replaying ${ladderId} (${index + 1}/${ladderIds.length})`);
    try {
      const report = await store.replayRatings({
        ladderId,
        dryRun,
        from,
      });
      if (report.entries.length) {
        for (const entry of report.entries) {
          summary.entries.push(entry);
          summary.matchesProcessed += entry.matchesProcessed;
          summary.playersTouched += entry.playersTouched;
        }
        summary.laddersProcessed += report.entries.length;
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('replay_failed', { ladderId, message });
      failures.push({ ladderId, message });
      if (stopOnError) {
        break;
      }
    }
  }

  if (!summary.entries.length && failures.length) {
    console.log('No ladders were replayed successfully.');
  } else {
    printReport(summary);
  }

  if (failures.length) {
    console.warn('Replay failures:', failures.map((failure) => `${failure.ladderId} (${failure.message})`).join(', '));
  }
};

async function main() {
  await yargs(hideBin(process.argv))
    .scriptName('replay')
    .command(
      'queue',
      'Process queued ladders that require replay',
      (cmd) =>
        cmd
          .option('limit', {
            type: 'number',
            describe: 'Maximum ladders to process',
          })
          .option('dry-run', {
            type: 'boolean',
            default: false,
            describe: 'Preview replay work without mutating state',
          }),
      (argv) => runQueue(argv)
    )
    .command(
      ['ladder <ladderId>', 'one <ladderId>'],
      'Replay a specific ladder immediately',
      (cmd) =>
        cmd
          .positional('ladderId', {
            type: 'string',
            describe: 'Ladder identifier (sport:discipline)',
            demandOption: true,
          })
          .option('from', {
            type: 'string',
            describe: 'Optional ISO timestamp hint for earliest match to replay',
          })
          .option('dry-run', {
            type: 'boolean',
            default: false,
            describe: 'Preview replay without mutating state',
          }),
      (argv) => runLadder(argv)
    )
    .command(
      'all',
      'Replay every ladder sequentially',
      (cmd) =>
        cmd
          .option('from', {
            type: 'string',
            describe: 'Optional ISO timestamp hint for earliest match to replay',
          })
          .option('dry-run', {
            type: 'boolean',
            default: false,
            describe: 'Preview replay without mutating state',
          })
          .option('stop-on-error', {
            type: 'boolean',
            default: false,
            describe: 'Abort immediately if a ladder replay fails',
          }),
      (argv) => runAll(argv)
    )
    .demandCommand(1)
    .strict()
    .help()
    .parseAsync();
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      const pool = getPool();
      await pool.end();
    } catch (err) {
      if (process.env.DATABASE_URL) {
        console.error('Failed to close database connection', err);
      }
    }
  });
