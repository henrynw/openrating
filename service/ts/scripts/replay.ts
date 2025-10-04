#!/usr/bin/env tsx
import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

import { getStore } from '../src/store/index.js';
import { getPool } from '../src/db/client.js';
import type { RatingReplayReport } from '../src/store/index.js';

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
