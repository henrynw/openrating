import { getStore } from '../src/store/index.js';

const main = async () => {
  const store = getStore();
  await store.runNightlyStabilization();
  console.log('Nightly stabilization completed');
};

main().catch((err) => {
  console.error('nightly_stabilization_failed', err);
  process.exitCode = 1;
});
