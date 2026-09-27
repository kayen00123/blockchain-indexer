import 'dotenv/config';
import { Pool } from 'pg';
import { startPancakeSwapInfinityIndexer, type PancakeSwapInfinityMode } from './pancakeswap_infinity_indexer.js';

const mode = (process.argv[2] ?? 'all') as PancakeSwapInfinityMode;
if (!['all', 'pools', 'prices'].includes(mode)) {
  throw new Error(`Unknown mode: ${mode}. Use all, pools, or prices.`);
}

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
const websocketUrl = process.env.PANCAKESWAP_INFINITY_WS_URL ?? '';

console.log(`[pancakeswap-infinity] starting standalone ${mode} process`);
await startPancakeSwapInfinityIndexer(pgPool, websocketUrl, undefined, mode);

let stopping = false;
const shutdown = async () => {
  if (stopping) return;
  stopping = true;
  console.log('[pancakeswap-infinity] shutting down');
  await pgPool.end();
  process.exit(0);
};
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());