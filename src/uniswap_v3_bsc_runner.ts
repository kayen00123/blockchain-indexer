import 'dotenv/config';
import { Pool } from 'pg';
import { startUniswapV3BscIndexer } from './uniswap_v3_bsc_indexer.js';

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
const websocketUrl = process.env.UNISWAP_V3_BSC_WS_URL ?? '';

console.log('[uniswap-v3-bsc] starting standalone listener');
await startUniswapV3BscIndexer(pgPool, websocketUrl);

let stopping = false;
const shutdown = async () => {
  if (stopping) return;
  stopping = true;
  console.log('[uniswap-v3-bsc] shutting down');
  await pgPool.end();
  process.exit(0);
};
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());