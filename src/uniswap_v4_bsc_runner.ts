import 'dotenv/config';
import { Pool } from 'pg';
import { startUniswapV4BscIndexer } from './uniswap_v4_bsc_indexer.js';

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startUniswapV4BscIndexer(pgPool, process.env.UNISWAP_V4_BSC_WS_URL ?? '');

let stopping = false;
const shutdown = async () => { if (stopping) return; stopping = true; console.log('[uniswap-v4-bsc] shutting down'); await pgPool.end(); process.exit(0); };
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());