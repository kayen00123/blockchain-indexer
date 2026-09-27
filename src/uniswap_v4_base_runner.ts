import 'dotenv/config';
import { Pool } from 'pg';
import { startUniswapV4BaseIndexer } from './uniswap_v4_base_indexer.js';

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startUniswapV4BaseIndexer(pgPool, process.env.UNISWAP_V4_BASE_WS_URL ?? '');
let stopping = false;
const shutdown = async () => { if (stopping) return; stopping = true; console.log('[uniswap-v4-base] shutting down'); await pgPool.end(); process.exit(0); };
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());