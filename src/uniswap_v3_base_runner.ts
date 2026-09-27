import 'dotenv/config';
import { Pool } from 'pg';
import { startUniswapV3BaseIndexer } from './uniswap_v3_base_indexer.js';
const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startUniswapV3BaseIndexer(pgPool, process.env.UNISWAP_V3_BASE_WS_URL ?? '');
let stopping = false; const shutdown = async () => { if (stopping) return; stopping = true; await pgPool.end(); process.exit(0); }; process.once('SIGINT', () => void shutdown()); process.once('SIGTERM', () => void shutdown());