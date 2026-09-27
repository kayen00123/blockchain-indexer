import 'dotenv/config';
import { Pool } from 'pg';
import { startUniswapV2BaseIndexer } from './uniswap_v2_base_indexer.js';
const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startUniswapV2BaseIndexer(pgPool, process.env.UNISWAP_V2_BASE_WS_URL ?? '');