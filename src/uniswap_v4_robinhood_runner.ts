import 'dotenv/config';
import { Pool } from 'pg';
import { startRobinhoodUniswapV4Indexer } from './uniswap_v4_robinhood_indexer.js';
const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startRobinhoodUniswapV4Indexer(pgPool, process.env.ROBINHOOD_V4_WS_URL ?? '');