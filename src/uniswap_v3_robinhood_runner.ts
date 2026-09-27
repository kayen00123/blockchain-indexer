import 'dotenv/config';
import { Pool } from 'pg';
import { startRobinhoodUniswapV3Indexer } from './uniswap_v3_robinhood_indexer.js';
import { readWebsocketEndpoints } from './evm_ws_rotation.js';

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await startRobinhoodUniswapV3Indexer(pgPool, readWebsocketEndpoints('ROBINHOOD_V3', process.env.ROBINHOOD_V3_WS_URL ?? process.env.ROBINHOOD_V4_WS_URL ?? ''));
