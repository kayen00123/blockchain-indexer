import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';
import { getReferencePrice } from './reference_prices.js';

export const PANCAKESWAP_INFINITY_CL_POOL_MANAGER = (process.env.PANCAKESWAP_INFINITY_CL_POOL_MANAGER ?? '0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b').toLowerCase();
const SWAP_TOPIC = '0x04206ad2b7c0f463bff3dd4f33c5735b0f2957a351e4f79763a4fa9e775dd237';
const POOL_KEY_SELECTOR = '0x0e2d484a';
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const WBNB = (process.env.PANCAKESWAP_INFINITY_WBNB ?? '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c').toLowerCase();
const USDT = (process.env.PANCAKESWAP_INFINITY_USDT ?? '0x55d398326f99059ff775485246999027b3197955').toLowerCase();
const BNB_PRICE_URL = process.env.BNB_PRICE_URL ?? 'https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c';
const SWAP_THRESHOLD = Number(process.env.PANCAKESWAP_INFINITY_SWAP_THRESHOLD ?? process.env.EVM_PROMOTION_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 100);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.PANCAKESWAP_INFINITY_MIN_UNIQUE_WALLETS ?? process.env.EVM_PROMOTION_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 20);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const FAILURE_FILE = process.env.PANCAKESWAP_INFINITY_FAILURE_FILE ?? 'pancakeswap-infinity-failures.jsonl';
const PRICE_FILE = process.env.PANCAKESWAP_INFINITY_PRICE_EVENT_FILE ?? 'pancakeswap-infinity-price-events.jsonl';
const TIMEFRAMES = [{ name: '1m', milliseconds: 60_000 }, { name: '5m', milliseconds: 300_000 }, { name: '15m', milliseconds: 900_000 }, { name: '1h', milliseconds: 3_600_000 }, { name: '4h', milliseconds: 14_400_000 }, { name: '1d', milliseconds: 86_400_000 }] as const;

type Pool = {
  id: string;
  address: string;
  currency0: string;
  currency1: string;
  symbol0: string;
  symbol1: string;
  decimals0: number;
  decimals1: number;
  hooks: string;
  fee: number;
  parameters: string;
};
type Activity = { pool: Pool; events: Array<{ wallet: string; timestamp: number }> };
export type PancakeSwapInfinityMode = 'all' | 'pools' | 'prices';

function word(data: string, index: number): bigint {
  const value = data.startsWith('0x') ? data.slice(2) : data;
  if (!/^[0-9a-f]+$/i.test(value) || value.length < (index + 1) * 64) throw new Error(`Invalid ABI data for word ${index}`);
  return BigInt(`0x${value.slice(index * 64, (index + 1) * 64)}`);
}

function signedWord(data: string, index: number, bits: number): number {
  const raw = word(data, index);
  const limit = 1n << BigInt(bits);
  const signed = raw >= limit / 2n ? raw - limit : raw;
  return Number(signed);
}

function addressWord(value: unknown): string {
  const raw = String(value ?? '');
  return /^0x[0-9a-f]{64}$/i.test(raw) ? `0x${raw.slice(-40)}`.toLowerCase() : ZERO_ADDRESS;
}

function decodeUint(value: unknown): number {
  try {
    if (typeof value !== 'string' || !/^0x[0-9a-f]+$/i.test(value)) return 0;
    const result = Number(BigInt(value));
    return Number.isFinite(result) ? result : 0;
  } catch {
    return 0;
  }
}

function decodeString(value: unknown): string {
  if (typeof value !== 'string' || value === '0x') return '';
  try {
    const offset = Number(word(value, 0));
    const length = Number(word(value, offset / 32));
    if (!Number.isInteger(offset) || offset < 0 || offset % 32 !== 0 || !Number.isInteger(length) || length <= 0 || length > 256) return '';
    const hex = value.slice(2 + (offset + 32) * 2, 2 + (offset + 32 + length) * 2);
    return Buffer.from(hex, 'hex').toString('utf8').replace(/\0/g, '');
  } catch {
    return '';
  }
}

function supported(pool: Pool): boolean {
  return pool.currency0 === WBNB || pool.currency1 === WBNB || pool.currency0 === USDT || pool.currency1 === USDT;
}

function tokenPriceUsd(pool: Pool, price: number, bnbUsd: number | null): number | null {
  if (pool.currency1 === USDT) return price;
  if (pool.currency0 === USDT) return 1 / price;
  if (pool.currency1 === WBNB && bnbUsd) return price * bnbUsd;
  if (pool.currency0 === WBNB && bnbUsd) return bnbUsd / price;
  return null;
}

function priceFromSqrt(sqrtPriceX96: bigint, decimals0: number, decimals1: number): number {
  const sqrt = Number(sqrtPriceX96) / Number(2n ** 96n);
  return sqrt * sqrt * 10 ** (decimals0 - decimals1);
}

async function failure(poolId: string, error: unknown): Promise<void> {
  try { await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolId, error: String(error) })}\n`, 'utf8'); } catch { /* best effort */ }
}

async function metadata(call: (method: string, params: unknown[]) => Promise<any>, address: string): Promise<{ symbol: string; decimals: number }> {
  if (address === ZERO_ADDRESS) return { symbol: 'BNB', decimals: 18 };
  const [symbolRaw, decimalsRaw] = await Promise.all([
    call('eth_call', [{ to: address, data: '0x95d89b41' }, 'latest']),
    call('eth_call', [{ to: address, data: '0x313ce567' }, 'latest']),
  ]);
  return { symbol: decodeString(symbolRaw) || address.slice(0, 8), decimals: decodeUint(decimalsRaw) };
}

async function resolvePool(call: (method: string, params: unknown[]) => Promise<any>, id: string): Promise<Pool | null> {
  const result = String(await call('eth_call', [{ to: PANCAKESWAP_INFINITY_CL_POOL_MANAGER, data: `${POOL_KEY_SELECTOR}${id.slice(2)}` }, 'latest']));
  if (!/^0x[0-9a-f]{384}$/i.test(result)) return null;
  const currency0 = addressWord(`0x${result.slice(2, 66)}`);
  const currency1 = addressWord(`0x${result.slice(66, 130)}`);
  const hooks = addressWord(`0x${result.slice(130, 194)}`);
  const poolManager = addressWord(`0x${result.slice(194, 258)}`);
  if (poolManager !== PANCAKESWAP_INFINITY_CL_POOL_MANAGER) return null;
  const [meta0, meta1] = await Promise.all([metadata(call, currency0), metadata(call, currency1)]);
  return { id, address: id, currency0, currency1, symbol0: meta0.symbol, symbol1: meta1.symbol, decimals0: meta0.decimals, decimals1: meta1.decimals, hooks, fee: decodeUint(`0x${result.slice(258, 322)}`), parameters: `0x${result.slice(322, 386)}` };
}

export async function startPancakeSwapInfinityIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, priceFile = PRICE_FILE, mode: PancakeSwapInfinityMode = 'all'): Promise<void> {
  const endpoints = normalizeWebsocketEndpoints(websocketUrl);
  if (endpoints.length === 0) { console.warn('[pancakeswap-infinity] PANCAKESWAP_INFINITY_WS_URL is not configured; indexing is disabled.'); return; }
  const activities = new Map<string, Activity>();
  const promoted = new Set<string>();
  let socket: WebSocket | undefined;
  let requestId = 1;
  let endpointIndex = 0;
  let reconnectDelay = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let bnbUsd: number | null = null;
  const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();
  const rpc = (method: string, params: unknown[]) => new Promise<any>((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return reject(new Error('PancakeSwap Infinity websocket is not open'));
    const id = requestId++;
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
    setTimeout(() => { const request = pending.get(id); if (request) { pending.delete(id); request.reject(new Error(`RPC timeout for ${method}`)); } }, 15_000);
  });
  let recordQueue = Promise.resolve();
  const record = (event: Record<string, unknown>) => { recordQueue = recordQueue.then(() => appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8')); return recordQueue; };

  const process = async (log: any) => {
    const topics = log?.topics;
    const id = String(topics?.[1] ?? '').toLowerCase();
    const wallet = addressWord(topics?.[2]);
    if (String(log?.address ?? '').toLowerCase() !== PANCAKESWAP_INFINITY_CL_POOL_MANAGER || topics?.[0]?.toLowerCase() !== SWAP_TOPIC || !/^0x[0-9a-f]{64}$/i.test(id) || wallet === ZERO_ADDRESS) return;
    const block = Number.parseInt(log.blockNumber ?? '0x0', 16);
    console.log(`[pancakeswap-infinity][websocket] Swap received poolId=${id} block=${block}`);
    let activity = activities.get(id);
    if (!activity) {
      if (!rpc) return;
      const pool = await resolvePool(rpc, id);
      if (!pool || !supported(pool)) return;
      activity = { pool, events: [] };
      activities.set(id, activity);
      console.log(`[pancakeswap-infinity] Pool resolved ${id} ${pool.symbol0}/${pool.symbol1} | ${pool.currency0}/${pool.currency1}`);
    }
    const now = Date.now();
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet, timestamp: now });
    const wallets = new Set(activity.events.map((event) => event.wallet)).size;
    console.log(`[pancakeswap-infinity][websocket] Tracked pool ${id} | swaps=${activity.events.length} | walletCount=${wallets}`);
    const pool = activity.pool;
    const sqrtPriceX96 = word(log.data, 2);
    const liquidity = word(log.data, 3);
    const tick = signedWord(log.data, 4, 256);
    const fee = decodeUint(`0x${log.data.slice(2 + 5 * 64, 2 + 6 * 64)}`);
    const price = priceFromSqrt(sqrtPriceX96, pool.decimals0, pool.decimals1);
    if (mode !== 'pools' && Number.isFinite(price) && price > 0) {
      const inverse = 1 / price;
      const usd = tokenPriceUsd(pool, price, bnbUsd);
      await record({ type: 'price', poolType: 'pancakeswap_infinity_cl', poolId: id, poolAddress: id, manager: PANCAKESWAP_INFINITY_CL_POOL_MANAGER, pair: `${pool.symbol0}/${pool.symbol1}`, price, inversePrice: inverse, tokenPriceUsd: usd, sqrtPriceX96: sqrtPriceX96.toString(), liquidity: liquidity.toString(), tick, fee, block });
      try {
        const client = await pgPool.connect();
        try {
          await client.query('BEGIN');
          await client.query(`INSERT INTO bsc_pancakeswap_infinity_cl_prices (pool_id,pool_address,price,inverse_price,base_currency,quote_currency,sqrt_price_x96,liquidity,tick,fee,updated_block,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW()) ON CONFLICT (pool_id) DO UPDATE SET pool_address=EXCLUDED.pool_address,price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,sqrt_price_x96=EXCLUDED.sqrt_price_x96,liquidity=EXCLUDED.liquidity,tick=EXCLUDED.tick,fee=EXCLUDED.fee,updated_block=EXCLUDED.updated_block,updated_at=NOW()`, [id, id, price, inverse, pool.currency0, pool.currency1, sqrtPriceX96.toString(), liquidity.toString(), tick, fee, block]);
          await client.query(`INSERT INTO latest_prices (pool_address,price,inverse_price,price_change,price_change_percent,price_change_direction,fdv_usd,token_price_usd,total_supply,supply_basis,base_reserve,quote_reserve,updated_slot,updated_at) VALUES ($1,$2,$3,NULL,NULL,NULL,NULL,$4,NULL,'pancakeswap_infinity_cl',0,0,$5,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,token_price_usd=EXCLUDED.token_price_usd,price_change=EXCLUDED.price-latest_prices.price,price_change_percent=CASE WHEN latest_prices.price IS NULL OR latest_prices.price=0 THEN NULL ELSE ((EXCLUDED.price - latest_prices.price) / latest_prices.price) * 100 END,price_change_direction=CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price > latest_prices.price THEN 'up' WHEN EXCLUDED.price < latest_prices.price THEN 'down' ELSE 'flat' END,updated_slot=EXCLUDED.updated_slot,updated_at=EXCLUDED.updated_at`, [id, price, inverse, usd, now]);
          await client.query(`INSERT INTO evm_price_history (pool_address,price,inverse_price,updated_block,updated_at) VALUES ($1,$2,$3,$4,NOW())`, [id, price, inverse, block]);
          for (const timeframe of TIMEFRAMES) {
            const bucketStart = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds;
            await client.query(`INSERT INTO price_candles (pool_address,timeframe,bucket_start,open,high,low,close,volume,updated_at) VALUES ($1,$2,$3,$4,$4,$4,$4,NULL,NOW()) ON CONFLICT (pool_address,timeframe,bucket_start) DO UPDATE SET high=GREATEST(price_candles.high,EXCLUDED.high),low=LEAST(price_candles.low,EXCLUDED.low),close=EXCLUDED.close,updated_at=EXCLUDED.updated_at`, [id, timeframe.name, bucketStart, price]);
          }
          await client.query(`UPDATE latest_prices AS latest SET high_24h = rolling.high, low_24h = rolling.low FROM (SELECT MAX(high) AS high, MIN(low) AS low FROM price_candles WHERE pool_address = $1 AND timeframe = '1m' AND bucket_start >= $2) AS rolling WHERE latest.pool_address = $1`, [id, now - 24 * 60 * 60 * 1000]);
          await client.query('COMMIT');
        } catch (error) {
          await client.query('ROLLBACK').catch(() => undefined);
          throw error;
        } finally {
          client.release();
        }
        console.log(`[pancakeswap-infinity][price] ${pool.symbol0}/${pool.symbol1}=${price} block=${block}`);
      } catch (error) {
        console.error(`[pancakeswap-infinity][price] database write failed for ${id}:`, error);
        await failure(id, error);
      }
    }
    if (mode === 'prices' || promoted.has(id) || activity.events.length < SWAP_THRESHOLD || wallets < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(id);
    try {
      await pgPool.query(`INSERT INTO bsc_pancakeswap_infinity_cl_pools (address,pool_type,chain,manager,pool_id,currency0,currency0_symbol,currency0_decimals,currency1,currency1_symbol,currency1_decimals,hooks,fee,parameters,sqrt_price_x96,tick,transaction_hash,block_number,discovered_at,indexed_at) VALUES ($1,'pancakeswap_infinity_cl','bsc',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW(),NOW()) ON CONFLICT (address) DO UPDATE SET sqrt_price_x96=EXCLUDED.sqrt_price_x96,tick=EXCLUDED.tick,block_number=EXCLUDED.block_number,indexed_at=NOW()`, [id, PANCAKESWAP_INFINITY_CL_POOL_MANAGER, id, pool.currency0, pool.symbol0, pool.decimals0, pool.currency1, pool.symbol1, pool.decimals1, pool.hooks, fee, pool.parameters, sqrtPriceX96.toString(), tick, log.transactionHash ?? '', block]);
      console.log(`[pancakeswap-infinity] Promoted ${id} ${pool.symbol0}/${pool.symbol1} | swaps=${activity.events.length} | wallets=${wallets}`);
    } catch (error) { promoted.delete(id); console.error(`[pancakeswap-infinity] Pool write failed for ${id}:`, error); await failure(id, error); }
  };
  const connect = () => {
    const endpoint = endpoints[endpointIndex];
    socket = new WebSocket(endpoint);
    socket.on('open', () => { reconnectDelay = 1000; console.log(`[pancakeswap-infinity] websocket connected on ${endpoint}; manager=${PANCAKESWAP_INFINITY_CL_POOL_MANAGER}`); void rpc('eth_subscribe', ['logs', { address: PANCAKESWAP_INFINITY_CL_POOL_MANAGER, topics: [SWAP_TOPIC] }]).then((id) => console.log(`[pancakeswap-infinity] Swap subscription active (id=${id}).`)); });
    socket.on('message', (raw) => { try { const payload = JSON.parse(raw.toString()); if (payload.id !== undefined && pending.has(Number(payload.id))) { const request = pending.get(Number(payload.id))!; pending.delete(Number(payload.id)); payload.error ? request.reject(new Error(JSON.stringify(payload.error))) : request.resolve(payload.result); return; } if (payload.method === 'eth_subscription' && payload.params?.result) void process(payload.params.result).catch((error) => void failure(String(payload.params.result.topics?.[1] ?? ''), error)); } catch (error) { console.error('[pancakeswap-infinity] message error:', error); } });
    socket.on('error', (error) => console.error('[pancakeswap-infinity] websocket error:', error));
    socket.on('close', () => { const next = nextWebsocketEndpoint(endpoints, endpointIndex); endpointIndex = next.index; if (!reconnectTimer) { reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelay); reconnectDelay = Math.min(reconnectDelay * 2, 30_000); } });
  };
  const refreshBnb = async () => { try { bnbUsd = await getReferencePrice('bnb'); } catch (error) { console.warn('[pancakeswap-infinity] WBNB/USD refresh failed:', error); } };
  await refreshBnb();
  setInterval(() => void refreshBnb(), 180_000);
  connect();
}