import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { PublicKey } from '@solana/web3.js';
import { getReferencePrice } from './reference_prices.js';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

export type RaydiumPricePool = {
  address: string;
  token0: string;
  token1: string;
  token0_decimals: number;
  token1_decimals: number;
  token0_vault: string;
  token1_vault: string;
  pool_type?: 'raydium_cpmm' | 'raydium_clmm';
};

export type RegisterRaydiumPricePool = (pool: RaydiumPricePool) => void;
type Subscription = { pool: RaydiumPricePool; account: string; kind: 'vault' | 'clmm-state' };

const AMOUNT_OFFSET = 64;
const CLMM_SQRT_PRICE_OFFSET = 253;
const SCAN_INTERVAL_MS = 5000;
const EVENT_FILE = process.env.RAYDIUM_PRICE_EVENT_FILE ?? 'raydium-price-events.jsonl';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SOL_PRICE_REFRESH_MS = 3 * 60 * 1000;
const CANDLE_TIMEFRAMES = [
  { name: '1m', milliseconds: 60_000 },
  { name: '5m', milliseconds: 5 * 60_000 },
  { name: '15m', milliseconds: 15 * 60_000 },
  { name: '1h', milliseconds: 60 * 60_000 },
  { name: '4h', milliseconds: 4 * 60 * 60_000 },
  { name: '1d', milliseconds: 24 * 60 * 60_000 },
] as const;

function validAddress(value: string): boolean {
  try { new PublicKey(value); return value !== 'unknown'; } catch { return false; }
}

function decodeAmount(data: unknown): bigint | null {
  if (!Array.isArray(data) || typeof data[0] !== 'string') return null;
  try {
    const bytes = Buffer.from(data[0], 'base64');
    if (bytes.length < AMOUNT_OFFSET + 8) return null;
    let amount = 0n;
    for (let index = 0; index < 8; index += 1) amount |= BigInt(bytes[AMOUNT_OFFSET + index]) << BigInt(index * 8);
    return amount;
  } catch { return null; }
}

function decimalAmount(raw: bigint, decimals: number): number {
  return Number(raw) / 10 ** decimals;
}

function decodeClmmState(data: unknown): { sqrtPriceX64: bigint; token0Decimals: number; token1Decimals: number } | null {
  if (!Array.isArray(data) || typeof data[0] !== 'string') return null;
  try {
    const bytes = Buffer.from(data[0], 'base64');
    if (bytes.length < CLMM_SQRT_PRICE_OFFSET + 16) return null;
    let sqrtPriceX64 = 0n;
    for (let index = 0; index < 16; index += 1) sqrtPriceX64 |= BigInt(bytes[CLMM_SQRT_PRICE_OFFSET + index]) << BigInt(index * 8);
    return { sqrtPriceX64, token0Decimals: bytes[233], token1Decimals: bytes[234] };
  } catch { return null; }
}

export async function startRaydiumPriceFetcher(pgPool: PgPool, websocketUrl: WebsocketEndpointInput): Promise<RegisterRaydiumPricePool> {
  const endpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  if (endpoints.length === 0) {
    console.warn('[raydium-price] RAYDIUM_PRICE_WS_URL is not configured; pricing is disabled.');
    return () => undefined;
  }

  const snapshots = new Map<string, { raw: bigint; slot: number }>();
  const clmmPrices = new Map<string, { price: number; slot: number }>();
  const pools = new Map<string, RaydiumPricePool>();
  const subscriptions = new Map<number, Subscription>();
  const pending = new Map<number, Subscription>();
  const requestedVaults = new Set<string>();
  let socket: WebSocket | undefined;
  let requestId = 1;
  let reconnectDelayMs = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let endpointIndex = 0;
  let solUsdPrice: number | null = null;
  let solPriceUpdatedAt: number | null = null;

  const record = async (event: Record<string, unknown>) => {
    try { await appendFile(EVENT_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8'); }
    catch (error) { console.error('[raydium-price] event log failed:', error); }
  };

  const refreshSolUsdPrice = async () => {
    try {
      const value = await getReferencePrice('sol');
      if (value === null) throw new Error('No SOL reference price available');
      solUsdPrice = value;
      solPriceUpdatedAt = Date.now();
      console.log(`[raydium-price] SOL/USD=${value}`);
    } catch (error) {
      console.warn('[raydium-price] SOL/USD refresh failed; retaining last good value:', error);
      void record({ type: 'error', stage: 'solPriceReference', error: String(error) });
    }
  };

  const getTokenPriceUsd = (pool: RaydiumPricePool, price: number, inversePrice: number): number | null => {
    if (pool.token1 === USDC_MINT) return price;
    if (pool.token0 === USDC_MINT) return inversePrice;
    if (pool.token1 === SOL_MINT && solUsdPrice !== null) return price * solUsdPrice;
    if (pool.token0 === SOL_MINT && solUsdPrice !== null) return inversePrice * solUsdPrice;
    return null;
  };

  const upsert = async (pool: RaydiumPricePool, slot: number) => {
    let reserve0: number;
    let reserve1: number;
    let price: number;
    let inversePrice: number;
    if (pool.pool_type === 'raydium_clmm') {
      const clmmPrice = clmmPrices.get(pool.address);
      if (!clmmPrice) return;
      price = clmmPrice.price;
      inversePrice = 1 / price;
      reserve0 = 0;
      reserve1 = 0;
    } else {
      const token0 = snapshots.get(pool.token0_vault);
      const token1 = snapshots.get(pool.token1_vault);
      if (!token0 || !token1) return;
      reserve0 = decimalAmount(token0.raw, pool.token0_decimals);
      reserve1 = decimalAmount(token1.raw, pool.token1_decimals);
      if (!Number.isFinite(reserve0) || !Number.isFinite(reserve1) || reserve0 <= 0 || reserve1 <= 0) return;
      price = reserve1 / reserve0;
      inversePrice = reserve0 / reserve1;
    }
    if (!Number.isFinite(price) || !Number.isFinite(inversePrice) || price <= 0 || inversePrice <= 0) return;
    const tokenPriceUsd = getTokenPriceUsd(pool, price, inversePrice);
    const now = Date.now();

    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO latest_prices (
          pool_address, price, inverse_price, price_change, price_change_percent,
          price_change_direction, fdv_usd, token_price_usd, total_supply, supply_basis,
          base_reserve, quote_reserve, updated_slot, updated_at
        ) VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $7, NULL, $8, $4, $5, $6, NOW())
        ON CONFLICT (pool_address) DO UPDATE SET
          price = EXCLUDED.price,
          inverse_price = EXCLUDED.inverse_price,
          token_price_usd = EXCLUDED.token_price_usd,
          price_change = EXCLUDED.price - latest_prices.price,
          price_change_percent = CASE WHEN latest_prices.price IS NULL OR latest_prices.price = 0 THEN NULL ELSE ((EXCLUDED.price - latest_prices.price) / latest_prices.price) * 100 END,
          price_change_direction = CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price > latest_prices.price THEN 'up' WHEN EXCLUDED.price < latest_prices.price THEN 'down' ELSE 'flat' END,
          base_reserve = EXCLUDED.base_reserve,
          quote_reserve = EXCLUDED.quote_reserve,
          updated_slot = EXCLUDED.updated_slot,
          updated_at = EXCLUDED.updated_at`,
        [pool.address, price, inversePrice, reserve0, reserve1, slot, tokenPriceUsd, pool.pool_type === 'raydium_clmm' ? 'raydium_clmm_vaults' : 'raydium_cpmm_reserves'],
      );
      for (const timeframe of CANDLE_TIMEFRAMES) {
        const bucketStart = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds;
        await client.query(
          `INSERT INTO price_candles (
             pool_address, timeframe, bucket_start, open, high, low, close, volume, updated_at
           ) VALUES ($1, $2, $3, $4, $4, $4, $4, NULL, NOW())
           ON CONFLICT (pool_address, timeframe, bucket_start) DO UPDATE SET
             high = GREATEST(price_candles.high, EXCLUDED.high),
             low = LEAST(price_candles.low, EXCLUDED.low),
             close = EXCLUDED.close,
             updated_at = EXCLUDED.updated_at`,
          [pool.address, timeframe.name, bucketStart, price],
        );
      }
      await client.query(
        `UPDATE latest_prices AS latest
            SET high_24h = rolling.high,
                low_24h = rolling.low
           FROM (
             SELECT MAX(high) AS high, MIN(low) AS low
               FROM price_candles
              WHERE pool_address = $1
                AND timeframe = '1m'
                AND bucket_start >= $2
           ) AS rolling
          WHERE latest.pool_address = $1`,
        [pool.address, now - 24 * 60 * 60 * 1000],
      );
      await client.query('COMMIT');
      await record({ type: 'price', poolAddress: pool.address, poolType: pool.pool_type ?? 'raydium_cpmm', price, inversePrice, tokenPriceUsd, solUsdPrice, solPriceUpdatedAt, high24h: price, low24h: price, token0Reserve: reserve0, token1Reserve: reserve1, slot });
      console.log(`[raydium-price] ${pool.address} price=${price} inverse=${inversePrice} slot=${slot}`);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally { client.release(); }
  };

  const subscribe = (entry: Subscription) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    if (requestedVaults.has(entry.account)) return;
    requestedVaults.add(entry.account);
    const id = requestId++;
    pending.set(id, entry);
    socket.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [entry.account, { commitment: 'confirmed', encoding: 'base64' }] }));
  };

  const register: RegisterRaydiumPricePool = (pool) => {
    if (!validAddress(pool.token0_vault) || !validAddress(pool.token1_vault)) return;
    if (pool.token0 !== SOL_MINT && pool.token0 !== USDC_MINT && pool.token1 !== SOL_MINT && pool.token1 !== USDC_MINT) return;
    pools.set(pool.address, pool);
    if (pool.pool_type === 'raydium_clmm') {
      subscribe({ pool, account: pool.address, kind: 'clmm-state' });
    } else {
      subscribe({ pool, account: pool.token0_vault, kind: 'vault' });
      subscribe({ pool, account: pool.token1_vault, kind: 'vault' });
    }
  };

  const connect = () => {
    const endpoint = endpoints[endpointIndex] ?? endpoints[0];
    if (!endpoint) return;
    socket = new WebSocket(endpoint);
    subscriptions.clear();
    pending.clear();
    requestedVaults.clear();
    socket.on('open', () => {
      reconnectDelayMs = 1000;
      console.log('[raydium-price] Chainstack websocket connected.');
      for (const pool of pools.values()) {
        if (pool.pool_type === 'raydium_clmm') subscribe({ pool, account: pool.address, kind: 'clmm-state' });
        else {
          subscribe({ pool, account: pool.token0_vault, kind: 'vault' });
          subscribe({ pool, account: pool.token1_vault, kind: 'vault' });
        }
      }
    });
    socket.on('message', (raw) => {
      let payload: any;
      try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) {
        void record({ type: 'error', stage: 'accountSubscribe', error: payload.error });
        pending.delete(payload.id);
        return;
      }
      if (payload.id !== undefined && payload.result !== undefined) {
        const entry = pending.get(payload.id);
        pending.delete(payload.id);
        if (entry) subscriptions.set(payload.result, entry);
        return;
      }
      if (payload.method !== 'accountNotification') return;
      const entry = subscriptions.get(payload.params?.subscription);
      const result = payload.params?.result;
      if (!entry || !result) return;
      const slot = Number(result.context?.slot ?? 0);
      if (slot <= 0) {
        void record({ type: 'error', stage: 'accountNotification', poolAddress: entry.pool.address, account: entry.account, message: 'Invalid slot payload' });
        return;
      }
      if (entry.kind === 'clmm-state') {
        const state = decodeClmmState(result.value?.data);
        if (state === null || state.sqrtPriceX64 === 0n || state.token0Decimals > 18 || state.token1Decimals > 18) {
          void record({ type: 'error', stage: 'accountNotification', poolAddress: entry.pool.address, account: entry.account, message: 'Invalid CLMM sqrt price payload' });
          return;
        }
        entry.pool.token0_decimals = state.token0Decimals;
        entry.pool.token1_decimals = state.token1Decimals;
        const sqrtPriceNumber = Number(state.sqrtPriceX64) / 2 ** 64;
        const price = sqrtPriceNumber ** 2 * 10 ** (state.token0Decimals - state.token1Decimals);
        if (!Number.isFinite(price) || price <= 0) return;
        clmmPrices.set(entry.pool.address, { price, slot });
        void pgPool.query(
          'UPDATE raydium_pools SET token_mint_0_decimals = $2, token_mint_1_decimals = $3, updated_slot = $4 WHERE address = $1 AND pool_type = \'raydium_clmm\'',
          [entry.pool.address, state.token0Decimals, state.token1Decimals, slot],
        ).catch((error) => console.warn('[raydium-price] CLMM decimals update failed:', error));
      } else {
        const rawAmount = decodeAmount(result.value?.data);
        if (rawAmount === null) {
          void record({ type: 'error', stage: 'accountNotification', poolAddress: entry.pool.address, account: entry.account, message: 'Invalid reserve payload' });
          return;
        }
        snapshots.set(entry.account, { raw: rawAmount, slot });
      }
      void upsert(entry.pool, slot).catch(async (error) => {
        console.error('[raydium-price] upsert failed:', error);
        await record({ type: 'error', stage: 'priceUpsert', poolAddress: entry.pool.address, error: String(error) });
      });
    });
    socket.on('error', (error) => { console.error('[raydium-price] websocket error:', error); void record({ type: 'error', stage: 'websocket', error: String(error) }); });
    socket.on('close', (code, reason) => {
      const next = nextWebsocketEndpoint(endpoints, endpointIndex);
      endpointIndex = next.index;
      console.warn(`[raydium-price] websocket closed: ${code} ${reason.toString()}`);
      void record({ type: 'error', stage: 'websocketClose', code, reason: reason.toString() });
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    });
  };

  const refresh = async () => {
    try {
      const result = await pgPool.query<RaydiumPricePool>(`
        SELECT address, token0, token1, token0_decimals, token1_decimals, token0_vault, token1_vault, 'raydium_cpmm' AS pool_type
        FROM raydium_cpmm_pools
        WHERE token0 IN ('${SOL_MINT}', '${USDC_MINT}') OR token1 IN ('${SOL_MINT}', '${USDC_MINT}')
        UNION ALL
         SELECT address, token_mint_0 AS token0, token_mint_1 AS token1,
               token_mint_0_decimals AS token0_decimals, token_mint_1_decimals AS token1_decimals,
           token_vault_0 AS token0_vault, token_vault_1 AS token1_vault, 'raydium_clmm' AS pool_type
        FROM raydium_pools
        WHERE pool_type = 'raydium_clmm'
          AND (token_mint_0 IN ('${SOL_MINT}', '${USDC_MINT}') OR token_mint_1 IN ('${SOL_MINT}', '${USDC_MINT}'))`);
      for (const pool of result.rows) register(pool);
    } catch (error) {
      console.error('[raydium-price] pool scan failed:', error);
      void record({ type: 'error', stage: 'poolScan', error: String(error) });
    }
  };

  connect();
  await refreshSolUsdPrice();
  setInterval(() => void refreshSolUsdPrice(), SOL_PRICE_REFRESH_MS);
  await refresh();
  setInterval(() => void refresh(), SCAN_INTERVAL_MS);
  console.log(`[raydium-price] websocket pricing started; event log=${EVENT_FILE}`);
  return register;
}
