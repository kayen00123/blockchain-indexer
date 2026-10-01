import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { getReferencePrice } from './reference_prices.js';

export type IndexedPool = {
  address: string;
  base_mint: string;
  quote_mint: string;
  base_decimals: number;
  quote_decimals: number;
  pool_base_token_account: string;
  pool_quote_token_account: string;
};

type VaultSubscription = { pool: IndexedPool; address: string };
export type RegisterPricePool = (pool: IndexedPool) => void;

const TOKEN_ACCOUNT_AMOUNT_OFFSET = 64;
const POOL_SCAN_INTERVAL_MS = 5000;
const MAX_POOLS_PER_SOCKET = 25;
const PRICE_EVENT_FILE = process.env.PRICE_EVENT_FILE ?? 'price-events.jsonl';
const SOL_PRICE_REFRESH_MS = 3 * 60 * 1000;
const CANDLE_TIMEFRAMES = [
  { name: '1m', milliseconds: 60_000 },
  { name: '5m', milliseconds: 5 * 60_000 },
  { name: '15m', milliseconds: 15 * 60_000 },
  { name: '1h', milliseconds: 60 * 60_000 },
  { name: '4h', milliseconds: 4 * 60 * 60_000 },
  { name: '1d', milliseconds: 24 * 60 * 60_000 },
] as const;

function validAddress(address: string): boolean {
  return Boolean(address) && address !== 'unknown' && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(address);
}

function decodeTokenAmount(data: unknown): bigint | null {
  if (Array.isArray(data) && typeof data[0] === 'string') {
    try {
      const bytes = Buffer.from(data[0], 'base64');
      if (bytes.length < TOKEN_ACCOUNT_AMOUNT_OFFSET + 8) return null;
      let amount = 0n;
      for (let index = 0; index < 8; index += 1) {
        amount |= BigInt(bytes[TOKEN_ACCOUNT_AMOUNT_OFFSET + index]) << BigInt(index * 8);
      }
      return amount;
    } catch {
      return null;
    }
  }

  const parsedAmount = (data as any)?.parsed?.info?.tokenAmount?.amount;
  return typeof parsedAmount === 'string' && /^\d+$/.test(parsedAmount) ? BigInt(parsedAmount) : null;
}

function toDecimalAmount(rawAmount: bigint, decimals: number): number {
  return Number(rawAmount) / 10 ** decimals;
}

export async function startPriceFetcher(pgPool: PgPool, websocketUrl: string): Promise<RegisterPricePool> {
  if (!websocketUrl) {
    console.warn('SHYFT_WS_URL is not configured; websocket price fetcher is disabled.');
    return () => undefined;
  }

  const snapshots = new Map<string, { rawAmount: bigint; slot: number }>();
  const subscriptions = new Map<number, VaultSubscription>();
  const pendingRequests = new Map<number, VaultSubscription>();
  const pools = new Map<string, IndexedPool>();
  let requestId = 1;
  let reconnectDelayMs = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let socket: WebSocket | undefined;
  let solUsdPrice: number | null = null;
  let solPriceUpdatedAt: number | null = null;

  const recordPriceEvent = async (event: Record<string, unknown>) => {
    try {
      await appendFile(PRICE_EVENT_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8');
    } catch (error) {
      console.error('Failed to write price event record:', error);
    }
  };

  const refreshSolUsdPrice = async () => {
    try {
      const value = await getReferencePrice('sol');
      if (value === null) throw new Error('No SOL reference price available');
      solUsdPrice = value;
      solPriceUpdatedAt = Date.now();
      console.log(`[price-reference] SOL/USD=${value}`);
    } catch (error) {
      console.warn('SOL/USD reference refresh failed; retaining last good value:', error);
      void recordPriceEvent({ type: 'error', stage: 'solPriceReference', error: String(error) });
    }
  };

  const getTokenPriceUsd = (pool: IndexedPool, price: number): number | null => {
    if (pool.quote_mint === 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') return price;
    if (pool.quote_mint === 'So11111111111111111111111111111111111111112' && solUsdPrice !== null) return price * solUsdPrice;
    if (pool.base_mint === 'So11111111111111111111111111111111111111112' && solUsdPrice !== null) return solUsdPrice;
    return null;
  };

  const upsertPrice = async (pool: IndexedPool, slot: number) => {
    const base = snapshots.get(pool.pool_base_token_account);
    const quote = snapshots.get(pool.pool_quote_token_account);
    if (!base || !quote) return;

    const baseReserve = toDecimalAmount(base.rawAmount, pool.base_decimals);
    const quoteReserve = toDecimalAmount(quote.rawAmount, pool.quote_decimals);
    if (!Number.isFinite(baseReserve) || !Number.isFinite(quoteReserve) || baseReserve <= 0 || quoteReserve <= 0) return;

    const price = quoteReserve / baseReserve;
    const inversePrice = baseReserve / quoteReserve;
    if (!Number.isFinite(price) || !Number.isFinite(inversePrice) || price <= 0 || inversePrice <= 0) return;
    const tokenPriceUsd = getTokenPriceUsd(pool, price);

    await recordPriceEvent({
      type: 'price',
      poolAddress: pool.address,
      price,
      inversePrice,
      baseReserve,
      quoteReserve,
      slot,
      tokenPriceUsd,
      solUsdPrice,
      solPriceUpdatedAt,
    });

    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
      `INSERT INTO latest_prices (
        pool_address, price, inverse_price, price_change, price_change_percent,
        price_change_direction, fdv_usd, token_price_usd, total_supply, supply_basis,
        base_reserve, quote_reserve, updated_slot, updated_at
      ) VALUES (
         $1, $2, $3, NULL, NULL, NULL,
        NULL, $7, NULL, 'pool_reserves', $4, $5, $6, NOW()
      ) ON CONFLICT (pool_address) DO UPDATE SET
        price = EXCLUDED.price,
        inverse_price = EXCLUDED.inverse_price,
        token_price_usd = EXCLUDED.token_price_usd,
         price_change = EXCLUDED.price - latest_prices.price,
         price_change_percent = CASE WHEN latest_prices.price IS NULL OR latest_prices.price = 0 THEN NULL
                   ELSE ((EXCLUDED.price - latest_prices.price) / latest_prices.price) * 100 END,
         price_change_direction = CASE WHEN latest_prices.price IS NULL THEN NULL
                     WHEN EXCLUDED.price > latest_prices.price THEN 'up'
                     WHEN EXCLUDED.price < latest_prices.price THEN 'down'
                     ELSE 'flat' END,
         price_change_24h = (
           SELECT CASE
             WHEN ref.open IS NULL OR ref.open = 0 THEN NULL
             ELSE ((EXCLUDED.price - ref.open) / ref.open) * 100
           END
           FROM (
             SELECT open
             FROM price_candles
             WHERE pool_address = EXCLUDED.pool_address
               AND timeframe = '1h'
               AND bucket_start >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '25 hours')) * 1000
             ORDER BY bucket_start ASC
             LIMIT 1
           ) ref
         ),
        base_reserve = EXCLUDED.base_reserve,
        quote_reserve = EXCLUDED.quote_reserve,
        updated_slot = EXCLUDED.updated_slot,
        updated_at = EXCLUDED.updated_at`,
        [pool.address, price, inversePrice, baseReserve, quoteReserve, slot, tokenPriceUsd],
      );

      const now = Date.now();
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
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    console.log(`[price] ${pool.address} price=${price} inverse=${inversePrice} slot=${slot}`);
  };

  const subscribe = (entry: VaultSubscription) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    const id = requestId++;
    pendingRequests.set(id, entry);
    socket.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [entry.address, { commitment: 'confirmed', encoding: 'base64' }] }));
  };

  const registerPool: RegisterPricePool = (pool) => {
    if (!validAddress(pool.pool_base_token_account) || !validAddress(pool.pool_quote_token_account)) return;
    pools.set(pool.address, pool);
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    const activeAddresses = new Set([...subscriptions.values()].map((entry) => entry.address));
    for (const address of [pool.pool_base_token_account, pool.pool_quote_token_account]) {
      if (!activeAddresses.has(address)) subscribe({ pool, address });
    }
  };

  const connect = () => {
    socket = new WebSocket(websocketUrl);
    subscriptions.clear();
    pendingRequests.clear();
    socket.on('open', () => {
      reconnectDelayMs = 1000;
      console.log('Shyft websocket price stream connected.');
      for (const pool of pools.values()) {
        subscribe({ pool, address: pool.pool_base_token_account });
        subscribe({ pool, address: pool.pool_quote_token_account });
      }
    });
    socket.on('message', (raw) => {
      let payload: any;
      try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) {
        console.warn('Shyft account subscription failed:', payload.error);
        void recordPriceEvent({ type: 'error', stage: 'accountSubscribe', error: payload.error });
        pendingRequests.delete(payload.id);
        return;
      }
      if (payload.id !== undefined && payload.result !== undefined) {
        const entry = pendingRequests.get(payload.id);
        pendingRequests.delete(payload.id);
        if (entry) subscriptions.set(payload.result, entry);
        return;
      }
      if (payload.method !== 'accountNotification') return;
      const entry = subscriptions.get(payload.params?.subscription);
      const result = payload.params?.result;
      if (!entry || !result) return;
      const rawAmount = decodeTokenAmount(result.value?.data);
      const slot = Number(result.context?.slot ?? 0);
      if (rawAmount === null || !Number.isSafeInteger(slot) || slot <= 0) {
        void recordPriceEvent({ type: 'error', stage: 'accountNotification', poolAddress: entry.pool.address, account: entry.address, message: 'Invalid reserve amount or slot' });
        return;
      }
      snapshots.set(entry.address, { rawAmount, slot });
      void upsertPrice(entry.pool, slot).catch((error) => {
        console.error('Price upsert failed:', error);
        void recordPriceEvent({ type: 'error', stage: 'priceUpsert', poolAddress: entry.pool.address, error: String(error) });
      });
    });
    socket.on('error', (error) => {
      console.error('Shyft price websocket error:', error);
      void recordPriceEvent({ type: 'error', stage: 'websocket', error: String(error) });
    });
    socket.on('close', (code, reason) => {
      console.warn(`Shyft price websocket closed: ${code} ${reason.toString()}`);
      void recordPriceEvent({ type: 'error', stage: 'websocketClose', code, reason: reason.toString() });
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    });
  };

  const refreshPools = async () => {
    try {
      const result = await pgPool.query<IndexedPool>(`SELECT address, base_mint, quote_mint, base_decimals, quote_decimals, pool_base_token_account, pool_quote_token_account FROM pools WHERE pool_base_token_account <> 'unknown' AND pool_quote_token_account <> 'unknown'`);
      pools.clear();
      for (const pool of result.rows) registerPool(pool);
      if (!socket || socket.readyState !== WebSocket.OPEN) return;
      const activeAddresses = new Set([...subscriptions.values()].map((entry) => entry.address));
      for (const pool of pools.values()) {
        for (const address of [pool.pool_base_token_account, pool.pool_quote_token_account]) {
          if (!activeAddresses.has(address)) subscribe({ pool, address });
        }
      }
    } catch (error) {
      console.error('Price pool scan failed:', error);
      void recordPriceEvent({ type: 'error', stage: 'poolScan', error: String(error) });
    }
  };

  connect();
  await refreshSolUsdPrice();
  setInterval(() => void refreshSolUsdPrice(), SOL_PRICE_REFRESH_MS);
  await refreshPools();
  setInterval(() => void refreshPools(), POOL_SCAN_INTERVAL_MS);
  console.log(`Websocket price fetcher started; target capacity is ${MAX_POOLS_PER_SOCKET} pools per socket.`);
  console.log(`Price events and errors are appended to ${PRICE_EVENT_FILE}.`);
  return registerPool;
}
