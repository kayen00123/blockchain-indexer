import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { getReferencePrice } from './reference_prices.js';
import { nextWebsocketEndpoint, readWebsocketEndpoints } from './evm_ws_rotation.js';

export const PANCAKESWAP_V3_FACTORY = process.env.PANCAKESWAP_V3_FACTORY ?? '0x0bc1d4e3627e8a4d6a5f9bfefe0b0d8f2d9d8a41';
const SWAP_TOPIC = '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83';
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const WBNB = (process.env.PANCAKESWAP_V3_WBNB ?? '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c').toLowerCase();
const USDT = (process.env.PANCAKESWAP_V3_USDT ?? '0x55d398326f99059ff775485246999027b3197955').toLowerCase();
const BNB_PRICE_URL = process.env.BNB_PRICE_URL ?? 'https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c';
const SWAP_THRESHOLD = Number(process.env.PANCAKESWAP_V3_SWAP_THRESHOLD ?? process.env.PANCAKESWAP_V2_SWAP_THRESHOLD ?? process.env.EVM_PROMOTION_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 100);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.PANCAKESWAP_V3_MIN_UNIQUE_WALLETS ?? process.env.PANCAKESWAP_V2_MIN_UNIQUE_WALLETS ?? process.env.EVM_PROMOTION_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 20);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const FAILURE_FILE = process.env.PANCAKESWAP_V3_FAILURE_FILE ?? 'pancakeswap-v3-failures.jsonl';
const PRICE_FILE = process.env.PANCAKESWAP_V3_PRICE_EVENT_FILE ?? 'pancakeswap-v3-price-events.jsonl';
const TIMEFRAMES = [{ name: '1m', milliseconds: 60_000 }, { name: '5m', milliseconds: 300_000 }, { name: '15m', milliseconds: 900_000 }, { name: '1h', milliseconds: 3_600_000 }, { name: '4h', milliseconds: 14_400_000 }, { name: '1d', milliseconds: 86_400_000 }] as const;

type Pair = { address: string; token0: string; token1: string; symbol0: string; symbol1: string; decimals0: number; decimals1: number; fee: number; block: number };
type Activity = { pair: Pair; events: Array<{ wallet: string; timestamp: number }> };

function word(data: string, index: number): bigint {
  if (typeof data !== 'string' || !/^0x[0-9a-f]+$/i.test(data) || data.length < 2 + (index + 1) * 64) {
    throw new Error(`Invalid ABI data for word ${index}`);
  }
  return BigInt(`0x${data.slice(2 + index * 64, 2 + (index + 1) * 64)}`);
}

function addressWord(data: string): string {
  return typeof data === 'string' && /^0x[0-9a-f]{40,}$/i.test(data) ? `0x${data.slice(-40)}`.toLowerCase() : ZERO_ADDRESS;
}

function decodeString(value: unknown): string {
  if (typeof value !== 'string' || value === '0x') return '';
  try {
    const data = value.startsWith('0x') ? value : `0x${value}`;
    const offset = Number(word(data, 0));
    if (!Number.isInteger(offset) || offset < 0 || offset % 32 !== 0) return '';
    const length = Number(word(data, offset / 32));
    if (!Number.isInteger(length) || length < 0 || length > 256) return '';
    const payloadStart = 2 + (offset + 32) * 2;
    const payload = data.slice(payloadStart, payloadStart + length * 2);
    if (!payload) return '';
    return Buffer.from(payload, 'hex').toString('utf8').replace(/\0/g, '');
  } catch {
    return '';
  }
}

function usableSymbol(value: string): boolean {
  return value.length > 0 && value.length <= 64 && !/[\u0000-\u001f\u007f]/.test(value);
}

function decodeUint(value: unknown): number {
  if (typeof value !== 'string' || value === '0x' || !/^0x[0-9a-f]+$/i.test(value)) return 0;
  try {
    const n = Number(BigInt(value));
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0;
  }
}

function supportedPair(token0: string, token1: string): boolean {
  return token0 === WBNB || token1 === WBNB || token0 === USDT || token1 === USDT;
}

function tokenPriceUsd(pair: Pair, price: number, bnbUsd: number | null): number | null {
  if (pair.token1 === USDT) return price;
  if (pair.token0 === USDT) return 1 / price;
  if (pair.token1 === WBNB && bnbUsd) return price * bnbUsd;
  if (pair.token0 === WBNB && bnbUsd) return bnbUsd / price;
  return null;
}

function computePriceFromSqrt(sqrtPriceX96: bigint, decimals0: number, decimals1: number): number {
  const q96 = 2n ** 96n;
  const sqrtPrice = Number(sqrtPriceX96) / Number(q96);
  const raw = sqrtPrice * sqrtPrice;
  const baseAdjustment = 10 ** Math.max(0, decimals0 - decimals1);
  const quoteAdjustment = 10 ** Math.max(0, decimals1 - decimals0);
  const adjusted = decimals0 >= decimals1 ? raw * baseAdjustment : raw / quoteAdjustment;
  return adjusted;
}

async function failure(pair: string, error: unknown): Promise<void> {
  try {
    await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: pair, error: String(error) })}\n`, 'utf8');
  } catch {
    /* best effort */
  }
}

export function createPancakeSwapV3Processor(pgPool: PgPool, registerPricePair?: (pair: Pair) => void, call?: (method: string, params: unknown[]) => Promise<any>) {
  const activities = new Map<string, Activity>();
  const promoted = new Set<string>();

  return async (log: any, blockNumber: number, source = 'websocket') => {
    const pairAddress = String(log?.address ?? '').toLowerCase();
    const topics = log?.topics;
    if (!/^0x[0-9a-f]{40}$/.test(pairAddress) || !Array.isArray(topics) || topics[0]?.toLowerCase() !== SWAP_TOPIC) return;

    const wallet = addressWord(topics[1] ?? '');
    if (wallet === ZERO_ADDRESS) return;

    let pair = activities.get(pairAddress)?.pair;
    if (!pair) {
      if (!call) return;
      const [token0Raw, token1Raw] = await Promise.all([
        call('eth_call', [{ to: pairAddress, data: '0x0dfe1681' }, 'latest']),
        call('eth_call', [{ to: pairAddress, data: '0xd21220a7' }, 'latest']),
      ]);

      const token0 = addressWord(String(token0Raw ?? ''));
      const token1 = addressWord(String(token1Raw ?? ''));
      if (!supportedPair(token0, token1)) return;

      const [symbol0Raw, symbol1Raw, decimals0Raw, decimals1Raw] = await Promise.all([
        call('eth_call', [{ to: token0, data: '0x95d89b41' }, 'latest']),
        call('eth_call', [{ to: token1, data: '0x95d89b41' }, 'latest']),
        call('eth_call', [{ to: token0, data: '0x313ce567' }, 'latest']),
        call('eth_call', [{ to: token1, data: '0x313ce567' }, 'latest']),
      ]);

      pair = {
        address: pairAddress,
        token0,
        token1,
        symbol0: decodeString(symbol0Raw) || 'TOKEN0',
        symbol1: decodeString(symbol1Raw) || 'TOKEN1',
        decimals0: decodeUint(decimals0Raw),
        decimals1: decodeUint(decimals1Raw),
        fee: 0,
        block: blockNumber,
      };

      activities.set(pairAddress, { pair, events: [] });
    } else if (call && (!usableSymbol(pair.symbol0) || !usableSymbol(pair.symbol1))) {
      const [symbol0Raw, symbol1Raw] = await Promise.all([
        call('eth_call', [{ to: pair.token0, data: '0x95d89b41' }, 'latest']),
        call('eth_call', [{ to: pair.token1, data: '0x95d89b41' }, 'latest']),
      ]);
      pair.symbol0 = decodeString(symbol0Raw) || pair.symbol0;
      pair.symbol1 = decodeString(symbol1Raw) || pair.symbol1;
    }

    const activity = activities.get(pairAddress)!;
    const now = Date.now();
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet, timestamp: now });

    const wallets = new Set(activity.events.map((event) => event.wallet)).size;
    console.log(`[pancakeswap-v3][${source}] Tracked pool ${pairAddress} | swaps=${activity.events.length} | walletCount=${wallets}`);

    if (promoted.has(pairAddress) || activity.events.length < SWAP_THRESHOLD || wallets < UNIQUE_WALLET_THRESHOLD) return pair;

    promoted.add(pairAddress);
    try {
      await pgPool.query(
        `INSERT INTO bsc_pancakeswap_v3_pools (address,pool_type,chain,factory,token0,token0_symbol,token0_decimals,token1,token1_symbol,token1_decimals,fee,tick_spacing,transaction_hash,block_number,discovered_at,indexed_at) VALUES ($1,'pancakeswap_v3','bsc',$2,$3,$4,$5,$6,$7,$8,$9,0,$10,$11,NOW(),NOW()) ON CONFLICT (address) DO UPDATE SET token0=EXCLUDED.token0,token0_symbol=EXCLUDED.token0_symbol,token0_decimals=EXCLUDED.token0_decimals,token1=EXCLUDED.token1,token1_symbol=EXCLUDED.token1_symbol,token1_decimals=EXCLUDED.token1_decimals,fee=EXCLUDED.fee,block_number=EXCLUDED.block_number,indexed_at=NOW()`,
        [pair.address, PANCAKESWAP_V3_FACTORY, pair.token0, pair.symbol0, pair.decimals0, pair.token1, pair.symbol1, pair.decimals1, 0, log.transactionHash ?? '', blockNumber],
      );
      registerPricePair?.(pair);
      console.log(`[pancakeswap-v3] Promoted ${pair.address} ${pair.symbol0}/${pair.symbol1} | swaps=${activity.events.length} | wallets=${wallets}`);
    } catch (error) {
      promoted.delete(pairAddress);
      console.error(`[pancakeswap-v3] Promotion failed for ${pairAddress}:`, error);
      await failure(pairAddress, error);
    }
    return pair;
  };
}

export async function startPancakeSwapV3Indexer(pgPool: PgPool, websocketUrl: string | string[], priceFile = PRICE_FILE): Promise<void> {
  const endpoints = Array.isArray(websocketUrl) ? websocketUrl : readWebsocketEndpoints('PANCAKESWAP_V3', websocketUrl);
  if (endpoints.length === 0) {
    console.warn('[pancakeswap-v3] No websocket endpoints are configured; indexing is disabled.');
    return;
  }
  if (endpoints.some((endpoint) => endpoint.includes('YOUR_') || endpoint.includes('your-'))) {
    console.warn('[pancakeswap-v3] A websocket endpoint still contains a placeholder key; indexing is disabled.');
    return;
  }

  const pairs = new Map<string, Pair>();
  const subscriptions = new Map<number, { kind: 'swap'; pair?: Pair }>();
  let socket: WebSocket | undefined;
  let requestId = 1;
  let reconnectDelay = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let bnbUsd: number | null = null;
  let endpointIndex = 0;
  const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();

  const rpc = (method: string, params: unknown[]) => new Promise<any>((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return reject(new Error('PancakeSwap V3 websocket is not open'));
    const id = requestId++;
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
    setTimeout(() => {
      const request = pending.get(id);
      if (request) {
        pending.delete(id);
        request.reject(new Error(`RPC timeout for ${method}`));
      }
    }, 15_000);
  });

  const subscribe = (filters: unknown[], entry: { kind: 'swap'; pair?: Pair }) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    void rpc('eth_subscribe', ['logs', filters[0] ?? {}]).then((id) => {
      subscriptions.set(Number(id), entry);
      console.log(`[pancakeswap-v3] ${entry.kind} subscription active (id=${id}).`);
    }).catch((error) => console.error('[pancakeswap-v3] subscription failed:', error));
  };

  const register = (pair: Pair) => {
    if (!pairs.has(pair.address)) pairs.set(pair.address, pair);
  };

  let recordQueue = Promise.resolve();
  const record = (event: Record<string, unknown>) => {
    recordQueue = recordQueue.then(() => appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8')).catch((error) => {
      console.error('[pancakeswap-v3-price] event log failed:', error);
    });
    return recordQueue;
  };

  const price = async (pair: Pair, sqrtPriceX96: bigint, block: number) => {
    if (sqrtPriceX96 === 0n) return;
    const q96 = 2n ** 96n;
    const sqrtPrice = Number(sqrtPriceX96) / Number(q96);
    const rawPrice = sqrtPrice * sqrtPrice;
    const value = pair.decimals0 >= pair.decimals1 ? rawPrice * 10 ** (pair.decimals0 - pair.decimals1) : rawPrice / 10 ** (pair.decimals1 - pair.decimals0);
    if (!Number.isFinite(value) || value <= 0) return;
    const inverse = 1 / value;
    const usd = tokenPriceUsd(pair, value, bnbUsd);
    const now = Date.now();

    await record({
      type: 'price',
      poolType: 'pancakeswap_v3',
      poolAddress: pair.address,
      pair: `${pair.symbol0}/${pair.symbol1}`,
      price: value,
      inversePrice: inverse,
      tokenPriceUsd: usd,
      sqrtPriceX96: sqrtPriceX96.toString(),
      block,
      fee: pair.fee,
    });

    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO bsc_pancakeswap_v3_prices (pool_address,price,inverse_price,base_token,quote_token,sqrt_price_x96,liquidity,tick,updated_block,updated_at) VALUES ($1,$2,$3,$4,$5,$6,0,0,$7,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,sqrt_price_x96=EXCLUDED.sqrt_price_x96,updated_block=EXCLUDED.updated_block,updated_at=NOW()`,
        [pair.address, value, inverse, pair.token0, pair.token1, sqrtPriceX96.toString(), block],
      );
      await client.query(
        `INSERT INTO latest_prices (pool_address,price,inverse_price,price_change,price_change_percent,price_change_direction,fdv_usd,token_price_usd,total_supply,supply_basis,base_reserve,quote_reserve,updated_slot,updated_at) VALUES ($1,$2,$3,NULL,NULL,NULL,NULL,$4,NULL,'pancakeswap_v3',0,0,$5,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,token_price_usd=EXCLUDED.token_price_usd,price_change=EXCLUDED.price-latest_prices.price,price_change_percent=CASE WHEN latest_prices.price IS NULL OR latest_prices.price=0 THEN NULL ELSE ((EXCLUDED.price - latest_prices.price) / latest_prices.price) * 100 END,price_change_direction=CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price > latest_prices.price THEN 'up' WHEN EXCLUDED.price < latest_prices.price THEN 'down' ELSE 'flat' END,updated_slot=EXCLUDED.updated_slot,updated_at=EXCLUDED.updated_at`,
        [pair.address, value, inverse, usd, now],
      );
      await client.query(`INSERT INTO evm_price_history (pool_address,price,inverse_price,updated_block,updated_at) VALUES ($1,$2,$3,$4,NOW())`, [pair.address, value, inverse, block]);
      for (const timeframe of TIMEFRAMES) {
        const bucketStart = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds;
        await client.query(
          `INSERT INTO price_candles (pool_address,timeframe,bucket_start,open,high,low,close,volume,updated_at) VALUES ($1,$2,$3,$4,$4,$4,$4,NULL,NOW()) ON CONFLICT (pool_address,timeframe,bucket_start) DO UPDATE SET high=GREATEST(price_candles.high,EXCLUDED.high),low=LEAST(price_candles.low,EXCLUDED.low),close=EXCLUDED.close,updated_at=EXCLUDED.updated_at`,
          [pair.address, timeframe.name, bucketStart, value],
        );
      }
      await client.query(
        `UPDATE latest_prices AS latest SET high_24h = rolling.high, low_24h = rolling.low FROM (SELECT MAX(high) AS high, MIN(low) AS low FROM price_candles WHERE pool_address = $1 AND timeframe = '1m' AND bucket_start >= $2) AS rolling WHERE latest.pool_address = $1`,
        [pair.address, now - 24 * 60 * 60 * 1000],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    console.log(`[pancakeswap-v3-price] ${pair.address} price=${value} block=${block}`);
  };

  const processLog = createPancakeSwapV3Processor(pgPool, register, rpc);

  const connect = () => {
    const endpoint = endpoints[endpointIndex];
    console.log(`[pancakeswap-v3] connecting websocket endpoint ${endpointIndex + 1}/${endpoints.length}`);
    socket = new WebSocket(endpoint);
    socket.on('open', () => {
      reconnectDelay = 1000;
      console.log('[pancakeswap-v3] websocket connected.');
      subscribe([{ topics: [SWAP_TOPIC] }], { kind: 'swap' });
    });

    socket.on('message', (raw) => {
      try {
        const payload = JSON.parse(raw.toString());
        if (payload.id !== undefined && pending.has(Number(payload.id))) {
          const request = pending.get(Number(payload.id))!;
          pending.delete(Number(payload.id));
          if (payload.error) request.reject(new Error(JSON.stringify(payload.error)));
          else request.resolve(payload.result);
          return;
        }
        if (payload.method !== 'eth_subscription') {
          if (payload.method) console.log(`[pancakeswap-v3] unexpected websocket message: ${payload.method}`);
          return;
        }
        const result = payload.params?.result;
        if (!result) return;
        const topic = result.topics?.[0]?.toLowerCase();
        if (topic !== SWAP_TOPIC) return;

        const block = Number.parseInt(result.blockNumber ?? '0x0', 16);
        const pairAddress = String(result.address ?? '').toLowerCase();
        console.log(`[pancakeswap-v3][websocket] Swap event received pool=${pairAddress} block=${block}`);
        void processLog(result, block, 'websocket').then(async (pair) => {
          if (!pair) return;
          // Only fetch prices for promoted/registered pools
          if (!pairs.has(pair.address)) return;
          try {
            const sqrtPriceX96 = word(result.data, 2);
            await price(pair, sqrtPriceX96, block);
          } catch (error) {
            await failure(pair.address, error);
          }
        }).catch((error) => {
          console.error('[pancakeswap-v3] swap processing failed:', error);
          void failure(pairAddress, error);
        });
      } catch (error) {
        console.error('[pancakeswap-v3] message handler error:', error);
      }
    });

    socket.on('error', (error) => {
      console.error('[pancakeswap-v3] websocket error:', error);
    });

    socket.on('close', () => {
      if (!reconnectTimer) {
        const next = nextWebsocketEndpoint(endpoints, endpointIndex);
        endpointIndex = next.index;
        reconnectTimer = setTimeout(() => {
          reconnectTimer = undefined;
          subscriptions.clear();
          connect();
        }, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
      }
    });
  };

  const refreshBnb = async () => {
    try {
      bnbUsd = await getReferencePrice('bnb');
    } catch (error) {
      console.warn('[pancakeswap-v3-price] WBNB/USD refresh failed; retaining last good value:', error);
    }
  };

  try {
    const existing = await pgPool.query<{ address: string; token0: string; token1: string; token0_symbol: string | null; token1_symbol: string | null; token0_decimals: number; token1_decimals: number; fee: number; block_number: bigint | number }>(`SELECT address,token0,token1,token0_symbol,token1_symbol,token0_decimals,token1_decimals,fee,block_number FROM bsc_pancakeswap_v3_pools`);
    for (const pair of existing.rows) {
      register({
        address: pair.address,
        token0: pair.token0,
        token1: pair.token1,
        symbol0: pair.token0_symbol ?? 'TOKEN0',
        symbol1: pair.token1_symbol ?? 'TOKEN1',
        decimals0: Number(pair.token0_decimals),
        decimals1: Number(pair.token1_decimals),
        fee: Number(pair.fee ?? 0),
        block: Number(pair.block_number ?? 0),
      });
    }
  } catch (error) {
    console.warn('[pancakeswap-v3] Existing pool hydration failed:', error);
  }

  await refreshBnb();
  setInterval(() => void refreshBnb(), 180_000);
  connect();
}
