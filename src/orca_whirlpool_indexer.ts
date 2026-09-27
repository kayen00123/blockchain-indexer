import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import bs58 from 'bs58';
import { getReferencePrice } from './reference_prices.js';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

export const ORCA_WHIRLPOOL_PROGRAM_ID = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SWAP_DISCRIMINATORS = [
  [248, 198, 158, 145, 225, 117, 135, 200],
  [43, 4, 237, 11, 26, 201, 30, 98],
].map((value) => Buffer.from(value));
const FAILURE_FILE = process.env.ORCA_WHIRLPOOL_FAILURE_FILE ?? 'orca-whirlpool-failures.jsonl';
const PRICE_FILE = process.env.ORCA_WHIRLPOOL_PRICE_EVENT_FILE ?? 'orca-whirlpool-price-events.jsonl';
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SWAP_THRESHOLD = Number(process.env.ORCA_WHIRLPOOL_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 300);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.ORCA_WHIRLPOOL_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 30);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const ACCOUNT_SUBSCRIBE_DELAY_MS = Number(process.env.ORCA_WHIRLPOOL_ACCOUNT_SUBSCRIBE_DELAY_MS ?? 500);
const PRICE_TIMEFRAMES = [{ name: '1m', milliseconds: 60_000 }, { name: '5m', milliseconds: 300_000 }, { name: '15m', milliseconds: 900_000 }, { name: '1h', milliseconds: 3_600_000 }, { name: '4h', milliseconds: 14_400_000 }, { name: '1d', milliseconds: 86_400_000 }] as const;

type Instruction = { programId?: string; programIdIndex?: number; accounts?: Array<number | string>; data?: string };
type Metadata = { name: string | null; symbol: string | null; logo: string | null; decimals: number | null };
type Swap = { pool: string; wallet: string; tokenA: string; tokenB: string; vaultA: string; vaultB: string; slot: number };
type PricePool = Swap & { tokenADecimals: number; tokenBDecimals: number };
type PoolState = { sqrtPrice: bigint; liquidity: bigint; tickCurrentIndex: number; tokenMintA: string; tokenVaultA: string; tokenMintB: string; tokenVaultB: string; tickSpacing: number; feeRate: number; protocolFeeRate: number };

function decode(value: unknown): Buffer | null {
  try {
    return typeof value === 'string' ? Buffer.from(bs58.decode(value)) : null;
  } catch {
    return null;
  }
}
function readU128(buffer: Buffer, offset: number): bigint {
  let result = 0n;
  for (let index = 0; index < 16; index += 1) result |= BigInt(buffer[offset + index]) << BigInt(index * 8);
  return result;
}
function readU64(buffer: Buffer, offset: number): bigint {
  let result = 0n;
  for (let index = 0; index < 8; index += 1) result |= BigInt(buffer[offset + index]) << BigInt(index * 8);
  return result;
}
function readPubkey(buffer: Buffer, offset: number): string {
  return bs58.encode(buffer.subarray(offset, offset + 32));
}
function supportedPair(tokenA: string, tokenB: string): boolean {
  return tokenA === SOL_MINT || tokenB === SOL_MINT || tokenA === USDC_MINT || tokenB === USDC_MINT;
}
function addresses(instruction: Instruction, keys: string[]): string[] {
  return (instruction.accounts ?? []).map((item) => typeof item === 'number' ? keys[item] ?? '' : item);
}

function readOrcaPoolState(data: Buffer): PoolState | null {
  if (data.length < 304) return null;
  const sqrtPrice = readU128(data, 65);
  const liquidity = readU128(data, 49);
  const tickCurrentIndex = data.readInt32LE(81);
  const tokenMintA = readPubkey(data, 101);
  const tokenVaultA = readPubkey(data, 133);
  const tokenMintB = readPubkey(data, 181);
  const tokenVaultB = readPubkey(data, 213);
  const tickSpacing = data.readUInt16LE(41);
  const feeRate = data.readUInt16LE(45);
  const protocolFeeRate = data.readUInt16LE(47);
  if (!tokenMintA || !tokenMintB || !tokenVaultA || !tokenVaultB) return null;
  return { sqrtPrice, liquidity, tickCurrentIndex, tokenMintA, tokenVaultA, tokenMintB, tokenVaultB, tickSpacing, feeRate, protocolFeeRate };
}

function readTokenAccountAmount(data: Buffer): bigint | null {
  if (data.length < 72) return null;
  return readU64(data, 64);
}

async function fetchMetadata(mints: string[]): Promise<Map<string, Metadata>> {
  const result = new Map<string, Metadata>([[SOL_MINT, { name: 'Wrapped SOL', symbol: 'SOL', logo: null, decimals: 9 }], [USDC_MINT, { name: 'USD Coin', symbol: 'USDC', logo: null, decimals: 6 }]]);
  if (!METAPLEX_RPC_URL) return result;
  try {
    const response = await fetch(METAPLEX_RPC_URL, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(mints.map((mint, index) => ({ jsonrpc: '2.0', id: `orca-whirlpool-${index}`, method: 'getAsset', params: { id: mint } }))) });
    if (!response.ok) throw new Error(`Metaplex DAS returned HTTP ${response.status}`);
    const body = await response.json() as Array<{ result?: any }>;
    for (const item of body) {
      const asset = item.result;
      if (!asset?.id) continue;
      const metadata = asset.content?.metadata ?? {};
      const links = asset.content?.links ?? {};
      result.set(asset.id, {
        name: typeof metadata.name === 'string' ? metadata.name : null,
        symbol: typeof metadata.symbol === 'string' ? metadata.symbol : null,
        logo: typeof links.image === 'string' ? links.image : null,
        decimals: typeof asset.token_info?.decimals === 'number' ? asset.token_info.decimals : null,
      });
    }
  } catch (error) { console.warn('[orca-whirlpool] Token metadata enrichment failed:', error); }
  return result;
}

export function parseOrcaWhirlpoolSwap(tx: any, slot: number): Swap | null {
  const message = tx?.transaction?.message;
  const keys = Array.isArray(message?.accountKeys) ? message.accountKeys.map((key: any) => typeof key === 'string' ? key : key?.pubkey ?? '') : [];
  const programIndex = keys.indexOf(ORCA_WHIRLPOOL_PROGRAM_ID);
  if (programIndex < 0) return null;
  const instructions: Instruction[] = [...(message.instructions ?? []), ...((tx.meta?.innerInstructions ?? []).flatMap((entry: any) => entry.instructions ?? []))];
  for (const instruction of instructions) {
    if (instruction.programId !== ORCA_WHIRLPOOL_PROGRAM_ID && instruction.programIdIndex !== programIndex) continue;
    const data = decode(instruction.data);
    if (!data || !SWAP_DISCRIMINATORS.some((value) => data.subarray(0, 8).equals(value))) continue;
    const account = addresses(instruction, keys);
    if (account.length < 11) continue;
    const isSwapV2 = data.subarray(0, 8).equals(Buffer.from(SWAP_DISCRIMINATORS[1]));
    const pool = isSwapV2 ? account[4] : account[2];
    const wallet = isSwapV2 ? account[3] : account[1];
    const vaultA = isSwapV2 ? account[8] : account[4];
    const vaultB = isSwapV2 ? account[10] : account[6];
    if (pool && wallet && vaultA && vaultB) {
      return { pool, wallet, tokenA: '', tokenB: '', vaultA, vaultB, slot };
    }
  }
  return null;
}

async function fetchPoolAccountInfo(poolAddress: string): Promise<{ owner: string; data: Array<string> } | null> {
  const endpoints = [
    process.env.SOLANA_RPC_URL ?? 'https://api.mainnet-beta.solana.com',
    'https://api.mainnet-beta.solana.com',
    'https://solana-rpc.publicnode.com',
  ];

  for (const endpoint of endpoints) {
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getAccountInfo', params: [poolAddress, { encoding: 'base64' }] }),
      });
      if (!response.ok) continue;
      const info = await response.json() as any;
      const value = info?.result?.value;
      if (!value || !Array.isArray(value.data) || typeof value.data[0] !== 'string') continue;
      if (typeof value.owner !== 'string' || !value.owner) continue;
      return { owner: value.owner, data: value.data as Array<string> };
    } catch {
      // try the next endpoint instead of failing the promotion path immediately
    }
  }

  return null;
}

async function failureMessage(pool: string, error: unknown): Promise<void> {
  try {
    await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: pool, error: String(error) })}\n`, 'utf8');
  } catch {
    // best-effort logging only
  }
}

export function createOrcaWhirlpoolProcessor(pgPool: PgPool, registerPricePool?: (pool: PricePool) => void) {
  const activities = new Map<string, { swap: Swap; events: Array<{ wallet: string; timestamp: number }> }>();
  const promoted = new Set<string>();
  return async (tx: any, slot: number, source = 'websocket') => {
    if (tx?.meta?.err) return;
    const swap = parseOrcaWhirlpoolSwap(tx, slot);
    if (!swap) return;
    const now = Date.now();
    const activity = activities.get(swap.pool) ?? { swap, events: [] };
    activity.events = activity.events.filter((entry) => entry.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet: swap.wallet, timestamp: now });
    activities.set(swap.pool, activity);
    const walletCount = new Set(activity.events.map((event) => event.wallet)).size;
    console.log(`[orca-whirlpool][${source}] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${walletCount}`);
    if (promoted.has(swap.pool) || activity.events.length < SWAP_THRESHOLD || walletCount < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(swap.pool);
    try {
      let tickSpacing = 0;
      const state = await pgPool.query<{ token_mint_a: string; token_mint_b: string; token_vault_a: string; token_vault_b: string; tick_spacing: number }>(`SELECT token_mint_a, token_mint_b, token_vault_a, token_vault_b, tick_spacing FROM orca_whirlpools WHERE address = $1 LIMIT 1`, [swap.pool]);
      if (state.rowCount === 0) {
        const accountInfo = await fetchPoolAccountInfo(swap.pool);
        if (!accountInfo || accountInfo.owner !== ORCA_WHIRLPOOL_PROGRAM_ID) {
          throw new Error(`No valid Whirlpool account data for promotion: ${swap.pool}`);
        }
        const poolState = readOrcaPoolState(Buffer.from(accountInfo.data[0], 'base64'));
        if (!poolState) throw new Error(`Invalid Whirlpools pool state for ${swap.pool}.`);
        swap.tokenA = poolState.tokenMintA;
        swap.tokenB = poolState.tokenMintB;
        swap.vaultA = poolState.tokenVaultA;
        swap.vaultB = poolState.tokenVaultB;
        tickSpacing = poolState.tickSpacing;
      } else {
        swap.tokenA = state.rows[0].token_mint_a;
        swap.tokenB = state.rows[0].token_mint_b;
        swap.vaultA = state.rows[0].token_vault_a;
        swap.vaultB = state.rows[0].token_vault_b;
        tickSpacing = state.rows[0].tick_spacing;
      }
      if (!supportedPair(swap.tokenA, swap.tokenB)) return;
      const metadata = await fetchMetadata([swap.tokenA, swap.tokenB]);
      const tokenA = metadata.get(swap.tokenA); const tokenB = metadata.get(swap.tokenB);
      await pgPool.query(`INSERT INTO orca_whirlpools (address, pool_type, program_id, network, whirlpools_config, token_mint_a, token_mint_a_symbol, token_mint_a_decimals, token_mint_a_total_supply_raw, token_mint_a_logo_url, token_mint_b, token_mint_b_symbol, token_mint_b_decimals, token_mint_b_total_supply_raw, token_mint_b_logo_url, token_vault_a, token_vault_b, tick_spacing, fee_rate, protocol_fee_rate, liquidity, sqrt_price_x64, tick_current_index, updated_slot, discovered_at, indexed_at) VALUES ($1, 'orca_whirlpool', $2, 'solana', '', $3, $4, $5, 0, $6, $7, $8, $9, 0, $10, $11, $12, $13, 0, 0, 0, 0, 0, $14, NOW(), NOW()) ON CONFLICT (address) DO UPDATE SET token_mint_a_symbol=EXCLUDED.token_mint_a_symbol, token_mint_a_decimals=EXCLUDED.token_mint_a_decimals, token_mint_a_logo_url=EXCLUDED.token_mint_a_logo_url, token_mint_b_symbol=EXCLUDED.token_mint_b_symbol, token_mint_b_decimals=EXCLUDED.token_mint_b_decimals, token_mint_b_logo_url=EXCLUDED.token_mint_b_logo_url, token_vault_a=EXCLUDED.token_vault_a, token_vault_b=EXCLUDED.token_vault_b, updated_slot=EXCLUDED.updated_slot, indexed_at=NOW()`, [swap.pool, ORCA_WHIRLPOOL_PROGRAM_ID, swap.tokenA, tokenA?.symbol ?? null, tokenA?.decimals ?? 9, tokenA?.logo ?? null, swap.tokenB, tokenB?.symbol ?? null, tokenB?.decimals ?? 9, tokenB?.logo ?? null, swap.vaultA, swap.vaultB, tickSpacing, swap.slot]);
      registerPricePool?.({ ...swap, tokenADecimals: tokenA?.decimals ?? 9, tokenBDecimals: tokenB?.decimals ?? 9 });
      console.log(`[orca-whirlpool] Promoted ${swap.pool} | swaps=${activity.events.length} | wallets=${walletCount}`);
    } catch (error) { promoted.delete(swap.pool); console.error(`[orca-whirlpool] Promotion failed for ${swap.pool}:`, error); await failureMessage(swap.pool, error); }
  };
}

export async function startOrcaWhirlpoolIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, priceFile = PRICE_FILE, priceWebsocketUrl: WebsocketEndpointInput = websocketUrl): Promise<void> {
  const blockEndpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  const priceEndpoints = normalizeWebsocketEndpoints(priceWebsocketUrl, blockEndpoints[0] ?? '');
  if (blockEndpoints.length === 0) { console.warn('[orca-whirlpool] ORCA_WHIRLPOOL_WS_URL is not configured; indexing is disabled.'); return; }
  const pools = new Map<string, PricePool>();
  const states = new Map<string, { sqrtPrice: bigint; liquidity: bigint; tickCurrentIndex: number; tokenMintA: string; tokenMintB: string; tokenVaultA: string; tokenVaultB: string; tickSpacing: number; feeRate: number; protocolFeeRate: number }>();
  const reserves = new Map<string, { raw: bigint; slot: number }>();
  const subscriptions = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'vault' }>();
  const pending = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'vault' }>();
  const requested = new Set<string>();
  const queue: Array<{ pool: PricePool; account: string; kind: 'state' | 'vault' }> = [];
  let draining = false;
  let socket: WebSocket | undefined; let priceSocket: WebSocket | undefined; let requestId = 1; let reconnectDelay = 1000; let reconnectTimer: NodeJS.Timeout | undefined; let priceReconnectTimer: NodeJS.Timeout | undefined; let solUsd: number | null = null;
  let endpointIndex = 0; let priceEndpointIndex = 0;
  const recordPrice = async (event: Record<string, unknown>) => {
    try { await appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8'); }
    catch (error) { console.error('[orca-whirlpool-price] event log failed:', error); }
  };
  const refreshSol = async () => {
    try {
      solUsd = await getReferencePrice('sol');
    } catch {
      // retain previous value
    }
  };
  const writePrice = async (pool: PricePool, slot: number) => {
    const state = states.get(pool.pool);
    if (!state || state.sqrtPrice === 0n) return;
    const rawA = reserves.get(pool.vaultA);
    const rawB = reserves.get(pool.vaultB);
    if (!rawA || !rawB) return;
    const price = (Number(state.sqrtPrice) / 2 ** 64) ** 2 * 10 ** (pool.tokenADecimals - pool.tokenBDecimals);
    const inversePrice = 1 / price;
    if (!Number.isFinite(price) || price <= 0) return;
    const reserveA = Number(rawA.raw) / 10 ** pool.tokenADecimals;
    const reserveB = Number(rawB.raw) / 10 ** pool.tokenBDecimals;
    const tokenPriceUsd = pool.tokenB === USDC_MINT ? price : pool.tokenA === USDC_MINT ? inversePrice : pool.tokenB === SOL_MINT && solUsd ? price * solUsd : pool.tokenA === SOL_MINT && solUsd ? inversePrice * solUsd : null;
    const now = Date.now();
    let high24h = price; let low24h = price;
    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`INSERT INTO latest_prices (pool_address,price,inverse_price,price_change,price_change_percent,price_change_direction,fdv_usd,token_price_usd,total_supply,supply_basis,base_reserve,quote_reserve,updated_slot,updated_at) VALUES ($1,$2,$3,NULL,NULL,NULL,NULL,$4,NULL,'orca_whirlpool',$5,$6,$7,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,token_price_usd=EXCLUDED.token_price_usd,price_change=EXCLUDED.price-latest_prices.price,price_change_percent=CASE WHEN latest_prices.price IS NULL OR latest_prices.price=0 THEN NULL ELSE ((EXCLUDED.price-latest_prices.price)/latest_prices.price)*100 END,price_change_direction=CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price>latest_prices.price THEN 'up' WHEN EXCLUDED.price<latest_prices.price THEN 'down' ELSE 'flat' END,base_reserve=EXCLUDED.base_reserve,quote_reserve=EXCLUDED.quote_reserve,updated_slot=EXCLUDED.updated_slot,updated_at=EXCLUDED.updated_at`, [pool.pool, price, inversePrice, tokenPriceUsd, reserveA, reserveB, slot]);
      for (const timeframe of PRICE_TIMEFRAMES) {
        const bucket = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds;
        await client.query(`INSERT INTO price_candles (pool_address,timeframe,bucket_start,open,high,low,close,volume,updated_at) VALUES ($1,$2,$3,$4,$4,$4,$4,NULL,NOW()) ON CONFLICT (pool_address,timeframe,bucket_start) DO UPDATE SET high=GREATEST(price_candles.high,EXCLUDED.high),low=LEAST(price_candles.low,EXCLUDED.low),close=EXCLUDED.close,updated_at=EXCLUDED.updated_at`, [pool.pool, timeframe.name, bucket, price]);
      }
      const rolling = await client.query<{ high: number | null; low: number | null }>(`SELECT MAX(high) AS high, MIN(low) AS low FROM price_candles WHERE pool_address=$1 AND timeframe='1m' AND bucket_start >= $2`, [pool.pool, now - 86_400_000]);
      high24h = rolling.rows[0]?.high ?? price;
      low24h = rolling.rows[0]?.low ?? price;
      await client.query(`UPDATE latest_prices SET high_24h=$2,low_24h=$3 WHERE pool_address=$1`, [pool.pool, high24h, low24h]);
      await client.query('COMMIT');
      await recordPrice({ type: 'price', poolType: 'orca_whirlpool', poolAddress: pool.pool, price, inversePrice, tokenPriceUsd, high24h, low24h, tickCurrentIndex: state.tickCurrentIndex, tickSpacing: state.tickSpacing, feeRate: state.feeRate, protocolFeeRate: state.protocolFeeRate, reserveA, reserveB, solUsd, slot });
      console.log(`[orca-whirlpool-price] ${pool.pool} price=${price} inverse=${inversePrice} slot=${slot}`);
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally { client.release(); }
  };
  const drain = () => {
    if (draining || queue.length === 0) return;
    draining = true;
    const entry = queue.shift()!;
    if (priceSocket?.readyState === WebSocket.OPEN) {
      const id = requestId++;
      pending.set(id, entry);
      priceSocket.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [entry.account, { commitment: 'confirmed', encoding: 'base64' }] }));
    }
    setTimeout(() => { draining = false; drain(); }, ACCOUNT_SUBSCRIBE_DELAY_MS);
  };
  const subscribe = (pool: PricePool, account: string, kind: 'state' | 'vault') => {
    const key = `${kind}:${account}`;
    if (requested.has(key)) return;
    requested.add(key);
    queue.push({ pool, account, kind });
    drain();
  };
  const register = (pool: PricePool) => {
    pools.set(pool.pool, pool);
    subscribe(pool, pool.pool, 'state');
    subscribe(pool, pool.vaultA, 'vault');
    subscribe(pool, pool.vaultB, 'vault');
  };
  const processTransaction = createOrcaWhirlpoolProcessor(pgPool, register);
  const handleAccount = async (payload: any, entry?: { pool: PricePool; account: string; kind: 'state' | 'vault' }) => {
    if (!entry) return;
    const slot = Number(payload.params?.result?.context?.slot ?? 0);
    if (entry.kind === 'state') {
      const data = payload.params?.result?.value?.data;
      if (Array.isArray(data) && typeof data[0] === 'string') {
        const parsed = readOrcaPoolState(Buffer.from(data[0], 'base64'));
        if (parsed) states.set(entry.pool.pool, { ...parsed, tokenMintA: parsed.tokenMintA, tokenMintB: parsed.tokenMintB, tokenVaultA: parsed.tokenVaultA, tokenVaultB: parsed.tokenVaultB });
      }
    } else {
      const data = payload.params?.result?.value?.data;
      if (Array.isArray(data) && typeof data[0] === 'string') {
        const tokenAmount = readTokenAccountAmount(Buffer.from(data[0], 'base64'));
        if (tokenAmount !== null) reserves.set(entry.account, { raw: tokenAmount, slot });
      }
    }
    await writePrice(entry.pool, slot);
  };
  const connect = () => {
    const endpoint = blockEndpoints[endpointIndex] ?? blockEndpoints[0] ?? '';
    if (!endpoint) return;
    socket = new WebSocket(endpoint);
    socket.on('open', () => {
      reconnectDelay = 1000;
      socket?.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'blockSubscribe', params: [{ mentionsAccountOrProgram: ORCA_WHIRLPOOL_PROGRAM_ID }, { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 }] }));
      console.log('[orca-whirlpool] block subscription started.');
    });
    socket.on('message', (raw) => {
      let payload: any;
      try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) {
        console.error('[orca-whirlpool] websocket error:', payload.error);
        void failureMessage('subscription', payload.error);
        return;
      }
      const block = payload?.params?.result?.value?.block;
      if (!block) return;
      const slot = Number(payload?.params?.result?.context?.slot ?? 0);
      for (const tx of block.transactions ?? []) void processTransaction(tx, slot).catch((error) => failureMessage('transaction', error));
    });
    socket.on('error', (error) => {
      console.error('[orca-whirlpool] websocket failure:', error);
      void failureMessage('websocket', error);
    });
    socket.on('close', (code, reason) => {
      const next = nextWebsocketEndpoint(blockEndpoints, endpointIndex);
      endpointIndex = next.index;
      for (const [id, entry] of pending) { pending.delete(id); }
      console.warn(`[orca-whirlpool] websocket closed: ${code} ${reason.toString()}`);
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
      }
    });
  };
  const connectPrice = () => {
    const endpoint = priceEndpoints[priceEndpointIndex] ?? priceEndpoints[0] ?? '';
    if (!endpoint) return;
    priceSocket = new WebSocket(endpoint);
    priceSocket.on('open', () => {
      for (const pool of pools.values()) { subscribe(pool, pool.pool, 'state'); subscribe(pool, pool.vaultA, 'vault'); subscribe(pool, pool.vaultB, 'vault'); }
    });
    priceSocket.on('message', (raw) => {
      let payload: any; try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) { void failureMessage('price-subscription', payload.error); return; }
      if (payload.id !== undefined && pending.has(payload.id)) { const entry = pending.get(payload.id)!; pending.delete(payload.id); if (payload.result !== undefined) subscriptions.set(payload.result, entry); return; }
      if (payload.method === 'accountNotification') void handleAccount(payload, subscriptions.get(payload.params?.subscription));
    });
    priceSocket.on('error', (error) => void failureMessage('price-websocket', error));
    priceSocket.on('close', () => {
      const next = nextWebsocketEndpoint(priceEndpoints, priceEndpointIndex);
      priceEndpointIndex = next.index;
      for (const [id] of pending) pending.delete(id);
      if (!priceReconnectTimer) { priceReconnectTimer = setTimeout(() => { priceReconnectTimer = undefined; connectPrice(); }, reconnectDelay); }
    });
  };

  try {
    const existing = await pgPool.query<PricePool>(`SELECT address AS pool, token_mint_a AS "tokenA", token_mint_b AS "tokenB", token_vault_a AS "vaultA", token_vault_b AS "vaultB", updated_slot AS slot, token_mint_a_decimals AS "tokenADecimals", token_mint_b_decimals AS "tokenBDecimals" FROM orca_whirlpools WHERE pool_type = 'orca_whirlpool' AND (token_mint_a IN ('${SOL_MINT}', '${USDC_MINT}') OR token_mint_b IN ('${SOL_MINT}', '${USDC_MINT}'))`);
    for (const pool of existing.rows) register(pool);
  } catch (error) { console.warn('[orca-whirlpool] Existing pool hydration failed:', error); }
  await refreshSol();
  setInterval(() => void refreshSol(), 180_000);
  connect();
  connectPrice();
}
