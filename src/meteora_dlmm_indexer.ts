import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { getReferencePrice } from './reference_prices.js';
import bs58 from 'bs58';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

export const METEORA_DLMM_PROGRAM_ID = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SWAP_DISCRIMINATORS = [
  [248, 198, 158, 145, 225, 117, 135, 200],
  [65, 75, 63, 76, 235, 91, 91, 136],
  [143, 190, 90, 218, 196, 30, 51, 222],
  [55, 217, 98, 86, 163, 74, 180, 173],
].map((value) => Buffer.from(value));
const FAILURE_FILE = process.env.METEORA_DLMM_FAILURE_FILE ?? 'meteora-dlmm-failures.jsonl';
const PRICE_FILE = process.env.METEORA_DLMM_PRICE_EVENT_FILE ?? 'meteora-dlmm-price-events.jsonl';
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SWAP_THRESHOLD = Number(process.env.METEORA_DLMM_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 300);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.METEORA_DLMM_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 30);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const ACCOUNT_SUBSCRIBE_DELAY_MS = Number(process.env.METEORA_DLMM_ACCOUNT_SUBSCRIBE_DELAY_MS ?? 500);
const PRICE_TIMEFRAMES = [{ name: '1m', milliseconds: 60_000 }, { name: '5m', milliseconds: 300_000 }, { name: '15m', milliseconds: 900_000 }, { name: '1h', milliseconds: 3_600_000 }, { name: '4h', milliseconds: 14_400_000 }, { name: '1d', milliseconds: 86_400_000 }] as const;

type Instruction = { programId?: string; programIdIndex?: number; accounts?: Array<number | string>; data?: string };
type Metadata = { name: string | null; symbol: string | null; logo: string | null; decimals: number | null };
type Swap = { pool: string; wallet: string; tokenX: string; tokenY: string; reserveX: string; reserveY: string; oracle: string; slot: number };
type Activity = Swap & { events: Array<{ wallet: string; timestamp: number }> };
type PricePool = Swap & { tokenXDecimals: number; tokenYDecimals: number };

function decode(value: unknown): Buffer | null { try { return typeof value === 'string' ? Buffer.from(bs58.decode(value)) : null; } catch { return null; } }
function supportedPair(tokenX: string, tokenY: string): boolean { return tokenX === SOL_MINT || tokenY === SOL_MINT || tokenX === USDC_MINT || tokenY === USDC_MINT; }
function readU64(data: unknown): bigint | null { if (!Array.isArray(data) || typeof data[0] !== 'string') return null; try { const bytes = Buffer.from(data[0], 'base64'); if (bytes.length < 72) return null; let result = 0n; for (let i = 0; i < 8; i += 1) result |= BigInt(bytes[64 + i]) << BigInt(i * 8); return result; } catch { return null; } }
function readDlmmState(data: unknown): { activeId: number; binStep: number } | null { if (!Array.isArray(data) || typeof data[0] !== 'string') return null; try { const bytes = Buffer.from(data[0], 'base64'); if (bytes.length < 83) return null; return { activeId: bytes.readInt32LE(76), binStep: bytes.readUInt16LE(80) }; } catch { return null; } }
function instructionAccounts(instruction: Instruction, keys: string[]): string[] { return (instruction.accounts ?? []).map((item) => typeof item === 'number' ? keys[item] ?? '' : item); }

export function parseMeteoraDlmmSwap(tx: any, slot: number): Swap | null {
  const message = tx?.transaction?.message;
  const keys = Array.isArray(message?.accountKeys) ? message.accountKeys.map((key: any) => typeof key === 'string' ? key : key?.pubkey ?? '') : [];
  const programIndex = keys.indexOf(METEORA_DLMM_PROGRAM_ID);
  if (programIndex < 0) return null;
  const instructions: Instruction[] = [...(message.instructions ?? []), ...((tx.meta?.innerInstructions ?? []).flatMap((entry: any) => entry.instructions ?? []))];
  for (const instruction of instructions) {
    if (instruction.programId !== METEORA_DLMM_PROGRAM_ID && instruction.programIdIndex !== programIndex) continue;
    const data = decode(instruction.data);
    if (!data || !SWAP_DISCRIMINATORS.some((discriminator) => data.subarray(0, 8).equals(discriminator))) continue;
    const accounts = instructionAccounts(instruction, keys);
    if (accounts.length < 13) continue;
    // The bitmap-extension account is optional, so all following accounts shift by one.
    const offset = accounts.length >= 15 ? 0 : -1;
    const pool = accounts[0];
    const reserveX = accounts[2 + offset];
    const reserveY = accounts[3 + offset];
    const tokenX = accounts[6 + offset];
    const tokenY = accounts[7 + offset];
    const oracle = accounts[8 + offset];
    const wallet = accounts[10 + offset];
    if (pool && reserveX && reserveY && tokenX && tokenY && oracle && wallet && supportedPair(tokenX, tokenY)) return { pool, wallet, tokenX, tokenY, reserveX, reserveY, oracle, slot };
  }
  return null;
}

async function fetchMetadata(mints: string[]): Promise<Map<string, Metadata>> {
  const result = new Map<string, Metadata>([[SOL_MINT, { name: 'Wrapped SOL', symbol: 'SOL', logo: null, decimals: 9 }], [USDC_MINT, { name: 'USD Coin', symbol: 'USDC', logo: null, decimals: 6 }]]);
  if (!METAPLEX_RPC_URL) return result;
  try {
    const response = await fetch(METAPLEX_RPC_URL, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(mints.map((mint, index) => ({ jsonrpc: '2.0', id: `meteora-dlmm-${index}`, method: 'getAsset', params: { id: mint } }))) });
    if (!response.ok) throw new Error(`Metaplex DAS returned HTTP ${response.status}`);
    for (const item of await response.json() as Array<{ result?: any }>) {
      const asset = item.result;
      if (!asset?.id) continue;
      const metadata = asset.content?.metadata ?? {};
      const links = asset.content?.links ?? {};
      result.set(asset.id, { name: typeof metadata.name === 'string' ? metadata.name : null, symbol: typeof metadata.symbol === 'string' ? metadata.symbol : null, logo: typeof links.image === 'string' ? links.image : null, decimals: typeof asset.token_info?.decimals === 'number' ? asset.token_info.decimals : null });
    }
  } catch (error) { console.warn('[meteora-dlmm] Token metadata enrichment failed:', error); }
  return result;
}

async function recordFailure(pool: string, error: unknown): Promise<void> { try { await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: pool, error: String(error) })}\n`, 'utf8'); } catch { /* logging must not stop the stream */ } }

export function createMeteoraDlmmProcessor(pgPool: PgPool, registerPricePool?: (pool: PricePool) => void) {
  const activities = new Map<string, Activity>();
  const promoted = new Set<string>();
  return async (tx: any, slot: number, source = 'websocket') => {
    if (tx?.meta?.err) return;
    const swap = parseMeteoraDlmmSwap(tx, slot);
    if (!swap) return;
    const now = Date.now();
    const activity = activities.get(swap.pool) ?? { ...swap, events: [] };
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet: swap.wallet, timestamp: now });
    activities.set(swap.pool, activity);
    const walletCount = new Set(activity.events.map((event) => event.wallet)).size;
    console.log(`[meteora-dlmm][${source}] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${walletCount}`);
    if (promoted.has(swap.pool) || activity.events.length < SWAP_THRESHOLD || walletCount < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(swap.pool);
    try {
      const metadata = await fetchMetadata([swap.tokenX, swap.tokenY]);
      const x = metadata.get(swap.tokenX); const y = metadata.get(swap.tokenY);
      await pgPool.query(`INSERT INTO meteora_dlmm_pools (address, pool_type, program_id, network, creator, token_x_mint, token_x_name, token_x_symbol, token_x_decimals, token_x_total_supply_raw, token_x_logo_url, token_y_mint, token_y_name, token_y_symbol, token_y_decimals, token_y_total_supply_raw, token_y_logo_url, reserve_x, reserve_y, oracle, active_id, bin_step, activation_point, updated_slot, discovered_at, indexed_at) VALUES ($1, 'meteora_dlmm', $2, 'solana', $3, $4, $5, $6, $7, 0, $8, $9, $10, $11, $12, 0, $13, $14, $15, $16, 0, 0, 0, $17, NOW(), NOW()) ON CONFLICT (address) DO UPDATE SET creator=EXCLUDED.creator, token_x_name=EXCLUDED.token_x_name, token_x_symbol=EXCLUDED.token_x_symbol, token_x_decimals=EXCLUDED.token_x_decimals, token_x_logo_url=EXCLUDED.token_x_logo_url, token_y_name=EXCLUDED.token_y_name, token_y_symbol=EXCLUDED.token_y_symbol, token_y_decimals=EXCLUDED.token_y_decimals, token_y_logo_url=EXCLUDED.token_y_logo_url, reserve_x=EXCLUDED.reserve_x, reserve_y=EXCLUDED.reserve_y, updated_slot=EXCLUDED.updated_slot, indexed_at=NOW()`, [swap.pool, METEORA_DLMM_PROGRAM_ID, swap.wallet, swap.tokenX, x?.name ?? null, x?.symbol ?? null, x?.decimals ?? 9, x?.logo ?? null, swap.tokenY, y?.name ?? null, y?.symbol ?? null, y?.decimals ?? 9, y?.logo ?? null, swap.reserveX, swap.reserveY, swap.oracle, swap.slot]);
      registerPricePool?.({ ...swap, tokenXDecimals: x?.decimals ?? 9, tokenYDecimals: y?.decimals ?? 9 });
      console.log(`[meteora-dlmm] Promoted ${swap.pool} | swaps=${activity.events.length} | wallets=${walletCount}`);
    } catch (error) { promoted.delete(swap.pool); console.error(`[meteora-dlmm] Promotion failed for ${swap.pool}:`, error); await recordFailure(swap.pool, error); }
  };
}

export async function startMeteoraDlmmIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, priceFile = PRICE_FILE, priceWebsocketUrl: WebsocketEndpointInput = websocketUrl): Promise<void> {
  const blockEndpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  const priceEndpoints = normalizeWebsocketEndpoints(priceWebsocketUrl, blockEndpoints[0] ?? '');
  if (blockEndpoints.length === 0) { console.warn('[meteora-dlmm] METEORA_DLMM_WS_URL is not configured; indexing is disabled.'); return; }
  const pricePools = new Map<string, PricePool>();
  const snapshots = new Map<string, { raw: bigint; slot: number }>();
  const states = new Map<string, { activeId: number; binStep: number; slot: number }>();
  const subscriptions = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'reserve' }>();
  const requested = new Set<string>();
  const subscriptionQueue: Array<{ pool: PricePool; account: string; kind: 'state' | 'reserve' }> = [];
  let subscriptionQueueActive = false;
  let socket: WebSocket | undefined; let priceSocket: WebSocket | undefined; let requestId = 1; let reconnectDelayMs = 1000; let reconnectTimer: NodeJS.Timeout | undefined; let priceReconnectTimer: NodeJS.Timeout | undefined; let solUsd: number | null = null;
  let endpointIndex = 0; let priceEndpointIndex = 0;
  const recordPrice = async (event: Record<string, unknown>) => { try { await appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8'); } catch (error) { console.error('[meteora-dlmm-price] event log failed:', error); } };
  const refreshSol = async () => { try { solUsd = await getReferencePrice('sol'); } catch { /* retain last good SOL price */ } };
  const writePrice = async (pool: PricePool, slot: number) => {
    const state = states.get(pool.pool); if (!state) return;
    const rawX = snapshots.get(pool.reserveX); const rawY = snapshots.get(pool.reserveY); if (!rawX || !rawY) return;
    const price = (1 + state.binStep / 10000) ** state.activeId * 10 ** (pool.tokenXDecimals - pool.tokenYDecimals);
    const inversePrice = 1 / price; if (!Number.isFinite(price) || price <= 0) return;
    const tokenPriceUsd = pool.tokenY === USDC_MINT ? price : pool.tokenX === USDC_MINT ? inversePrice : pool.tokenY === SOL_MINT && solUsd ? price * solUsd : pool.tokenX === SOL_MINT && solUsd ? inversePrice * solUsd : null;
    const reserveX = Number(rawX.raw) / 10 ** pool.tokenXDecimals; const reserveY = Number(rawY.raw) / 10 ** pool.tokenYDecimals; const now = Date.now(); const client = await pgPool.connect();
    let high24h = price;
    let low24h = price;
    try { await client.query('BEGIN'); await client.query(`INSERT INTO latest_prices (pool_address, price, inverse_price, price_change, price_change_percent, price_change_direction, fdv_usd, token_price_usd, total_supply, supply_basis, base_reserve, quote_reserve, updated_slot, updated_at) VALUES ($1,$2,$3,NULL,NULL,NULL,NULL,$4,NULL,'meteora_dlmm_bins',$5,$6,$7,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,token_price_usd=EXCLUDED.token_price_usd,price_change=EXCLUDED.price-latest_prices.price,price_change_percent=CASE WHEN latest_prices.price IS NULL OR latest_prices.price=0 THEN NULL ELSE ((EXCLUDED.price-latest_prices.price)/latest_prices.price)*100 END,price_change_direction=CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price>latest_prices.price THEN 'up' WHEN EXCLUDED.price<latest_prices.price THEN 'down' ELSE 'flat' END,base_reserve=EXCLUDED.base_reserve,quote_reserve=EXCLUDED.quote_reserve,updated_slot=EXCLUDED.updated_slot,updated_at=EXCLUDED.updated_at`, [pool.pool, price, inversePrice, tokenPriceUsd, reserveX, reserveY, slot]); for (const timeframe of PRICE_TIMEFRAMES) { const bucket = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds; await client.query(`INSERT INTO price_candles (pool_address,timeframe,bucket_start,open,high,low,close,volume,updated_at) VALUES ($1,$2,$3,$4,$4,$4,$4,NULL,NOW()) ON CONFLICT (pool_address,timeframe,bucket_start) DO UPDATE SET high=GREATEST(price_candles.high,EXCLUDED.high),low=LEAST(price_candles.low,EXCLUDED.low),close=EXCLUDED.close,updated_at=EXCLUDED.updated_at`, [pool.pool, timeframe.name, bucket, price]); } await client.query(`UPDATE latest_prices SET high_24h=(SELECT MAX(high) FROM price_candles WHERE pool_address=$1 AND timeframe='1m' AND bucket_start >= $2),low_24h=(SELECT MIN(low) FROM price_candles WHERE pool_address=$1 AND timeframe='1m' AND bucket_start >= $2) WHERE pool_address=$1`, [pool.pool, now - 86_400_000]); const rolling = await client.query<{ high: number | null; low: number | null }>(`SELECT MAX(high) AS high, MIN(low) AS low FROM price_candles WHERE pool_address=$1 AND timeframe='1m' AND bucket_start >= $2`, [pool.pool, now - 86_400_000]); high24h = rolling.rows[0]?.high ?? price; low24h = rolling.rows[0]?.low ?? price; await client.query('COMMIT'); await recordPrice({ type: 'price', poolType: 'meteora_dlmm', poolAddress: pool.pool, price, inversePrice, tokenPriceUsd, high24h, low24h, activeId: state.activeId, binStep: state.binStep, reserveX, reserveY, solUsd, slot }); console.log(`[meteora-dlmm-price] ${pool.pool} price=${price} inverse=${inversePrice} slot=${slot}`); } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error; } finally { client.release(); }
  };
  const drainSubscriptionQueue = () => {
    if (subscriptionQueueActive || subscriptionQueue.length === 0) return;
    subscriptionQueueActive = true;
    const entry = subscriptionQueue.shift()!;
    if (priceSocket?.readyState === WebSocket.OPEN) {
      const id = requestId++;
      pending.set(id, entry);
      priceSocket.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [entry.account, { commitment: 'confirmed', encoding: 'base64' }] }));
    }
    setTimeout(() => { subscriptionQueueActive = false; drainSubscriptionQueue(); }, ACCOUNT_SUBSCRIBE_DELAY_MS);
  };
  const subscribe = (pool: PricePool, account: string, kind: 'state' | 'reserve') => { if (requested.has(`${kind}:${account}`)) return; requested.add(`${kind}:${account}`); subscriptionQueue.push({ pool, account, kind }); drainSubscriptionQueue(); };
  const register = (pool: PricePool) => { pricePools.set(pool.pool, pool); subscribe(pool, pool.pool, 'state'); subscribe(pool, pool.reserveX, 'reserve'); subscribe(pool, pool.reserveY, 'reserve'); };
  const processTransaction = createMeteoraDlmmProcessor(pgPool, register);
  const pending = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'reserve' }>();
  const connect = () => { const endpoint = blockEndpoints[endpointIndex] ?? blockEndpoints[0] ?? ''; if (!endpoint) return; socket = new WebSocket(endpoint); socket.on('open', () => { reconnectDelayMs = 1000; socket?.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'blockSubscribe', params: [{ mentionsAccountOrProgram: METEORA_DLMM_PROGRAM_ID }, { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 }] })); console.log('[meteora-dlmm] block subscription started.'); for (const pool of pricePools.values()) { subscribe(pool, pool.pool, 'state'); subscribe(pool, pool.reserveX, 'reserve'); subscribe(pool, pool.reserveY, 'reserve'); } }); socket.on('message', (raw) => { let payload: any; try { payload = JSON.parse(raw.toString()); } catch { return; } if (payload.error) { console.error('[meteora-dlmm] websocket error:', payload.error); void recordFailure('subscription', payload.error); return; } if (payload.id !== undefined && pending.has(payload.id)) { const entry = pending.get(payload.id)!; pending.delete(payload.id); subscriptions.set(payload.result, entry); return; } if (payload.method === 'accountNotification') { void handleAccount(payload, subscriptions.get(payload.params?.subscription)); return; } const block = payload?.params?.result?.value?.block; if (!block) return; const slot = Number(payload?.params?.result?.context?.slot ?? 0); for (const tx of block.transactions ?? []) void processTransaction(tx, slot).catch((error) => recordFailure('transaction', error)); }); socket.on('error', (error) => { console.error('[meteora-dlmm] websocket failure:', error); void recordFailure('websocket', error); }); socket.on('close', (code, reason) => { const next = nextWebsocketEndpoint(blockEndpoints, endpointIndex); endpointIndex = next.index; console.warn(`[meteora-dlmm] websocket closed: ${code} ${reason.toString()}`); if (!reconnectTimer) { reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelayMs); reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000); } }); };
  const connectPrice = () => { const endpoint = priceEndpoints[priceEndpointIndex] ?? priceEndpoints[0] ?? ''; if (!endpoint) return; priceSocket = new WebSocket(endpoint); priceSocket.on('open', () => { for (const pool of pricePools.values()) { subscribe(pool, pool.pool, 'state'); subscribe(pool, pool.reserveX, 'reserve'); subscribe(pool, pool.reserveY, 'reserve'); } }); priceSocket.on('message', (raw) => { let payload: any; try { payload = JSON.parse(raw.toString()); } catch { return; } if (payload.error) { void recordFailure('price-subscription', payload.error); return; } if (payload.id !== undefined && pending.has(payload.id)) { const entry = pending.get(payload.id)!; pending.delete(payload.id); subscriptions.set(payload.result, entry); return; } if (payload.method === 'accountNotification') void handleAccount(payload, subscriptions.get(payload.params?.subscription)); }); priceSocket.on('error', (error) => void recordFailure('price-websocket', error)); priceSocket.on('close', () => { const next = nextWebsocketEndpoint(priceEndpoints, priceEndpointIndex); priceEndpointIndex = next.index; for (const [id] of pending) pending.delete(id); if (!priceReconnectTimer) priceReconnectTimer = setTimeout(() => { priceReconnectTimer = undefined; connectPrice(); }, reconnectDelayMs); }); };
  async function handleAccount(payload: any, entry?: { pool: PricePool; account: string; kind: 'state' | 'reserve' }) { if (!entry) return; const slot = Number(payload.params?.result?.context?.slot ?? 0); if (entry.kind === 'state') { const state = readDlmmState(payload.params?.result?.value?.data); if (!state) return; states.set(entry.pool.pool, { ...state, slot }); } else { const raw = readU64(payload.params?.result?.value?.data); if (raw === null) return; snapshots.set(entry.account, { raw, slot }); } await writePrice(entry.pool, slot); }
  try {
    const existing = await pgPool.query<PricePool>(`SELECT address AS pool, creator AS wallet, token_x_mint AS "tokenX", token_y_mint AS "tokenY", reserve_x AS "reserveX", reserve_y AS "reserveY", oracle, updated_slot AS slot, token_x_decimals AS "tokenXDecimals", token_y_decimals AS "tokenYDecimals" FROM meteora_dlmm_pools WHERE pool_type = 'meteora_dlmm' AND (token_x_mint IN ('${SOL_MINT}', '${USDC_MINT}') OR token_y_mint IN ('${SOL_MINT}', '${USDC_MINT}'))`);
    for (const pool of existing.rows) register(pool);
  } catch (error) { console.warn('[meteora-dlmm] Existing pool hydration failed:', error); }
  await refreshSol(); setInterval(() => void refreshSol(), 180_000); connect(); connectPrice();
}
