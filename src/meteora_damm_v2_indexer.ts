import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { getReferencePrice } from './reference_prices.js';
import bs58 from 'bs58';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

export const METEORA_DAMM_V2_PROGRAM_ID = 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SWAP_DISCRIMINATORS = [
  [248, 198, 158, 145, 225, 117, 135, 200],
  [65, 75, 63, 76, 235, 91, 91, 136],
].map((value) => Buffer.from(value));
const POOL_DISCRIMINATOR = Buffer.from([241, 154, 109, 4, 17, 177, 109, 188]);
const SQRT_PRICE_OFFSET = 456;
const FAILURE_FILE = process.env.METEORA_DAMM_V2_FAILURE_FILE ?? 'meteora-damm-v2-failures.jsonl';
const PRICE_FILE = process.env.METEORA_DAMM_V2_PRICE_EVENT_FILE ?? 'meteora-damm-v2-price-events.jsonl';
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SWAP_THRESHOLD = Number(process.env.METEORA_DAMM_V2_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 300);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.METEORA_DAMM_V2_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 30);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const ACCOUNT_SUBSCRIBE_DELAY_MS = Number(process.env.METEORA_DAMM_V2_ACCOUNT_SUBSCRIBE_DELAY_MS ?? 500);
const TIMEFRAMES = [{ name: '1m', milliseconds: 60_000 }, { name: '5m', milliseconds: 300_000 }, { name: '15m', milliseconds: 900_000 }, { name: '1h', milliseconds: 3_600_000 }, { name: '4h', milliseconds: 14_400_000 }, { name: '1d', milliseconds: 86_400_000 }] as const;

type Instruction = { programId?: string; programIdIndex?: number; accounts?: Array<number | string>; data?: string };
type Metadata = { name: string | null; symbol: string | null; logo: string | null; decimals: number | null };
type Swap = { pool: string; wallet: string; tokenA: string; tokenB: string; vaultA: string; vaultB: string; slot: number };
type PricePool = Swap & { tokenADecimals: number; tokenBDecimals: number };

function decode(value: unknown): Buffer | null { try { return typeof value === 'string' ? Buffer.from(bs58.decode(value)) : null; } catch { return null; } }
function supportedPair(tokenA: string, tokenB: string): boolean { return tokenA === SOL_MINT || tokenB === SOL_MINT || tokenA === USDC_MINT || tokenB === USDC_MINT; }
function addresses(instruction: Instruction, keys: string[]): string[] { return (instruction.accounts ?? []).map((item) => typeof item === 'number' ? keys[item] ?? '' : item); }

export function parseMeteoraDammV2Swap(tx: any, slot: number): Swap | null {
  const message = tx?.transaction?.message;
  const keys = Array.isArray(message?.accountKeys) ? message.accountKeys.map((key: any) => typeof key === 'string' ? key : key?.pubkey ?? '') : [];
  const programIndex = keys.indexOf(METEORA_DAMM_V2_PROGRAM_ID);
  if (programIndex < 0) return null;
  const instructions: Instruction[] = [...(message.instructions ?? []), ...((tx.meta?.innerInstructions ?? []).flatMap((entry: any) => entry.instructions ?? []))];
  for (const instruction of instructions) {
    if (instruction.programId !== METEORA_DAMM_V2_PROGRAM_ID && instruction.programIdIndex !== programIndex) continue;
    const data = decode(instruction.data);
    if (!data || !SWAP_DISCRIMINATORS.some((value) => data.subarray(0, 8).equals(value))) continue;
    const account = addresses(instruction, keys);
    if (account.length < 9) continue;
    const pool = account[1];
    const vaultA = account[4];
    const vaultB = account[5];
    const tokenA = account[6];
    const tokenB = account[7];
    const wallet = account[8];
    if (pool && vaultA && vaultB && tokenA && tokenB && wallet && supportedPair(tokenA, tokenB)) return { pool, wallet, tokenA, tokenB, vaultA, vaultB, slot };
  }
  return null;
}

function readPoolState(data: unknown): { sqrtPrice: bigint; activationPoint: bigint; poolStatus: number } | null {
  if (!Array.isArray(data) || typeof data[0] !== 'string') return null;
  try {
    const bytes = Buffer.from(data[0], 'base64');
    if (bytes.length < SQRT_PRICE_OFFSET + 16 || !bytes.subarray(0, 8).equals(POOL_DISCRIMINATOR)) return null;
    let sqrtPrice = 0n;
    for (let index = 0; index < 16; index += 1) sqrtPrice |= BigInt(bytes[SQRT_PRICE_OFFSET + index]) << BigInt(index * 8);
    return { sqrtPrice, activationPoint: bytes.readBigUInt64LE(472), poolStatus: bytes[481] };
  } catch { return null; }
}

async function fetchMetadata(mints: string[]): Promise<Map<string, Metadata>> {
  const result = new Map<string, Metadata>([[SOL_MINT, { name: 'Wrapped SOL', symbol: 'SOL', logo: null, decimals: 9 }], [USDC_MINT, { name: 'USD Coin', symbol: 'USDC', logo: null, decimals: 6 }]]);
  if (!METAPLEX_RPC_URL) return result;
  try {
    const response = await fetch(METAPLEX_RPC_URL, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(mints.map((mint, index) => ({ jsonrpc: '2.0', id: `meteora-damm-v2-${index}`, method: 'getAsset', params: { id: mint } }))) });
    if (!response.ok) throw new Error(`Metaplex DAS returned HTTP ${response.status}`);
    for (const item of await response.json() as Array<{ result?: any }>) {
      const asset = item.result;
      if (!asset?.id) continue;
      const metadata = asset.content?.metadata ?? {};
      const links = asset.content?.links ?? {};
      result.set(asset.id, { name: typeof metadata.name === 'string' ? metadata.name : null, symbol: typeof metadata.symbol === 'string' ? metadata.symbol : null, logo: typeof links.image === 'string' ? links.image : null, decimals: typeof asset.token_info?.decimals === 'number' ? asset.token_info.decimals : null });
    }
  } catch (error) { console.warn('[meteora-damm-v2] Token metadata enrichment failed:', error); }
  return result;
}
async function failure(pool: string, error: unknown): Promise<void> { try { await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: pool, error: String(error) })}\n`, 'utf8'); } catch { /* logging must not stop the stream */ } }

export function createMeteoraDammV2Processor(pgPool: PgPool, registerPricePool?: (pool: PricePool) => void) {
  const activities = new Map<string, { swap: Swap; events: Array<{ wallet: string; timestamp: number }> }>();
  const promoted = new Set<string>();
  return async (tx: any, slot: number, source = 'websocket') => {
    if (tx?.meta?.err) return;
    const swap = parseMeteoraDammV2Swap(tx, slot);
    if (!swap) return;
    const now = Date.now();
    const activity = activities.get(swap.pool) ?? { swap, events: [] };
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet: swap.wallet, timestamp: now });
    activities.set(swap.pool, activity);
    const wallets = new Set(activity.events.map((event) => event.wallet)).size;
    console.log(`[meteora-damm-v2][${source}] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${wallets}`);
    if (promoted.has(swap.pool) || activity.events.length < SWAP_THRESHOLD || wallets < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(swap.pool);
    try {
      const metadata = await fetchMetadata([swap.tokenA, swap.tokenB]);
      const tokenA = metadata.get(swap.tokenA); const tokenB = metadata.get(swap.tokenB);
      await pgPool.query(`INSERT INTO meteora_damm_v2_pools (address, pool_type, program_id, network, creator, token_a_mint, token_a_name, token_a_symbol, token_a_decimals, token_a_total_supply_raw, token_a_logo_url, token_b_mint, token_b_name, token_b_symbol, token_b_decimals, token_b_total_supply_raw, token_b_logo_url, token_a_vault, token_b_vault, token_a_amount, token_b_amount, sqrt_price, activation_point, pool_mode, updated_slot, discovered_at, indexed_at) VALUES ($1,'meteora_damm_v2',$2,'solana',$3,$4,$5,$6,$7,0,$8,$9,$10,$11,$12,0,$13,$14,$15,0,0,0,0,0,$16,NOW(),NOW()) ON CONFLICT (address) DO UPDATE SET creator=EXCLUDED.creator, token_a_name=EXCLUDED.token_a_name, token_a_symbol=EXCLUDED.token_a_symbol, token_a_decimals=EXCLUDED.token_a_decimals, token_a_logo_url=EXCLUDED.token_a_logo_url, token_b_name=EXCLUDED.token_b_name, token_b_symbol=EXCLUDED.token_b_symbol, token_b_decimals=EXCLUDED.token_b_decimals, token_b_logo_url=EXCLUDED.token_b_logo_url, token_a_vault=EXCLUDED.token_a_vault, token_b_vault=EXCLUDED.token_b_vault, updated_slot=EXCLUDED.updated_slot, indexed_at=NOW()`, [swap.pool, METEORA_DAMM_V2_PROGRAM_ID, swap.wallet, swap.tokenA, tokenA?.name ?? null, tokenA?.symbol ?? null, tokenA?.decimals ?? 9, tokenA?.logo ?? null, swap.tokenB, tokenB?.name ?? null, tokenB?.symbol ?? null, tokenB?.decimals ?? 9, tokenB?.logo ?? null, swap.vaultA, swap.vaultB, swap.slot]);
      registerPricePool?.({ ...swap, tokenADecimals: tokenA?.decimals ?? 9, tokenBDecimals: tokenB?.decimals ?? 9 });
      console.log(`[meteora-damm-v2] Promoted ${swap.pool} | swaps=${activity.events.length} | wallets=${wallets}`);
    } catch (error) { promoted.delete(swap.pool); console.error(`[meteora-damm-v2] Promotion failed for ${swap.pool}:`, error); await failure(swap.pool, error); }
  };
}

export async function startMeteoraDammV2Indexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, priceFile = PRICE_FILE, priceWebsocketUrl: WebsocketEndpointInput = websocketUrl): Promise<void> {
  const blockEndpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  const priceEndpoints = normalizeWebsocketEndpoints(priceWebsocketUrl, blockEndpoints[0] ?? '');
  if (blockEndpoints.length === 0) { console.warn('[meteora-damm-v2] METEORA_DAMM_V2_WS_URL is not configured; indexing is disabled.'); return; }
  const pools = new Map<string, PricePool>(); const states = new Map<string, { sqrtPrice: bigint; activationPoint: bigint; poolStatus: number }>(); const reserves = new Map<string, { raw: bigint; slot: number }>();
  const subscriptions = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'reserve' }>(); const pending = new Map<number, { pool: PricePool; account: string; kind: 'state' | 'reserve' }>(); const requested = new Set<string>(); const queue: Array<{ pool: PricePool; account: string; kind: 'state' | 'reserve' }> = []; let draining = false;
  let socket: WebSocket | undefined; let priceSocket: WebSocket | undefined; let requestId = 1; let reconnectDelay = 1000; let reconnectTimer: NodeJS.Timeout | undefined; let priceReconnectTimer: NodeJS.Timeout | undefined; let solUsd: number | null = null;
  let endpointIndex = 0; let priceEndpointIndex = 0;
  const logPrice = async (event: Record<string, unknown>) => { try { await appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`, 'utf8'); } catch (error) { console.error('[meteora-damm-v2-price] event log failed:', error); } };
  const refreshSol = async () => { try { solUsd = await getReferencePrice('sol'); } catch { /* retain last good SOL price */ } };
  const writePrice = async (pool: PricePool, slot: number) => { const state = states.get(pool.pool); if (!state || state.sqrtPrice === 0n) return; const price = (Number(state.sqrtPrice) / 2 ** 64) ** 2 * 10 ** (pool.tokenADecimals - pool.tokenBDecimals); const inversePrice = 1 / price; const rawA = reserves.get(pool.vaultA); const rawB = reserves.get(pool.vaultB); if (!rawA || !rawB || !Number.isFinite(price) || price <= 0) return; const reserveA = Number(rawA.raw) / 10 ** pool.tokenADecimals; const reserveB = Number(rawB.raw) / 10 ** pool.tokenBDecimals; const tokenPriceUsd = pool.tokenB === USDC_MINT ? price : pool.tokenA === USDC_MINT ? inversePrice : pool.tokenB === SOL_MINT && solUsd ? price * solUsd : pool.tokenA === SOL_MINT && solUsd ? inversePrice * solUsd : null; const now = Date.now(); let high24h = price; let low24h = price; const client = await pgPool.connect(); try { await client.query('BEGIN'); await client.query(`INSERT INTO latest_prices (pool_address,price,inverse_price,price_change,price_change_percent,price_change_direction,fdv_usd,token_price_usd,total_supply,supply_basis,base_reserve,quote_reserve,updated_slot,updated_at) VALUES ($1,$2,$3,NULL,NULL,NULL,NULL,$4,NULL,'meteora_damm_v2_pool',$5,$6,$7,NOW()) ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price,inverse_price=EXCLUDED.inverse_price,token_price_usd=EXCLUDED.token_price_usd,price_change=EXCLUDED.price-latest_prices.price,price_change_percent=CASE WHEN latest_prices.price IS NULL OR latest_prices.price=0 THEN NULL ELSE ((EXCLUDED.price-latest_prices.price)/latest_prices.price)*100 END,price_change_direction=CASE WHEN latest_prices.price IS NULL THEN NULL WHEN EXCLUDED.price>latest_prices.price THEN 'up' WHEN EXCLUDED.price<latest_prices.price THEN 'down' ELSE 'flat' END,base_reserve=EXCLUDED.base_reserve,quote_reserve=EXCLUDED.quote_reserve,updated_slot=EXCLUDED.updated_slot,updated_at=EXCLUDED.updated_at`, [pool.pool,price,inversePrice,tokenPriceUsd,reserveA,reserveB,slot]); for (const timeframe of TIMEFRAMES) { const bucket = Math.floor(now / timeframe.milliseconds) * timeframe.milliseconds; await client.query(`INSERT INTO price_candles (pool_address,timeframe,bucket_start,open,high,low,close,volume,updated_at) VALUES ($1,$2,$3,$4,$4,$4,$4,NULL,NOW()) ON CONFLICT (pool_address,timeframe,bucket_start) DO UPDATE SET high=GREATEST(price_candles.high,EXCLUDED.high),low=LEAST(price_candles.low,EXCLUDED.low),close=EXCLUDED.close,updated_at=EXCLUDED.updated_at`, [pool.pool,timeframe.name,bucket,price]); } const rolling = await client.query<{ high: number | null; low: number | null }>(`SELECT MAX(high) AS high,MIN(low) AS low FROM price_candles WHERE pool_address=$1 AND timeframe='1m' AND bucket_start >= $2`, [pool.pool,now - 86_400_000]); high24h=rolling.rows[0]?.high ?? price; low24h=rolling.rows[0]?.low ?? price; await client.query(`UPDATE latest_prices SET high_24h=$2,low_24h=$3 WHERE pool_address=$1`, [pool.pool,high24h,low24h]); await client.query('COMMIT'); await logPrice({ type:'price',poolType:'meteora_damm_v2',poolAddress:pool.pool,price,inversePrice,tokenPriceUsd,high24h,low24h,activationPoint:state.activationPoint.toString(),poolStatus:state.poolStatus,reserveA,reserveB,solUsd,slot }); console.log(`[meteora-damm-v2-price] ${pool.pool} price=${price} inverse=${inversePrice} slot=${slot}`); } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error; } finally { client.release(); } };
  const drain = () => { if (draining || queue.length === 0) return; draining = true; const entry = queue.shift()!; if (priceSocket?.readyState === WebSocket.OPEN) { const id = requestId++; pending.set(id, entry); priceSocket.send(JSON.stringify({ jsonrpc:'2.0',id,method:'accountSubscribe',params:[entry.account,{commitment:'confirmed',encoding:'base64'}]})); } setTimeout(() => { draining = false; drain(); }, ACCOUNT_SUBSCRIBE_DELAY_MS); };
  const subscribe = (pool: PricePool, account: string, kind: 'state' | 'reserve') => { const key = `${kind}:${account}`; if (requested.has(key)) return; requested.add(key); queue.push({ pool,account,kind }); drain(); };
  const register = (pool: PricePool) => { pools.set(pool.pool,pool); subscribe(pool,pool.pool,'state'); subscribe(pool,pool.vaultA,'reserve'); subscribe(pool,pool.vaultB,'reserve'); };
  const processTransaction = createMeteoraDammV2Processor(pgPool,register);
  const handleAccount = async (payload: any, entry?: { pool: PricePool; account: string; kind: 'state' | 'reserve' }) => { if (!entry) return; const slot=Number(payload.params?.result?.context?.slot??0); if (entry.kind==='state') { const state=readPoolState(payload.params?.result?.value?.data); if (state) states.set(entry.pool.pool,state); } else { const data=payload.params?.result?.value?.data; if (Array.isArray(data)&&typeof data[0]==='string') { const bytes=Buffer.from(data[0],'base64'); if(bytes.length>=72){let raw=0n;for(let i=0;i<8;i++)raw|=BigInt(bytes[64+i])<<BigInt(i*8);reserves.set(entry.account,{raw,slot});} } } await writePrice(entry.pool,slot); };
  const connect = () => { const endpoint = blockEndpoints[endpointIndex] ?? blockEndpoints[0] ?? ''; if (!endpoint) return; socket=new WebSocket(endpoint); socket.on('open',()=>{reconnectDelay=1000;socket?.send(JSON.stringify({jsonrpc:'2.0',id:1,method:'blockSubscribe',params:[{mentionsAccountOrProgram:METEORA_DAMM_V2_PROGRAM_ID},{commitment:'confirmed',encoding:'jsonParsed',transactionDetails:'full',maxSupportedTransactionVersion:0}]}));console.log('[meteora-damm-v2] block subscription started.');for(const pool of pools.values()){subscribe(pool,pool.pool,'state');subscribe(pool,pool.vaultA,'reserve');subscribe(pool,pool.vaultB,'reserve');}}); socket.on('message',raw=>{let payload:any;try{payload=JSON.parse(raw.toString())}catch{return}if(payload.error){console.error('[meteora-damm-v2] websocket error:',payload.error);void failure('subscription',payload.error);return}if(payload.id!==undefined&&pending.has(payload.id)){const entry=pending.get(payload.id)!;pending.delete(payload.id);if(payload.result!==undefined)subscriptions.set(payload.result,entry);return}if(payload.method==='accountNotification'){void handleAccount(payload,subscriptions.get(payload.params?.subscription));return}const block=payload?.params?.result?.value?.block;if(!block)return;const slot=Number(payload?.params?.result?.context?.slot??0);for(const tx of block.transactions??[])void processTransaction(tx,slot).catch(error=>failure('transaction',error));});socket.on('error',error=>{console.error('[meteora-damm-v2] websocket failure:',error);void failure('websocket',error)});socket.on('close',(code,reason)=>{const next = nextWebsocketEndpoint(blockEndpoints, endpointIndex); endpointIndex = next.index; console.warn(`[meteora-damm-v2] websocket closed: ${code} ${reason.toString()}`);if(!reconnectTimer){reconnectTimer=setTimeout(()=>{reconnectTimer=undefined;connect()},reconnectDelay);reconnectDelay=Math.min(reconnectDelay*2,30000)}})};
  const connectPrice = () => { const endpoint = priceEndpoints[priceEndpointIndex] ?? priceEndpoints[0] ?? ''; if (!endpoint) return; priceSocket = new WebSocket(endpoint); priceSocket.on('open',()=>{for(const pool of pools.values()){subscribe(pool,pool.pool,'state');subscribe(pool,pool.vaultA,'reserve');subscribe(pool,pool.vaultB,'reserve');}}); priceSocket.on('message',raw=>{let payload:any;try{payload=JSON.parse(raw.toString())}catch{return}if(payload.error){void failure('price-subscription',payload.error);return}if(payload.id!==undefined&&pending.has(payload.id)){const entry=pending.get(payload.id)!;pending.delete(payload.id);if(payload.result!==undefined)subscriptions.set(payload.result,entry);return}if(payload.method==='accountNotification')void handleAccount(payload,subscriptions.get(payload.params?.subscription));}); priceSocket.on('error',error=>void failure('price-websocket',error)); priceSocket.on('close',()=>{const next = nextWebsocketEndpoint(priceEndpoints, priceEndpointIndex); priceEndpointIndex = next.index; for(const [id] of pending)pending.delete(id);if(!priceReconnectTimer)priceReconnectTimer=setTimeout(()=>{priceReconnectTimer=undefined;connectPrice()},reconnectDelay)}); };
  try { const existing=await pgPool.query<PricePool>(`SELECT address AS pool,creator AS wallet,token_a_mint AS "tokenA",token_b_mint AS "tokenB",token_a_vault AS "vaultA",token_b_vault AS "vaultB",updated_slot AS slot,token_a_decimals AS "tokenADecimals",token_b_decimals AS "tokenBDecimals" FROM meteora_damm_v2_pools WHERE pool_type='meteora_damm_v2' AND (token_a_mint IN ('${SOL_MINT}','${USDC_MINT}') OR token_b_mint IN ('${SOL_MINT}','${USDC_MINT}'))`);for(const pool of existing.rows)register(pool)}catch(error){console.warn('[meteora-damm-v2] Existing pool hydration failed:',error)} await refreshSol();setInterval(()=>void refreshSol(),180000);connect();connectPrice();
}
