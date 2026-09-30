import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import bs58 from 'bs58';
import type { RegisterRaydiumPricePool } from './raydium_cpmm_price_fetcher.js';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

const RAYDIUM_CPMM_PROGRAM_ID = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';
const WRAPPED_SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SWAP_BASE_INPUT = Buffer.from([143, 190, 90, 218, 196, 30, 51, 222]);
const SWAP_BASE_OUTPUT = Buffer.from([55, 217, 98, 86, 163, 74, 180, 173]);
const FAILURE_FILE = process.env.RAYDIUM_FAILURE_FILE ?? 'raydium-failures.jsonl';
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SWAP_THRESHOLD = Number(process.env.RAYDIUM_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 300);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.RAYDIUM_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 30);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);

export type RaydiumSwap = {
  pool: string;
  wallet: string;
  ammConfig: string;
  token0: string;
  token1: string;
  vault0: string;
  vault1: string;
  observationKey: string;
  slot: number;
};

type PoolActivity = RaydiumSwap & { events: Array<{ wallet: string; timestamp: number }>; promoted: boolean; lastSeenAt: number };

type AccountKey = string | { pubkey?: string };
type TokenMetadata = { name: string | null; symbol: string | null; logo: string | null; decimals: number | null };

function address(value: AccountKey | undefined): string {
  return typeof value === 'string' ? value : value?.pubkey ?? '';
}

function decode(data: unknown): Buffer | null {
  if (typeof data !== 'string') return null;
  try { return Buffer.from(bs58.decode(data)); } catch { return null; }
}

function supportedPair(token0: string, token1: string): boolean {
  return token0 === WRAPPED_SOL_MINT || token1 === WRAPPED_SOL_MINT || token0 === USDC_MINT || token1 === USDC_MINT;
}

function readTokenMetadata(asset: any): TokenMetadata | null {
  if (!asset?.id) return null;
  const metadata = asset.content?.metadata ?? {};
  const links = asset.content?.links ?? {};
  const image = Array.isArray(asset.content?.files)
    ? asset.content.files.find((file: any) => typeof file?.uri === 'string' && (file.mime?.startsWith('image/') || file.type?.startsWith('image/')))?.uri
    : null;
  return {
    name: typeof metadata.name === 'string' && metadata.name.trim() ? metadata.name.trim() : null,
    symbol: typeof metadata.symbol === 'string' && metadata.symbol.trim() ? metadata.symbol.trim() : null,
    logo: typeof links.image === 'string' ? links.image : image,
    decimals: typeof asset.token_info?.decimals === 'number' ? asset.token_info.decimals : null,
  };
}

async function fetchTokenMetadata(mints: string[]): Promise<Map<string, TokenMetadata>> {
  const result = new Map<string, TokenMetadata>();
  if (!METAPLEX_RPC_URL) return result;
  try {
    const response = await fetch(METAPLEX_RPC_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(mints.map((mint, index) => ({ jsonrpc: '2.0', id: `raydium-metadata-${index}`, method: 'getAsset', params: { id: mint } }))),
    });
    if (!response.ok) throw new Error(`Metaplex DAS returned HTTP ${response.status}`);
    const body = await response.json() as Array<{ result?: any }>;
    for (const item of body) {
      const metadata = readTokenMetadata(item.result);
      if (metadata && item.result?.id) result.set(item.result.id, metadata);
    }
  } catch (error) {
    console.warn('[raydium] Token metadata enrichment failed:', error);
  }
  return result;
}

export function parseSwap(tx: any, slot: number): RaydiumSwap | null {
  const message = tx?.transaction?.message;
  const keys = Array.isArray(message?.accountKeys) ? message.accountKeys.map(address) : [];
  const programIndex = keys.indexOf(RAYDIUM_CPMM_PROGRAM_ID);
  if (programIndex < 0) return null;

  const outer = Array.isArray(message.instructions) ? message.instructions : [];
  const inner = Array.isArray(tx?.meta?.innerInstructions)
    ? tx.meta.innerInstructions.flatMap((entry: any) => entry?.instructions ?? [])
    : [];

  for (const instruction of [...outer, ...inner]) {
    const hasProgramAddress = typeof instruction?.programId === 'string';
    const matchesProgram = hasProgramAddress
      ? instruction.programId === RAYDIUM_CPMM_PROGRAM_ID
      : instruction?.programIdIndex === programIndex;
    const bytes = decode(instruction?.data);
    if (!matchesProgram || !bytes || bytes.length < 8) continue;
    if (!bytes.subarray(0, 8).equals(SWAP_BASE_INPUT) && !bytes.subarray(0, 8).equals(SWAP_BASE_OUTPUT)) continue;

    const accounts = (instruction.accounts ?? []).map((item: number | string) => typeof item === 'number' ? keys[item] : item).filter(Boolean);
    if (accounts.length < 13) continue;
    const wallet = accounts[0];
    const ammConfig = accounts[2];
    const pool = accounts[3];
    const vault0 = accounts[6];
    const vault1 = accounts[7];
    const token0 = accounts[10];
    const token1 = accounts[11];
    const observationKey = accounts[12];
    if (!wallet || !pool || !ammConfig || !vault0 || !vault1 || !token0 || !token1 || !observationKey || pool === ammConfig || !supportedPair(token0, token1)) continue;
    return { pool, wallet, ammConfig, token0, token1, vault0, vault1, observationKey, slot };
  }
  return null;
}

async function recordFailure(pool: string, error: unknown): Promise<void> {
  const value = { timestamp: new Date().toISOString(), poolAddress: pool, error: String(error) };
  try { await appendFile(FAILURE_FILE, `${JSON.stringify(value)}\n`, 'utf8'); } catch (writeError) { console.error('Raydium failure log write failed:', writeError); }
}

export function createRaydiumCpmmProcessor(pgPool: PgPool, registerPricePool?: RegisterRaydiumPricePool) {
  const activities = new Map<string, PoolActivity>();
  const promoted = new Set<string>();

  const promote = async (activity: PoolActivity) => {
    if (promoted.has(activity.pool) || activity.events.length < SWAP_THRESHOLD || new Set(activity.events.map((event) => event.wallet)).size < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(activity.pool);
    try {
      const metadata = await fetchTokenMetadata([activity.token0, activity.token1]);
      const token0Metadata = metadata.get(activity.token0);
      const token1Metadata = metadata.get(activity.token1);
      const token0Decimals = token0Metadata?.decimals ?? 9;
      const token1Decimals = token1Metadata?.decimals ?? 9;
      await pgPool.query(
        `INSERT INTO raydium_cpmm_pools (
          address, pool_type, program_id, network, amm_config, pool_creator,
          token0, token0_name, token0_symbol, token0_logo_url, token0_decimals,
          token1, token1_name, token1_symbol, token1_logo_url, token1_decimals, token0_vault, token1_vault,
          lp_mint, updated_slot, discovered_at, indexed_at
        ) VALUES ($1, 'raydium_cpmm', $2, 'solana', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'unknown', $17, NOW(), NOW())
        ON CONFLICT (address) DO UPDATE SET
          amm_config = EXCLUDED.amm_config, pool_creator = EXCLUDED.pool_creator,
          token0 = EXCLUDED.token0, token0_name = EXCLUDED.token0_name, token0_symbol = EXCLUDED.token0_symbol, token0_logo_url = EXCLUDED.token0_logo_url,
          token1 = EXCLUDED.token1, token1_name = EXCLUDED.token1_name, token1_symbol = EXCLUDED.token1_symbol, token1_logo_url = EXCLUDED.token1_logo_url,
          token0_decimals = EXCLUDED.token0_decimals, token1_decimals = EXCLUDED.token1_decimals,
          token0_vault = EXCLUDED.token0_vault, token1_vault = EXCLUDED.token1_vault,
          updated_slot = EXCLUDED.updated_slot, indexed_at = NOW()`,
        [activity.pool, RAYDIUM_CPMM_PROGRAM_ID, activity.ammConfig, activity.wallet,
          activity.token0, token0Metadata?.name, token0Metadata?.symbol, token0Metadata?.logo, token0Decimals,
          activity.token1, token1Metadata?.name, token1Metadata?.symbol, token1Metadata?.logo, token1Decimals,
          activity.vault0, activity.vault1, activity.slot],
      );
      registerPricePool?.({ address: activity.pool, token0: activity.token0, token1: activity.token1, token0_decimals: token0Decimals, token1_decimals: token1Decimals, token0_vault: activity.vault0, token1_vault: activity.vault1 });
      console.log(`[raydium] Promoted ${activity.pool} | swaps=${activity.events.length} | wallets=${new Set(activity.events.map((event) => event.wallet)).size}`);
    } catch (error) {
      promoted.delete(activity.pool);
      console.error(`[raydium] Promotion failed for ${activity.pool}:`, error);
      await recordFailure(activity.pool, error);
    }
  };

  return async (tx: any, slot: number, source = 'direct') => {
    if (tx?.meta?.err) return;
    const swap = parseSwap(tx, slot);
    if (!swap) return;
    const now = Date.now();
    const activity = activities.get(swap.pool) ?? { ...swap, events: [], promoted: false, lastSeenAt: now };
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet: swap.wallet, timestamp: now });
    activity.lastSeenAt = now;
    activities.set(swap.pool, activity);
    console.log(`[raydium][${source}] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${new Set(activity.events.map((event) => event.wallet)).size}`);
    await promote(activity);
  };
}

export async function startRaydiumCpmmIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, registerPricePool?: RegisterRaydiumPricePool): Promise<void> {
  const endpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  if (endpoints.length === 0) {
    console.warn('[raydium] RAYDIUM_WS_URL is not configured; Raydium CPMM indexing is disabled.');
    return;
  }

  console.log(`[raydium] Starting CPMM indexer for program ${RAYDIUM_CPMM_PROGRAM_ID}.`);
  const activities = new Map<string, PoolActivity>();
  const promoted = new Set<string>();
  let socket: WebSocket | undefined;
  let reconnectDelayMs = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let endpointIndex = 0;

  const promote = async (activity: PoolActivity) => {
    if (promoted.has(activity.pool) || activity.events.length < SWAP_THRESHOLD || new Set(activity.events.map((event) => event.wallet)).size < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(activity.pool);
    try {
      const metadata = await fetchTokenMetadata([activity.token0, activity.token1]);
      const token0Metadata = metadata.get(activity.token0);
      const token1Metadata = metadata.get(activity.token1);
      const token0Decimals = token0Metadata?.decimals ?? 9;
      const token1Decimals = token1Metadata?.decimals ?? 9;
      await pgPool.query(
        `INSERT INTO raydium_cpmm_pools (
          address, pool_type, program_id, network, amm_config, pool_creator,
          token0, token0_name, token0_symbol, token0_logo_url, token0_decimals,
          token1, token1_name, token1_symbol, token1_logo_url, token1_decimals, token0_vault, token1_vault,
          lp_mint, updated_slot, discovered_at, indexed_at
        ) VALUES ($1, 'raydium_cpmm', $2, 'solana', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'unknown', $17, NOW(), NOW())
        ON CONFLICT (address) DO UPDATE SET
          amm_config = EXCLUDED.amm_config, pool_creator = EXCLUDED.pool_creator,
          token0 = EXCLUDED.token0, token0_name = EXCLUDED.token0_name, token0_symbol = EXCLUDED.token0_symbol, token0_logo_url = EXCLUDED.token0_logo_url,
          token1 = EXCLUDED.token1, token1_name = EXCLUDED.token1_name, token1_symbol = EXCLUDED.token1_symbol, token1_logo_url = EXCLUDED.token1_logo_url,
          token0_decimals = EXCLUDED.token0_decimals, token1_decimals = EXCLUDED.token1_decimals,
          token0_vault = EXCLUDED.token0_vault, token1_vault = EXCLUDED.token1_vault,
          updated_slot = EXCLUDED.updated_slot, indexed_at = NOW()`,
        [activity.pool, RAYDIUM_CPMM_PROGRAM_ID, activity.ammConfig, activity.wallet,
          activity.token0, token0Metadata?.name, token0Metadata?.symbol, token0Metadata?.logo, token0Decimals,
          activity.token1, token1Metadata?.name, token1Metadata?.symbol, token1Metadata?.logo, token1Decimals,
          activity.vault0, activity.vault1, activity.slot],
      );
      registerPricePool?.({
        address: activity.pool,
        token0: activity.token0,
        token1: activity.token1,
        token0_decimals: token0Decimals,
        token1_decimals: token1Decimals,
        token0_vault: activity.vault0,
        token1_vault: activity.vault1,
      });
      console.log(`[raydium] Promoted ${activity.pool} | swaps=${activity.events.length} | wallets=${new Set(activity.events.map((event) => event.wallet)).size}`);
    } catch (error) {
      promoted.delete(activity.pool);
      console.error(`[raydium] Promotion failed for ${activity.pool}:`, error);
      await recordFailure(activity.pool, error);
    }
  };

  const connect = () => {
    const endpoint = endpoints[endpointIndex] ?? endpoints[0];
    if (!endpoint) return;
    socket = new WebSocket(endpoint);
    socket.on('open', () => {
      reconnectDelayMs = 1000;
      socket?.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'blockSubscribe', params: [
        { mentionsAccountOrProgram: RAYDIUM_CPMM_PROGRAM_ID },
        { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 2 },
      ] }));
      console.log('[raydium] block subscription started.');
    });
    socket.on('message', (raw) => {
      let payload: any;
      try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) { console.error('[raydium] websocket error:', payload.error); void recordFailure('subscription', payload.error); return; }
      const block = payload?.params?.result?.value?.block;
      if (!block) return;
      const slot = Number(payload?.params?.result?.context?.slot ?? 0);
      for (const tx of block.transactions ?? []) {
        if (tx?.meta?.err) continue;
        const swap = parseSwap(tx, slot);
        if (!swap) continue;
        const now = Date.now();
        const activity = activities.get(swap.pool) ?? { ...swap, events: [], promoted: false, lastSeenAt: now };
        activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
        activity.events.push({ wallet: swap.wallet, timestamp: now });
        activity.lastSeenAt = now;
        activities.set(swap.pool, activity);
        console.log(`[raydium] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${new Set(activity.events.map((event) => event.wallet)).size}`);
        void promote(activity);
      }
    });
    socket.on('error', (error) => { console.error('[raydium] websocket failure:', error); void recordFailure('websocket', error); });
    socket.on('close', (code, reason) => {
      const next = nextWebsocketEndpoint(endpoints, endpointIndex);
      endpointIndex = next.index;
      console.warn(`[raydium] websocket closed: ${code} ${reason.toString()}`);
      void recordFailure('websocket-close', { code, reason: reason.toString() });
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    });
  };

  connect();
  setInterval(() => {
    const now = Date.now();
    for (const [pool, activity] of activities) {
      activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
      if (activity.events.length === 0 && !promoted.has(pool)) activities.delete(pool);
    }
  }, 30000);
}
