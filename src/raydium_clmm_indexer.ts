import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import bs58 from 'bs58';
import type { RegisterRaydiumPricePool } from './raydium_cpmm_price_fetcher.js';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';

export const RAYDIUM_CLMM_PROGRAM_ID = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK';
const WRAPPED_SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SWAP_DISCRIMINATOR = Buffer.from([43, 4, 237, 11, 26, 201, 30, 98]);
const FAILURE_FILE = process.env.RAYDIUM_CLMM_FAILURE_FILE ?? 'raydium-clmm-failures.jsonl';
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SWAP_THRESHOLD = Number(process.env.RAYDIUM_CLMM_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 300);
const UNIQUE_WALLET_THRESHOLD = Number(process.env.RAYDIUM_CLMM_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 30);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);

type Instruction = { programId?: string; programIdIndex?: number; accounts?: Array<number | string>; data?: string };
export type RaydiumClmmSwap = {
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

type PoolActivity = RaydiumClmmSwap & { events: Array<{ wallet: string; timestamp: number }> };
type TokenMetadata = { name: string | null; symbol: string | null; logo: string | null; decimals: number | null };

function decode(data: unknown): Buffer | null {
  if (typeof data !== 'string') return null;
  try { return Buffer.from(bs58.decode(data)); } catch { return null; }
}

function supportedPair(token0: string, token1: string): boolean {
  return token0 === WRAPPED_SOL_MINT || token1 === WRAPPED_SOL_MINT || token0 === USDC_MINT || token1 === USDC_MINT;
}

async function fetchMetadata(mints: string[]): Promise<Map<string, TokenMetadata>> {
  const result = new Map<string, TokenMetadata>([
    [WRAPPED_SOL_MINT, { name: 'Wrapped SOL', symbol: 'SOL', logo: null, decimals: 9 }],
    [USDC_MINT, { name: 'USD Coin', symbol: 'USDC', logo: null, decimals: 6 }],
  ]);
  if (!METAPLEX_RPC_URL) return result;
  try {
    const response = await fetch(METAPLEX_RPC_URL, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify(mints.map((mint, index) => ({ jsonrpc: '2.0', id: `raydium-clmm-metadata-${index}`, method: 'getAsset', params: { id: mint } }))),
    });
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
  } catch (error) {
    console.warn('[raydium-clmm] Token metadata enrichment failed:', error);
  }
  return result;
}

export function parseClmmSwap(tx: any, slot: number): RaydiumClmmSwap | null {
  const message = tx?.transaction?.message;
  const keys = Array.isArray(message?.accountKeys) ? message.accountKeys.map((key: any) => typeof key === 'string' ? key : key?.pubkey ?? '') : [];
  const programIndex = keys.indexOf(RAYDIUM_CLMM_PROGRAM_ID);
  if (programIndex < 0) return null;
  const outer: Instruction[] = Array.isArray(message?.instructions) ? message.instructions : [];
  const inner: Instruction[] = Array.isArray(tx?.meta?.innerInstructions) ? tx.meta.innerInstructions.flatMap((entry: any) => entry?.instructions ?? []) : [];
  for (const instruction of [...outer, ...inner]) {
    const matchesProgram = instruction.programId === RAYDIUM_CLMM_PROGRAM_ID || instruction.programIdIndex === programIndex;
    if (!matchesProgram) continue;
    const data = decode(instruction.data);
    if (!data?.subarray(0, 8).equals(SWAP_DISCRIMINATOR)) continue;
    const accounts = (instruction.accounts ?? []).map((item) => typeof item === 'number' ? keys[item] : item).filter(Boolean) as string[];
    if (accounts.length < 13) continue;
    const wallet = accounts[0];
    const ammConfig = accounts[1];
    const pool = accounts[2];
    const vault0 = accounts[5];
    const vault1 = accounts[6];
    const observationKey = accounts[7];
    const token0 = accounts[11];
    const token1 = accounts[12];
    if (!wallet || !ammConfig || !pool || !vault0 || !vault1 || !observationKey || !token0 || !token1 || !supportedPair(token0, token1)) continue;
    return { pool, wallet, ammConfig, token0, token1, vault0, vault1, observationKey, slot };
  }
  return null;
}

async function recordFailure(pool: string, error: unknown) {
  try { await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: pool, error: String(error) })}\n`, 'utf8'); } catch { /* logging must not stop the stream */ }
}

export function createRaydiumClmmProcessor(pgPool: PgPool, registerPricePool?: RegisterRaydiumPricePool) {
  const activities = new Map<string, PoolActivity>();
  const promoted = new Set<string>();
  return async (tx: any, slot: number, source = 'direct') => {
    if (tx?.meta?.err) return;
    const swap = parseClmmSwap(tx, slot);
    if (!swap) return;
    const now = Date.now();
    const activity = activities.get(swap.pool) ?? { ...swap, events: [] };
    activity.events = activity.events.filter((event) => event.timestamp >= now - WINDOW_MS);
    activity.events.push({ wallet: swap.wallet, timestamp: now });
    activities.set(swap.pool, activity);
    console.log(`[raydium-clmm][${source}] Tracked pool ${swap.pool} | swaps=${activity.events.length} | walletCount=${new Set(activity.events.map((event) => event.wallet)).size}`);
    if (promoted.has(swap.pool) || activity.events.length < SWAP_THRESHOLD || new Set(activity.events.map((event) => event.wallet)).size < UNIQUE_WALLET_THRESHOLD) return;
    promoted.add(swap.pool);
    try {
      const metadata = await fetchMetadata([swap.token0, swap.token1]);
      const token0Metadata = metadata.get(swap.token0);
      const token1Metadata = metadata.get(swap.token1);
      await pgPool.query(
        `INSERT INTO raydium_pools (
          address, pool_type, program_id, network, amm_config, owner,
          token_mint_0, token_mint_0_name, token_mint_0_symbol, token_mint_0_decimals, token_mint_0_total_supply_raw, token_mint_0_logo_url,
          token_mint_1, token_mint_1_name, token_mint_1_symbol, token_mint_1_decimals, token_mint_1_total_supply_raw, token_mint_1_logo_url,
          token_vault_0, token_vault_1, observation_key, tick_spacing, sqrt_price_x64, tick_current, updated_slot, discovered_at, indexed_at
        ) VALUES ($1, 'raydium_clmm', $2, 'solana', $3, $4, $5, $6, $7, $8, 0, $9, $10, $11, $12, $13, 0, $14, $15, $16, $17, 0, 0, 0, $18, NOW(), NOW())
        ON CONFLICT (address) DO UPDATE SET
          pool_type = EXCLUDED.pool_type, program_id = EXCLUDED.program_id, network = EXCLUDED.network,
          amm_config = EXCLUDED.amm_config, owner = EXCLUDED.owner,
          token_mint_0 = EXCLUDED.token_mint_0, token_mint_0_name = EXCLUDED.token_mint_0_name, token_mint_0_symbol = EXCLUDED.token_mint_0_symbol,
          token_mint_0_decimals = EXCLUDED.token_mint_0_decimals, token_mint_0_logo_url = EXCLUDED.token_mint_0_logo_url,
          token_mint_1 = EXCLUDED.token_mint_1, token_mint_1_name = EXCLUDED.token_mint_1_name, token_mint_1_symbol = EXCLUDED.token_mint_1_symbol,
          token_mint_1_decimals = EXCLUDED.token_mint_1_decimals, token_mint_1_logo_url = EXCLUDED.token_mint_1_logo_url,
          token_vault_0 = EXCLUDED.token_vault_0, token_vault_1 = EXCLUDED.token_vault_1,
          observation_key = EXCLUDED.observation_key, updated_slot = EXCLUDED.updated_slot, indexed_at = NOW()`,
        [swap.pool, RAYDIUM_CLMM_PROGRAM_ID, swap.ammConfig, swap.wallet,
          swap.token0, token0Metadata?.name ?? null, token0Metadata?.symbol ?? null, token0Metadata?.decimals ?? 9, token0Metadata?.logo ?? null,
          swap.token1, token1Metadata?.name ?? null, token1Metadata?.symbol ?? null, token1Metadata?.decimals ?? 9, token1Metadata?.logo ?? null,
          swap.vault0, swap.vault1, swap.observationKey, swap.slot],
      );
      registerPricePool?.({ address: swap.pool, token0: swap.token0, token1: swap.token1, token0_decimals: token0Metadata?.decimals ?? 9, token1_decimals: token1Metadata?.decimals ?? 9, token0_vault: swap.vault0, token1_vault: swap.vault1, pool_type: 'raydium_clmm' });
      console.log(`[raydium-clmm] Promoted ${swap.pool} | swaps=${activity.events.length} | wallets=${new Set(activity.events.map((event) => event.wallet)).size}`);
    } catch (error) {
      promoted.delete(swap.pool);
      console.error(`[raydium-clmm] Promotion failed for ${swap.pool}:`, error);
      await recordFailure(swap.pool, error);
    }
  };
}

export async function startRaydiumClmmIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, registerPricePool?: RegisterRaydiumPricePool): Promise<void> {
  const endpoints = normalizeWebsocketEndpoints(websocketUrl, '');
  if (endpoints.length === 0) {
    console.warn('[raydium-clmm] RAYDIUM_WS_URL is not configured; Raydium CLMM indexing is disabled.');
    return;
  }

  console.log(`[raydium-clmm] Starting indexer for program ${RAYDIUM_CLMM_PROGRAM_ID}.`);
  const processTransaction = createRaydiumClmmProcessor(pgPool, registerPricePool);
  let socket: WebSocket | undefined;
  let reconnectDelayMs = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let endpointIndex = 0;

  const connect = () => {
    const endpoint = endpoints[endpointIndex] ?? endpoints[0];
    if (!endpoint) return;
    socket = new WebSocket(endpoint);
    socket.on('open', () => {
      reconnectDelayMs = 1000;
      socket?.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'blockSubscribe', params: [
        { mentionsAccountOrProgram: RAYDIUM_CLMM_PROGRAM_ID },
        { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 },
      ] }));
      console.log('[raydium-clmm] block subscription started.');
    });
    socket.on('message', (raw) => {
      let payload: any;
      try { payload = JSON.parse(raw.toString()); } catch { return; }
      if (payload.error) {
        console.error('[raydium-clmm] websocket error:', payload.error);
        void recordFailure('subscription', payload.error);
        return;
      }
      const block = payload?.params?.result?.value?.block;
      if (!block) return;
      const slot = Number(payload?.params?.result?.context?.slot ?? 0);
      for (const tx of block.transactions ?? []) {
        void processTransaction(tx, slot, 'websocket').catch(async (error) => {
          console.error('[raydium-clmm] transaction processing failed:', error);
          await recordFailure('transaction', error);
        });
      }
    });
    socket.on('error', (error) => {
      console.error('[raydium-clmm] websocket failure:', error);
      void recordFailure('websocket', error);
    });
    socket.on('close', (code, reason) => {
      const next = nextWebsocketEndpoint(endpoints, endpointIndex);
      endpointIndex = next.index;
      console.warn(`[raydium-clmm] websocket closed: ${code} ${reason.toString()}`);
      void recordFailure('websocket-close', { code, reason: reason.toString() });
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = undefined; connect(); }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    });
  };

  connect();
}
