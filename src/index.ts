import 'dotenv/config';
import { appendFile } from 'node:fs/promises';
import WebSocket from 'ws';
import bs58 from 'bs58';
import { PublicKey } from '@solana/web3.js';
import { createClient } from '@supabase/supabase-js';
import { Pool } from 'pg';
import { PoolTracker } from './tracker.js';
import { createRaydiumCpmmProcessor, startRaydiumCpmmIndexer } from './raydium_cpmm_indexer.js';
import type { RegisterRaydiumPricePool } from './raydium_cpmm_price_fetcher.js';
import { startRaydiumPriceFetcher } from './raydium_cpmm_price_fetcher.js';
import { startRaydiumClmmIndexer } from './raydium_clmm_indexer.js';
import { startMeteoraDlmmIndexer } from './meteora_dlmm_indexer.js';
import { startMeteoraDammV2Indexer } from './meteora_damm_v2_indexer.js';
import { startOrcaWhirlpoolIndexer } from './orca_whirlpool_indexer.js';
import { PUMPSWAP_PROGRAM_ID as DEFAULT_PUMPSWAP_PROGRAM_ID, getPumpAmmSwapMeta } from './pumpswap_decoder.js';
import { createPumpswapPriceFetcher, type RegisterPumpswapPricePool } from './pumpswap_price_fetcher.js';
import { startPancakeSwapV2Indexer } from './pancakeswap_v2_indexer.js';
import { startPancakeSwapV3Indexer } from './pancakeswap_v3_indexer.js';
import { startPancakeSwapInfinityIndexer } from './pancakeswap_infinity_indexer.js';
import { startUniswapV3BscIndexer } from './uniswap_v3_bsc_indexer.js';
import { startUniswapV4BaseIndexer } from './uniswap_v4_base_indexer.js';
import { startRobinhoodUniswapV4Indexer } from './uniswap_v4_robinhood_indexer.js';
import { startRobinhoodUniswapV3Indexer } from './uniswap_v3_robinhood_indexer.js';
import { startUniswapV3BaseIndexer } from './uniswap_v3_base_indexer.js';
import { startUniswapV2BaseIndexer } from './uniswap_v2_base_indexer.js';
import { startUniswapV4BscIndexer } from './uniswap_v4_bsc_indexer.js';
import { nextWebsocketEndpoint, readWebsocketEndpoints } from './evm_ws_rotation.js';
import { startUnifiedPriceEventMirror } from './unified_price_events.js';
import { fetchJson } from './http_json.js';
import { runTokenLogoWorker } from './token_logo_worker.js';

type SolanaAccountKey = { toBase58?: () => string; pubkey?: string } | string;
type SolanaTxLike = {
  meta?: any;
  transaction?: any;
  accountKeys?: SolanaAccountKey[];
};

const KNOWN_SYSTEM_ADDRESSES = new Set([
  '11111111111111111111111111111111',
  'Sysvar111111111111111111111111111111111',
  'SysvarC1ock11111111111111111111111111111111',
  'SysvarRent111111111111111111111111111111111',
  'SysvarInstructions111111111111111111111111111111',
  'SysvarEpochSchedu1e111111111111111111111111111',
  'ComputeBudget111111111111111111111111111111',
  'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
  'Memo1UhkJRfHyvLMcVucJwxXeuD7156h4a2p4',
  'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDL',
  '1111111527D8R6fVqaYVw5Mcg4LkPFP5YbV3A',
]);

const WRAPPED_SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL ?? 'https://api.mainnet-beta.solana.com';
const SOLANA_BLOCK_WS_URL = readWebsocketEndpoints('SOLANA_BLOCK', process.env.SOLANA_BLOCK_WS_URL ?? process.env.SOLANA_WS_URL ?? 'wss://api.mainnet-beta.solana.com');
const SOLANA_PRICE_WS_URL = readWebsocketEndpoints('SOLANA_PRICE', process.env.SOLANA_PRICE_WS_URL ?? process.env.SOLANA_WS_URL ?? 'wss://api.mainnet-beta.solana.com');
const SUPABASE_URL = process.env.SUPABASE_URL ?? 'http://127.0.0.1:54321';
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY ?? 'sb_publishable_ACJWlzQHlZjBrEguHvfOxg_3BJgxAaH';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? '';
const POSTGRES_URL = process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres';
const PUMPSWAP_PROGRAM_ID = process.env.PUMPSWAP_PROGRAM_ID ?? DEFAULT_PUMPSWAP_PROGRAM_ID;
const METAPLEX_RPC_URL = process.env.METAPLEX_RPC_URL ?? '';
const SHYFT_WS_URL = process.env.SHYFT_WS_URL ?? '';
const RAYDIUM_BLOCK_WS_URL = readWebsocketEndpoints('RAYDIUM_BLOCK', process.env.RAYDIUM_BLOCK_WS_URL ?? process.env.RAYDIUM_WS_URL ?? '');
const RAYDIUM_PRICE_WS_URL = readWebsocketEndpoints('RAYDIUM_PRICE', process.env.RAYDIUM_PRICE_WS_URL ?? process.env.RAYDIUM_WS_URL ?? '');
const METEORA_DLMM_BLOCK_WS_URL = readWebsocketEndpoints('METEORA_DLMM_BLOCK', process.env.METEORA_DLMM_BLOCK_WS_URL ?? process.env.METEORA_DLMM_WS_URL ?? '');
const METEORA_DAMM_V2_BLOCK_WS_URL = readWebsocketEndpoints('METEORA_DAMM_V2_BLOCK', process.env.METEORA_DAMM_V2_BLOCK_WS_URL ?? process.env.METEORA_DAMM_V2_WS_URL ?? '');
const ORCA_WHIRLPOOL_BLOCK_WS_URL = readWebsocketEndpoints('ORCA_WHIRLPOOL_BLOCK', process.env.ORCA_WHIRLPOOL_BLOCK_WS_URL ?? process.env.ORCA_WHIRLPOOL_WS_URL ?? '');
const METEORA_DLMM_PRICE_WS_URL = readWebsocketEndpoints('METEORA_DLMM_PRICE', process.env.METEORA_DLMM_PRICE_WS_URL ?? process.env.METEORA_DLMM_BLOCK_WS_URL ?? process.env.METEORA_DLMM_WS_URL ?? '');
const METEORA_DAMM_V2_PRICE_WS_URL = readWebsocketEndpoints('METEORA_DAMM_V2_PRICE', process.env.METEORA_DAMM_V2_PRICE_WS_URL ?? process.env.METEORA_DAMM_V2_BLOCK_WS_URL ?? process.env.METEORA_DAMM_V2_WS_URL ?? '');
const ORCA_WHIRLPOOL_PRICE_WS_URL = readWebsocketEndpoints('ORCA_WHIRLPOOL_PRICE', process.env.ORCA_WHIRLPOOL_PRICE_WS_URL ?? process.env.ORCA_WHIRLPOOL_BLOCK_WS_URL ?? process.env.ORCA_WHIRLPOOL_WS_URL ?? '');
const PANCAKESWAP_V2_WS_URL = readWebsocketEndpoints('PANCAKESWAP_V2', process.env.PANCAKESWAP_V2_WS_URL ?? '');
const PANCAKESWAP_V3_WS_URL = readWebsocketEndpoints('PANCAKESWAP_V3', process.env.PANCAKESWAP_V3_WS_URL ?? '');
const PANCAKESWAP_INFINITY_WS_URL = readWebsocketEndpoints('PANCAKESWAP_INFINITY', process.env.PANCAKESWAP_INFINITY_WS_URL ?? '');
const UNISWAP_V3_BSC_WS_URL = readWebsocketEndpoints('UNISWAP_V3_BSC', process.env.UNISWAP_V3_BSC_WS_URL ?? '');
const UNISWAP_V4_BASE_WS_URL = readWebsocketEndpoints('UNISWAP_V4_BASE', process.env.UNISWAP_V4_BASE_WS_URL ?? '');
const ROBINHOOD_V4_WS_URL = readWebsocketEndpoints('ROBINHOOD_V4', process.env.ROBINHOOD_V4_WS_URL ?? '');
const ROBINHOOD_V3_WS_URL = readWebsocketEndpoints('ROBINHOOD_V3', process.env.ROBINHOOD_V3_WS_URL ?? process.env.ROBINHOOD_V4_WS_URL ?? '');
const UNISWAP_V3_BASE_WS_URL = readWebsocketEndpoints('UNISWAP_V3_BASE', process.env.UNISWAP_V3_BASE_WS_URL ?? '');
const UNISWAP_V2_BASE_WS_URL = readWebsocketEndpoints('UNISWAP_V2_BASE', process.env.UNISWAP_V2_BASE_WS_URL ?? '');
const UNISWAP_V4_BSC_WS_URL = readWebsocketEndpoints('UNISWAP_V4_BSC', process.env.UNISWAP_V4_BSC_WS_URL ?? '');
const ENABLE_PRICE_FETCHING = process.env.ENABLE_PRICE_FETCHING === 'true';
const PROMOTION_FAILURE_FILE = process.env.PROMOTION_FAILURE_FILE ?? 'promotion-failures.jsonl';
const PROMOTION_SWAP_THRESHOLD = Number(process.env.PROMOTION_SWAP_THRESHOLD ?? 1000);
const PROMOTION_WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const PROMOTION_MIN_UNIQUE_WALLETS = Number(process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 50);
const PROMOTION_STALE_MS = Number(process.env.PROMOTION_STALE_MS ?? 60 * 1000);
const HEALTH_LOG_INTERVAL_MS = Number(process.env.HEALTH_LOG_INTERVAL_MS ?? 60_000);

const tracker = new PoolTracker({
  windowMs: PROMOTION_WINDOW_MS,
  minThreshold: PROMOTION_SWAP_THRESHOLD,
  minUniqueWallets: PROMOTION_MIN_UNIQUE_WALLETS,
  staleMs: PROMOTION_STALE_MS,
});
const poolsBeingPromoted = new Set<string>();
let registerRaydiumPricePool: RegisterRaydiumPricePool = () => undefined;
let pumpswapArrivalCount = 0;
let pumpswapDecodeMissCount = 0;

async function recordPromotionFailure(poolAddress: string, error: unknown) {
  const failure = {
    timestamp: new Date().toISOString(),
    poolAddress,
    error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : String(error),
  };

  try {
    await appendFile(PROMOTION_FAILURE_FILE, `${JSON.stringify(failure)}\n`, 'utf8');
  } catch (fileError) {
    console.error('Failed to write promotion failure record', fileError);
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
});
const pgPool = new Pool({ connectionString: POSTGRES_URL, connectionTimeoutMillis: 10000, statement_timeout: 15000, idleTimeoutMillis: 30000, max: Number(process.env.POSTGRES_POOL_MAX ?? 20) });

function consoleBanner() {
  console.log('=== PumpSwap Trending Indexer (MVP) ===');
  console.log(`RPC: ${SOLANA_RPC_URL}`);
  console.log(`Solana block WSS: ${SOLANA_BLOCK_WS_URL.join(', ') || 'not configured'}`);
  console.log(`Solana price WSS: ${SOLANA_PRICE_WS_URL.join(', ') || 'not configured'}`);
  console.log(`Supabase URL: ${SUPABASE_URL}`);
  console.log(`Promotion threshold: ${PROMOTION_SWAP_THRESHOLD} swaps in ${PROMOTION_WINDOW_MS / 1000}s`);
  console.log(`Min unique wallets: ${PROMOTION_MIN_UNIQUE_WALLETS}`);
  console.log(`Pumpswap program ID: ${PUMPSWAP_PROGRAM_ID || 'not configured'}`);
}

function debugLog(message: string) {
  console.log(`[indexer] ${message}`);
}

function isKnownSystemOrProgramAddress(address: string): boolean {
  if (!address) return true;
  if (KNOWN_SYSTEM_ADDRESSES.has(address)) return true;
  return address.startsWith('111111') || address.startsWith('Sysvar') || address.startsWith('Token') || address.startsWith('Memo');
}

function decodeInstructionData(data: unknown): Buffer | null {
  if (!data) {
    return null;
  }

  if (Buffer.isBuffer(data)) {
    return data;
  }

  try {
    if (typeof data === 'string') {
      return Buffer.from(bs58.decode(data));
    }
  } catch {
    return null;
  }

  try {
    const uint8Array = Uint8Array.from(data as ArrayLike<number>);
    return Buffer.from(uint8Array);
  } catch {
    return null;
  }
}

function getInstructionAccountKeys(instruction: any, accountKeys: string[]): string[] {
  const indices = Array.isArray(instruction?.accounts) ? instruction.accounts : [];
  return indices
    .map((account: number | string) => typeof account === 'number' ? accountKeys[account] : account)
    .filter((address: string | undefined): address is string => Boolean(address));
}

function normalizeAccountAddress(value: SolanaAccountKey | undefined): string {
  if (!value) return '';
  if (typeof value === 'string') return value;
  return value.pubkey ?? value.toBase58?.() ?? '';
}

function extractAccountKeys(tx: SolanaTxLike): string[] {
  const accountKeys: SolanaAccountKey[] = tx?.transaction?.message?.accountKeys ?? [];
  return accountKeys
    .map((key: SolanaAccountKey) => normalizeAccountAddress(key))
    .filter((address: string) => Boolean(address) && !isKnownSystemOrProgramAddress(address));
}

function detectPoolAddress(tx: SolanaTxLike): string | null {
  return getPumpAmmSwapMeta(tx, PUMPSWAP_PROGRAM_ID)?.poolAddress ?? null;
}

function detectWallet(tx: SolanaTxLike): string | null {
  return getPumpAmmSwapMeta(tx, PUMPSWAP_PROGRAM_ID)?.wallet ?? null;
}

function detectSwapMints(tx: SolanaTxLike): {
  baseMint: string;
  quoteMint: string;
  poolBaseTokenAccount: string;
  poolQuoteTokenAccount: string;
} | null {
  const meta = getPumpAmmSwapMeta(tx, PUMPSWAP_PROGRAM_ID);
  return meta ? {
    baseMint: meta.baseMint,
    quoteMint: meta.quoteMint,
    poolBaseTokenAccount: meta.poolBaseTokenAccount,
    poolQuoteTokenAccount: meta.poolQuoteTokenAccount,
  } : null;
}

function isSupportedPromotionPair(baseMint: string, quoteMint: string): boolean {
  return baseMint === WRAPPED_SOL_MINT || quoteMint === WRAPPED_SOL_MINT || baseMint === USDC_MINT || quoteMint === USDC_MINT;
}

type MintMetadata = {
  address: string;
  name: string | null;
  symbol: string | null;
  logo: string | null;
  decimals: number | null;
};

const knownMintMetadata = new Map<string, MintMetadata>([
  [WRAPPED_SOL_MINT, {
    address: WRAPPED_SOL_MINT,
    name: 'Wrapped SOL',
    symbol: 'SOL',
    logo: 'https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/solana/info/logo.png',
    decimals: 9,
  }],
  [USDC_MINT, {
    address: USDC_MINT,
    name: 'USD Coin',
    symbol: 'USDC',
    logo: 'https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/solana/assets/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png',
    decimals: 6,
  }],
]);

function extractMintMetadata(asset: any): MintMetadata | null {
  const address = typeof asset?.id === 'string' ? asset.id : '';
  if (!address) return null;

  const content = asset?.content ?? {};
  const metadata = content?.metadata ?? {};
  const links = content?.links ?? {};
  const file = Array.isArray(content?.files)
    ? content.files.find((entry: any) => typeof entry?.uri === 'string' && entry.type?.startsWith('image/'))
    : null;

  return {
    address,
    name: typeof metadata.name === 'string' && metadata.name.trim() ? metadata.name.trim() : null,
    symbol: typeof metadata.symbol === 'string' && metadata.symbol.trim() ? metadata.symbol.trim() : null,
    logo: typeof links.image === 'string' ? links.image : typeof file?.uri === 'string' ? file.uri : null,
    decimals: typeof asset?.token_info?.decimals === 'number' ? asset.token_info.decimals : null,
  };
}

async function fetchMintMetadata(mintAddresses: string[]): Promise<Map<string, MintMetadata>> {
  const result = new Map<string, MintMetadata>();
  const addresses = [...new Set(mintAddresses)].filter((address) => Boolean(address) && !knownMintMetadata.has(address));
  for (const address of mintAddresses) {
    const metadata = knownMintMetadata.get(address);
    if (metadata) result.set(address, metadata);
  }
  if (!METAPLEX_RPC_URL || addresses.length === 0) return result;

  try {
    const body = await fetchJson(METAPLEX_RPC_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(addresses.map((address, index) => ({
        jsonrpc: '2.0',
        id: `pumpswap-mint-metadata-${index}`,
        method: 'getAsset',
        params: { id: address },
      }))),
    })
      
    for (const item of body) {
      const asset = item.result;
      const metadata = extractMintMetadata(asset);
      if (metadata) result.set(metadata.address, metadata);
    }
  } catch (error) {
    console.warn('Metaplex metadata enrichment failed; promoting with mint addresses:', error);
  }

  return result;
}

function estimateVolumeFromTx(tx: SolanaTxLike): number {
  const meta = tx.meta;
  if (!meta) return 0;

  const preBalances = meta.preTokenBalances ?? [];
  const postBalances = meta.postTokenBalances ?? [];

  let result = 0;
  for (let i = 0; i < Math.max(preBalances.length, postBalances.length); i += 1) {
    const before = preBalances[i]?.uiTokenAmount?.uiAmount ?? 0;
    const after = postBalances[i]?.uiTokenAmount?.uiAmount ?? 0;
    result += Math.abs(after - before);
  }

  return result;
}

async function promotePool(poolAddress: string) {
  const pool = tracker.getPool(poolAddress);
  if (!pool || pool.promoted || poolsBeingPromoted.has(poolAddress)) {
    return;
  }

  poolsBeingPromoted.add(poolAddress);

  try {
    const globalNow = Date.now();
    const x = pool.events.filter((event) => event.timestamp >= globalNow - PROMOTION_WINDOW_MS);
    const uniqueWallets = new Set(x.map((event) => event.wallet));
    const totalVolume = x.reduce((sum, event) => sum + event.volume, 0);
    const baseMint = pool.baseMint ?? 'unknown';
    const quoteMint = pool.quoteMint ?? 'unknown';
    const metadata = await fetchMintMetadata([baseMint, quoteMint]);
    const baseMetadata = metadata.get(baseMint);
    const quoteMetadata = metadata.get(quoteMint);

    const payload = {
    address: poolAddress,
    pool_type: 'pumpswap',
    program_id: PUMPSWAP_PROGRAM_ID || 'unknown',
    network: 'solana',
    base_mint: baseMint,
    base_name: baseMetadata?.name ?? null,
    quote_mint: quoteMint,
    quote_name: quoteMetadata?.name ?? null,
    base_symbol: baseMetadata?.symbol ?? null,
    quote_symbol: quoteMetadata?.symbol ?? null,
    base_decimals: baseMetadata?.decimals ?? 9,
    quote_decimals: quoteMetadata?.decimals ?? 9,
    base_logo_url: baseMetadata?.logo ?? null,
    quote_logo_url: quoteMetadata?.logo ?? null,
    lp_mint: 'unknown',
    pool_base_token_account: pool.poolBaseTokenAccount ?? 'unknown',
    pool_quote_token_account: pool.poolQuoteTokenAccount ?? 'unknown',
    creator: 'unknown',
    coin_creator: 'unknown',
    pool_index: 0,
    updated_slot: 0,
    discovered_at: new Date(pool.createdAt).toISOString(),
    indexed_at: new Date().toISOString(),
    total_swaps_15m: x.length,
    unique_wallets_15m: uniqueWallets.size,
    total_volume_15m: totalVolume,
    };

    await pgPool.query(
      `
        INSERT INTO pools (
          address, pool_type, program_id, network, base_mint, base_name, base_symbol, base_decimals,
          base_logo_url, quote_mint, quote_name, quote_symbol, quote_decimals, quote_logo_url,
          lp_mint, pool_base_token_account, pool_quote_token_account, creator, coin_creator,
          pool_index, updated_slot, discovered_at, indexed_at
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
          $18, $19, $20, $21, $22, NOW()
        )
        ON CONFLICT (address) DO UPDATE SET
          pool_type = EXCLUDED.pool_type,
          program_id = EXCLUDED.program_id,
          network = EXCLUDED.network,
          base_mint = EXCLUDED.base_mint,
          base_name = EXCLUDED.base_name,
          base_symbol = EXCLUDED.base_symbol,
          base_decimals = EXCLUDED.base_decimals,
          base_logo_url = EXCLUDED.base_logo_url,
          quote_mint = EXCLUDED.quote_mint,
          quote_name = EXCLUDED.quote_name,
          quote_symbol = EXCLUDED.quote_symbol,
          quote_decimals = EXCLUDED.quote_decimals,
          quote_logo_url = EXCLUDED.quote_logo_url,
          lp_mint = EXCLUDED.lp_mint,
          pool_base_token_account = EXCLUDED.pool_base_token_account,
          pool_quote_token_account = EXCLUDED.pool_quote_token_account,
          creator = EXCLUDED.creator,
          coin_creator = EXCLUDED.coin_creator,
          pool_index = EXCLUDED.pool_index,
          updated_slot = EXCLUDED.updated_slot,
          discovered_at = EXCLUDED.discovered_at,
          indexed_at = NOW()
      `,
      [
        poolAddress,
        payload.pool_type,
        payload.program_id,
        payload.network,
        payload.base_mint,
        payload.base_name,
        payload.base_symbol,
        payload.base_decimals,
        payload.base_logo_url,
        payload.quote_mint,
        payload.quote_name,
        payload.quote_symbol,
        payload.quote_decimals,
        payload.quote_logo_url,
        payload.lp_mint,
        payload.pool_base_token_account,
        payload.pool_quote_token_account,
        payload.creator,
        payload.coin_creator,
        payload.pool_index,
        payload.updated_slot,
        payload.discovered_at,
      ],
    );

    tracker.markPromoted(poolAddress, Date.now());
    registerPumpswapPrice({
      poolAddress,
      baseMint,
      quoteMint,
      baseDecimals: payload.base_decimals,
      quoteDecimals: payload.quote_decimals,
      baseVault: payload.pool_base_token_account,
      quoteVault: payload.pool_quote_token_account,
    });

    console.log(`Promoted pool to DB: ${poolAddress} | swaps=${x.length} | wallets=${uniqueWallets.size}`);
  } catch (error) {
    console.error('Failed to promote pool', poolAddress, error);
    await recordPromotionFailure(poolAddress, error);
  } finally {
    poolsBeingPromoted.delete(poolAddress);
  }
}

async function handleTransaction(signature: string, tx: SolanaTxLike | null, source = 'direct') {
  if (!tx) {
    return;
  }

  if (!tx.meta || tx.meta.err) {
    return;
  }

  const poolAddress = detectPoolAddress(tx);
  const wallet = detectWallet(tx);
  const mints = detectSwapMints(tx);

  if (!poolAddress || !wallet || !mints) {
    return;
  }

  if (!isSupportedPromotionPair(mints.baseMint, mints.quoteMint)) {
    return;
  }

  const volume = estimateVolumeFromTx(tx);
  const poolState = tracker.addSwap(poolAddress, wallet, volume, Date.now(), mints);

  if (tracker.shouldPromote(poolAddress)) {
    await promotePool(poolAddress);
  }

  if (poolState.events.length > 0) {
    debugLog(`[${source}] Tracked pool ${poolAddress} | swaps=${poolState.events.length} | walletCount=${new Set(poolState.events.map((event) => event.wallet)).size}`);
  }
}

const processRaydiumTransaction = createRaydiumCpmmProcessor(pgPool, (pool) => registerRaydiumPricePool(pool));
const { registerPricePool: registerPumpswapPrice, processPumpswapPrices } = createPumpswapPriceFetcher(pgPool);

async function subscribeToPumpSwap() {
  if (!PUMPSWAP_PROGRAM_ID) {
    console.warn('PUMPSWAP_PROGRAM_ID is not set. Add it to your .env file before starting the indexer.');
    return;
  }

  const programId = new PublicKey(PUMPSWAP_PROGRAM_ID);
  const programAddress = programId.toBase58();
  console.log(`Listening for PumpSwap block stream on program ${programAddress}`);

  let reconnectDelayMs = 1000;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let endpointIndex = 0;
  const blockEndpoints = SOLANA_BLOCK_WS_URL.length > 0 ? SOLANA_BLOCK_WS_URL : [SOLANA_RPC_URL.replace(/^https:/, 'wss:')];

  const connect = () => {
    const endpoint = blockEndpoints[endpointIndex] ?? blockEndpoints[0] ?? '';
    if (!endpoint) {
      console.warn('PumpSwap websocket is not configured.');
      return;
    }

    const socket = new WebSocket(endpoint);

    socket.on('open', () => {
      reconnectDelayMs = 1000;
      const subscribeMessage = JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'blockSubscribe',
        params: [
          { mentionsAccountOrProgram: programAddress },
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed',
            transactionDetails: 'full',
            maxSupportedTransactionVersion: 0,
          },
        ],
      });

      socket.send(subscribeMessage);
      console.log('PumpSwap block subscription started.');
    });

    socket.on('message', async (raw) => {
      let payload: any;
      try {
        payload = JSON.parse(raw.toString());
      } catch (error) {
        console.warn('Ignoring invalid PumpSwap websocket payload:', error);
        return;
      }

      if (payload?.method !== 'blockNotification') {
        return;
      }

      const blockValue = payload?.params?.result?.value ?? null;
      if (!blockValue || !blockValue.block) {
        if (blockValue?.err) {
          console.log('block stream yielded unsupported block payload:', JSON.stringify(blockValue.err));
        }
        return;
      }

      const block = blockValue.block;
      const transactions = Array.isArray(block.transactions) ? block.transactions : [];

      for (const tx of transactions) {
        const txSignature = tx?.transaction?.signatures?.[0] ?? '';
        const slot = Number(blockValue.context?.slot ?? 0);
        const accountKeys = tx?.transaction?.message?.accountKeys ?? [];
        const accountPubkeys = accountKeys.map((key: SolanaAccountKey) => normalizeAccountAddress(key));

        if (!accountPubkeys.includes(programAddress)) {
          continue;
        }

        const swapMeta = getPumpAmmSwapMeta(tx, programAddress);
        if (!swapMeta) {
          continue;
        }

        if (!txSignature) {
          continue;
        }

        await handleTransaction(txSignature, tx);
        if (ENABLE_PRICE_FETCHING) await processPumpswapPrices(tx, slot);
      }
    });

    socket.on('error', (error) => {
      console.error('PumpSwap websocket error:', error);
    });

    socket.on('close', (code, reason) => {
      const next = nextWebsocketEndpoint(blockEndpoints, endpointIndex);
      endpointIndex = next.index;
      console.warn(`PumpSwap websocket closed: ${code} ${reason.toString()}`);
      if (!reconnectTimer) {
        console.log(`Reconnecting PumpSwap websocket in ${reconnectDelayMs / 1000}s.`);
        reconnectTimer = setTimeout(() => {
          reconnectTimer = undefined;
          connect();
        }, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
      }
    });
  };

  connect();
}

async function main() {
  tracker.reset();
  poolsBeingPromoted.clear();
  consoleBanner();
  console.log('Fresh indexer session: in-memory swap counts reset.');
  const sameEndpointSet = (a: string[], b: string[]) => a.length === b.length && a.every((value, index) => value === b[index]);
  if (!sameEndpointSet(METEORA_DLMM_PRICE_WS_URL, METEORA_DLMM_BLOCK_WS_URL) || !sameEndpointSet(METEORA_DAMM_V2_PRICE_WS_URL, METEORA_DAMM_V2_BLOCK_WS_URL) || !sameEndpointSet(ORCA_WHIRLPOOL_PRICE_WS_URL, ORCA_WHIRLPOOL_BLOCK_WS_URL)) {
    console.warn('Solana Meteora/Orca price endpoints are configured separately, but those indexers currently multiplex blockSubscribe and accountSubscribe on one socket; price endpoints remain reserved until dual-socket mode is enabled.');
  }
  await startUnifiedPriceEventMirror();

  try {
    if (SUPABASE_URL) {
      const { data, error } = await supabase.from('pools').select('address').limit(1);
      if (error) {
        console.warn('Supabase health check failed, but the indexer will continue:', error.message);
      } else {
        console.log('Supabase connection is responding.');
      }
    }
  } catch (error) {
    console.warn('Supabase warmup failed:', error);
  }

  if (ENABLE_PRICE_FETCHING) {
    registerRaydiumPricePool = await startRaydiumPriceFetcher(pgPool, RAYDIUM_PRICE_WS_URL);
  }
  await Promise.all([
    subscribeToPumpSwap(),
    startRaydiumCpmmIndexer(pgPool, RAYDIUM_BLOCK_WS_URL, (pool) => registerRaydiumPricePool(pool)),
    startRaydiumClmmIndexer(pgPool, RAYDIUM_BLOCK_WS_URL, (pool) => registerRaydiumPricePool(pool)),
    startMeteoraDlmmIndexer(pgPool, METEORA_DLMM_BLOCK_WS_URL, undefined, METEORA_DLMM_PRICE_WS_URL),
    startMeteoraDammV2Indexer(pgPool, METEORA_DAMM_V2_BLOCK_WS_URL, undefined, METEORA_DAMM_V2_PRICE_WS_URL),
    startOrcaWhirlpoolIndexer(pgPool, ORCA_WHIRLPOOL_BLOCK_WS_URL, undefined, ORCA_WHIRLPOOL_PRICE_WS_URL),
    startPancakeSwapV2Indexer(pgPool, PANCAKESWAP_V2_WS_URL),
    startPancakeSwapV3Indexer(pgPool, PANCAKESWAP_V3_WS_URL),
    startPancakeSwapInfinityIndexer(pgPool, PANCAKESWAP_INFINITY_WS_URL),
    startUniswapV3BscIndexer(pgPool, UNISWAP_V3_BSC_WS_URL),
    startUniswapV4BaseIndexer(pgPool, UNISWAP_V4_BASE_WS_URL),
    startRobinhoodUniswapV4Indexer(pgPool, ROBINHOOD_V4_WS_URL),
    startRobinhoodUniswapV3Indexer(pgPool, ROBINHOOD_V3_WS_URL),
    startUniswapV3BaseIndexer(pgPool, UNISWAP_V3_BASE_WS_URL),
    startUniswapV2BaseIndexer(pgPool, UNISWAP_V2_BASE_WS_URL),
    startUniswapV4BscIndexer(pgPool, UNISWAP_V4_BSC_WS_URL),
  ]);

  void runTokenLogoWorker(false, pgPool).catch((error) => {
    console.error('[indexer] token logo worker stopped unexpectedly:', error);
  });

  setInterval(() => {
    const candidates = tracker.listCandidates();
    if (candidates.length > 0) {
      console.log(`live candidates in memory: ${candidates.length}`);
    }

    for (const candidate of candidates) {
      if (tracker.shouldPromote(candidate.poolAddress)) {
        void promotePool(candidate.poolAddress);
      }
    }
  }, 30000);
  setInterval(() => {
    console.log(`[health] candidates=${tracker.listCandidates().length} pumpswapArrivals=${pumpswapArrivalCount} decodeMisses=${pumpswapDecodeMissCount} promotedInFlight=${poolsBeingPromoted.size}`);
  }, HEALTH_LOG_INTERVAL_MS);
}

main().catch((error) => {
  console.error('Indexer startup failed:', error);
  process.exit(1);
});
