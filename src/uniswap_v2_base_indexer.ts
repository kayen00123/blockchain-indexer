import 'dotenv/config';
import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import WebSocket from 'ws';
import { nextWebsocketEndpoint, normalizeWebsocketEndpoints, type WebsocketEndpointInput } from './evm_ws_rotation.js';
import { getReferencePrice } from './reference_prices.js';

export const UNISWAP_V2_BASE_FACTORY = (process.env.UNISWAP_V2_BASE_FACTORY ?? '0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6').toLowerCase();
const SWAP_TOPIC = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822';
const SYNC_TOPIC = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1';
const WETH = (process.env.UNISWAP_V2_BASE_WETH ?? '0x4200000000000000000000000000000000000006').toLowerCase();
const USDC = (process.env.UNISWAP_V2_BASE_USDC ?? '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913').toLowerCase();
const ZERO = '0x0000000000000000000000000000000000000000';
const SWAP_THRESHOLD = Number(process.env.UNISWAP_V2_BASE_SWAP_THRESHOLD ?? process.env.EVM_PROMOTION_SWAP_THRESHOLD ?? process.env.PROMOTION_SWAP_THRESHOLD ?? 100);
const WALLET_THRESHOLD = Number(process.env.UNISWAP_V2_BASE_MIN_UNIQUE_WALLETS ?? process.env.EVM_PROMOTION_MIN_UNIQUE_WALLETS ?? process.env.PROMOTION_MIN_UNIQUE_WALLETS ?? 20);
const WINDOW_MS = Number(process.env.PROMOTION_WINDOW_MS ?? 15 * 60 * 1000);
const PRICE_FILE = process.env.UNISWAP_V2_BASE_PRICE_EVENT_FILE ?? 'uniswap-v2-base-price-events.jsonl';
const FAILURE_FILE = process.env.UNISWAP_V2_BASE_FAILURE_FILE ?? 'uniswap-v2-base-failures.jsonl';

type Pair = { address: string; token0: string; token1: string; symbol0: string; symbol1: string; decimals0: number; decimals1: number };
type Activity = { pair: Pair; wallets: Map<string, number>; swapTimestamps: number[] };
const word = (data: string, index: number) => BigInt(`0x${data.replace(/^0x/, '').slice(index * 64, (index + 1) * 64)}`);
const addressWord = (value: unknown) => { const raw = String(value ?? ''); return /^0x[0-9a-f]{64}$/i.test(raw) ? `0x${raw.slice(-40)}`.toLowerCase() : ZERO; };
const decodeText = (value: unknown) => { try { const data = String(value); const offset = Number(word(data, 0)); const length = Number(word(data, offset / 32)); return Buffer.from(data.slice(2 + (offset + 32) * 2, 2 + (offset + 32 + length) * 2), 'hex').toString('utf8').replace(/\0/g, ''); } catch { return ''; } };
async function failure(address: string, error: unknown) { try { await appendFile(FAILURE_FILE, `${JSON.stringify({ timestamp: new Date().toISOString(), poolAddress: address, error: String(error) })}\n`); } catch { /* best effort */ } }

export async function startUniswapV2BaseIndexer(pgPool: PgPool, websocketUrl: WebsocketEndpointInput, priceFile = PRICE_FILE): Promise<void> {
  const endpoints = normalizeWebsocketEndpoints(websocketUrl);
  if (endpoints.length === 0) return;
  const pairs = new Map<string, Pair>();
  const activities = new Map<string, Activity>();
  const promoted = new Set<string>();
  const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();
  let socket: WebSocket | undefined;
  let requestId = 1;
  let endpointIndex = 0;
  let wethUsd: number | null = null;
  const rpc = (method: string, params: unknown[]) => new Promise<any>((resolve, reject) => { if (!socket || socket.readyState !== WebSocket.OPEN) return reject(new Error('Base websocket is not open')); const id = requestId++; pending.set(id, { resolve, reject }); socket.send(JSON.stringify({ jsonrpc: '2.0', id, method, params })); });
  const metadata = async (token: string) => { const [symbol, decimals] = await Promise.all([rpc('eth_call', [{ to: token, data: '0x95d89b41' }, 'latest']), rpc('eth_call', [{ to: token, data: '0x313ce567' }, 'latest'])]); return { symbol: decodeText(symbol) || token.slice(0, 8), decimals: Number(word(String(decimals), 0)) }; };
  const resolvePair = async (address: string): Promise<Pair | null> => { try { const [token0Result, token1Result] = await Promise.all([rpc('eth_call', [{ to: address, data: '0x0dfe1681' }, 'latest']), rpc('eth_call', [{ to: address, data: '0xd21220a7' }, 'latest'])]); const token0 = addressWord(token0Result); const token1 = addressWord(token1Result); if (token0 === ZERO || token1 === ZERO) return null; const [meta0, meta1] = await Promise.all([metadata(token0), metadata(token1)]); return { address, token0, token1, symbol0: meta0.symbol, decimals0: meta0.decimals, symbol1: meta1.symbol, decimals1: meta1.decimals }; } catch (error) { await failure(address, error); return null; } };
  let queue = Promise.resolve(); const record = (event: Record<string, unknown>) => { queue = queue.then(() => appendFile(priceFile, `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`)); return queue; };
  const processSwap = async (log: any) => { const address = String(log.address ?? '').toLowerCase(); const pair = pairs.get(address) ?? await resolvePair(address); if (!pair || ![pair.token0, pair.token1].some((token) => token === WETH || token === USDC)) return; pairs.set(address, pair); let activity = activities.get(address); if (!activity) { activity = { pair, wallets: new Map(), swapTimestamps: [] }; activities.set(address, activity); console.log(`[uniswap-v2-base] Pool discovered from Swap ${address} ${pair.symbol0}/${pair.symbol1}`); } const now = Date.now(); const windowStart = now - WINDOW_MS; activity.swapTimestamps = activity.swapTimestamps.filter((timestamp) => timestamp >= windowStart); for (const [wallet, timestamp] of activity.wallets) if (timestamp < windowStart) activity.wallets.delete(wallet); activity.swapTimestamps.push(now); activity.wallets.set(addressWord(log.topics?.[1]), now); const swaps = activity.swapTimestamps.length; const wallets = activity.wallets.size; console.log(`[uniswap-v2-base][websocket] Tracked pool ${address} | swaps=${swaps} | walletCount=${wallets}`); if (promoted.has(address) || swaps < SWAP_THRESHOLD || wallets < WALLET_THRESHOLD) return; promoted.add(address); try { await pgPool.query(`INSERT INTO base_uniswap_v2_pools (address,pool_type,chain,factory,token0,token0_symbol,token0_decimals,token1,token1_symbol,token1_decimals,transaction_hash,block_number,discovered_at,indexed_at) VALUES ($1,'uniswap_v2','base',$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW()) ON CONFLICT (address) DO UPDATE SET indexed_at=NOW(),block_number=EXCLUDED.block_number`, [address, UNISWAP_V2_BASE_FACTORY, pair.token0, pair.symbol0, pair.decimals0, pair.token1, pair.symbol1, pair.decimals1, log.transactionHash ?? '', Number.parseInt(log.blockNumber ?? '0x0', 16)]); console.log(`[uniswap-v2-base] Promoted ${address} ${pair.symbol0}/${pair.symbol1} | swaps=${swaps} | wallets=${wallets}`); } catch (error) { promoted.delete(address); await failure(address, error); } };
  const processSync = async (log: any) => { const address = String(log.address ?? '').toLowerCase(); const pair = pairs.get(address); if (!pair) return; const reserve0 = word(log.data, 0); const reserve1 = word(log.data, 1); const raw0 = Number(reserve0) / 10 ** pair.decimals0; const raw1 = Number(reserve1) / 10 ** pair.decimals1; const price = raw1 / raw0; if (!Number.isFinite(price) || price <= 0) return; const inverse = 1 / price; const tokenPriceUsd = pair.token1 === USDC ? price : pair.token0 === USDC ? inverse : pair.token1 === WETH && wethUsd ? price * wethUsd : pair.token0 === WETH && wethUsd ? wethUsd / price : null; await record({ type: 'price', poolType: 'uniswap_v2_base', poolAddress: address, pair: `${pair.symbol0}/${pair.symbol1}`, price, inversePrice: inverse, tokenPriceUsd, reserve0: raw0, reserve1: raw1, block: Number.parseInt(log.blockNumber ?? '0x0', 16) }); };
  const connect = () => { const endpoint = endpoints[endpointIndex]; socket = new WebSocket(endpoint); socket.on('error', (error) => console.error('[uniswap-v2-base] websocket error:', error)); socket.on('open', () => { console.log(`[uniswap-v2-base] swap-only monitoring on ${endpoint}`); void rpc('eth_subscribe', ['logs', { topics: [SWAP_TOPIC] }]).then((id) => console.log(`[uniswap-v2-base] global Swap subscription active id=${id}`)).catch((error) => console.error('[uniswap-v2-base] global Swap subscription failed:', error)); }); socket.on('message', (raw) => { try { const payload = JSON.parse(raw.toString()); if (payload.id !== undefined && pending.has(Number(payload.id))) { const request = pending.get(Number(payload.id))!; pending.delete(Number(payload.id)); if (payload.error) request.reject(new Error(JSON.stringify(payload.error))); else request.resolve(payload.result); return; } const result = payload.params?.result; if (payload.method === 'eth_subscription' && result && result.topics?.[0]?.toLowerCase() === SWAP_TOPIC) void processSwap(result); } catch (error) { console.error('[uniswap-v2-base] message error:', error); } }); socket.on('close', () => { const next = nextWebsocketEndpoint(endpoints, endpointIndex); endpointIndex = next.index; setTimeout(connect, 1000); }); };
  const refreshWeth = async () => { try { wethUsd = await getReferencePrice('weth'); } catch { /* retain last value */ } };
  await refreshWeth(); setInterval(() => void refreshWeth(), 180000); connect();
}
