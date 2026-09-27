import 'dotenv/config';
import { Pool } from 'pg';

type Network = 'solana' | 'bsc' | 'base' | 'robinhood';
type Source = { table: string; network: Network; tokenColumn: string; logoColumn: string };
type Logo = { address: string; url: string; source: string };

const postgresUrl = process.env.POSTGRES_URL || 'postgresql://postgres:postgres@127.0.0.1:54322/postgres';
const intervalMs = Number(process.env.TOKEN_LOGO_WORKER_INTERVAL_MS ?? 15 * 60 * 1000);
const delayMs = Number(process.env.TOKEN_LOGO_WORKER_REQUEST_DELAY_MS ?? 250);
const batchSize = Math.max(1, Number(process.env.TOKEN_LOGO_WORKER_BATCH_SIZE ?? 50));
const retryDelayMs = Number(process.env.TOKEN_LOGO_WORKER_RETRY_DELAY_MS ?? 6 * 60 * 60 * 1000);
const requestTimeoutMs = Number(process.env.TOKEN_LOGO_WORKER_REQUEST_TIMEOUT_MS ?? 10000);
const concurrency = Math.max(1, Number(process.env.TOKEN_LOGO_WORKER_CONCURRENCY ?? 5));
const dexscreenerNetworks: Record<Exclude<Network, 'solana'>, string> = {
  bsc: process.env.TOKEN_LOGO_DEXSCREENER_BSC_NETWORK ?? 'bsc',
  base: process.env.TOKEN_LOGO_DEXSCREENER_BASE_NETWORK ?? 'base',
  robinhood: process.env.TOKEN_LOGO_DEXSCREENER_ROBINHOOD_NETWORK ?? 'robinhood',
};

const sources: Source[] = [
  { table: 'pools', network: 'solana', tokenColumn: 'base_mint', logoColumn: 'base_logo_url' },
  { table: 'pools', network: 'solana', tokenColumn: 'quote_mint', logoColumn: 'quote_logo_url' },
  { table: 'raydium_pools', network: 'solana', tokenColumn: 'token_mint_0', logoColumn: 'token_mint_0_logo_url' },
  { table: 'raydium_pools', network: 'solana', tokenColumn: 'token_mint_1', logoColumn: 'token_mint_1_logo_url' },
  { table: 'raydium_cpmm_pools', network: 'solana', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'raydium_cpmm_pools', network: 'solana', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'meteora_damm_v2_pools', network: 'solana', tokenColumn: 'token_a_mint', logoColumn: 'token_a_logo_url' },
  { table: 'meteora_damm_v2_pools', network: 'solana', tokenColumn: 'token_b_mint', logoColumn: 'token_b_logo_url' },
  { table: 'meteora_dlmm_pools', network: 'solana', tokenColumn: 'token_x_mint', logoColumn: 'token_x_logo_url' },
  { table: 'meteora_dlmm_pools', network: 'solana', tokenColumn: 'token_y_mint', logoColumn: 'token_y_logo_url' },
  { table: 'orca_whirlpools', network: 'solana', tokenColumn: 'token_mint_a', logoColumn: 'token_mint_a_logo_url' },
  { table: 'orca_whirlpools', network: 'solana', tokenColumn: 'token_mint_b', logoColumn: 'token_mint_b_logo_url' },
  { table: 'bsc_pancakeswap_v2_pools', network: 'bsc', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'bsc_pancakeswap_v2_pools', network: 'bsc', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'bsc_pancakeswap_v3_pools', network: 'bsc', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'bsc_pancakeswap_v3_pools', network: 'bsc', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'bsc_pancakeswap_infinity_cl_pools', network: 'bsc', tokenColumn: 'currency0', logoColumn: 'currency0_logo_url' },
  { table: 'bsc_pancakeswap_infinity_cl_pools', network: 'bsc', tokenColumn: 'currency1', logoColumn: 'currency1_logo_url' },
  { table: 'bsc_uniswap_v3_pools', network: 'bsc', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'bsc_uniswap_v3_pools', network: 'bsc', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'bsc_uniswap_v4_pools', network: 'bsc', tokenColumn: 'currency0', logoColumn: 'currency0_logo_url' },
  { table: 'bsc_uniswap_v4_pools', network: 'bsc', tokenColumn: 'currency1', logoColumn: 'currency1_logo_url' },
  { table: 'base_uniswap_v2_pools', network: 'base', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'base_uniswap_v2_pools', network: 'base', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'base_uniswap_v3_pools', network: 'base', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'base_uniswap_v3_pools', network: 'base', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'base_uniswap_v4_pools', network: 'base', tokenColumn: 'currency0', logoColumn: 'currency0_logo_url' },
  { table: 'base_uniswap_v4_pools', network: 'base', tokenColumn: 'currency1', logoColumn: 'currency1_logo_url' },
  { table: 'robinhood_uniswap_v2_pools', network: 'robinhood', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'robinhood_uniswap_v2_pools', network: 'robinhood', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'robinhood_uniswap_v3_pools', network: 'robinhood', tokenColumn: 'token0', logoColumn: 'token0_logo_url' },
  { table: 'robinhood_uniswap_v3_pools', network: 'robinhood', tokenColumn: 'token1', logoColumn: 'token1_logo_url' },
  { table: 'robinhood_uniswap_v4_pools', network: 'robinhood', tokenColumn: 'currency0', logoColumn: 'currency0_logo_url' },
  { table: 'robinhood_uniswap_v4_pools', network: 'robinhood', tokenColumn: 'currency1', logoColumn: 'currency1_logo_url' },
];

const sleep = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function ensureLogoColumns(pg: Pool): Promise<void> {
  const statements = [...new Set(sources.map((source) => `ALTER TABLE ${source.table} ADD COLUMN IF NOT EXISTS ${source.logoColumn} TEXT`))];
  for (const statement of statements) {
    try { await pg.query(statement); }
    catch (error: any) { if (error?.code !== '42P01') throw error; }
  }
  await pg.query(`CREATE TABLE IF NOT EXISTS token_logo_attempts (network TEXT NOT NULL, token_address TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_error TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (network, token_address))`);
}

async function fetchJson(url: string): Promise<any> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    const response = await fetch(url, { headers: { accept: 'application/json' }, signal: controller.signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } finally { clearTimeout(timer); }
}

async function fetchDexScreenerLogo(network: Network, address: string): Promise<Logo | null> {
  const chain = network === 'solana' ? 'solana' : dexscreenerNetworks[network];
  const pairs = await fetchJson(`https://api.dexscreener.com/tokens/v1/${encodeURIComponent(chain)}/${encodeURIComponent(address)}`) as any[];
  const image = pairs.find((pair) => typeof pair?.info?.imageUrl === 'string')?.info?.imageUrl;
  return typeof image === 'string' && image.length > 0 ? { address, url: image, source: 'dexscreener' } : null;
}

async function fetchGeckoTerminalLogo(network: Network, address: string): Promise<Logo | null> {
  const geckoNetwork = network === 'robinhood' ? (process.env.TOKEN_LOGO_GECKO_ROBINHOOD_NETWORK ?? 'robinhood') : network;
  const body = await fetchJson(`https://api.geckoterminal.com/api/v2/networks/${encodeURIComponent(geckoNetwork)}/tokens/multi/${encodeURIComponent(address)}?include_inactive_source=false&include=top_pools&include_composition=false`);
  const token = Array.isArray(body?.data) ? body.data.find((entry: any) => String(entry?.attributes?.address ?? '').toLowerCase() === address.toLowerCase()) ?? body.data[0] : null;
  const image = token?.attributes?.image_url;
  return typeof image === 'string' && image.length > 0 ? { address, url: image, source: 'geckoterminal' } : null;
}

async function fetchLogo(network: Network, address: string): Promise<Logo | null> {
  try { const logo = await fetchDexScreenerLogo(network, address); if (logo) return logo; } catch { /* try GeckoTerminal */ }
  try { return await fetchGeckoTerminalLogo(network, address); } catch { return null; }
}

async function pendingTokens(pg: Pool): Promise<Map<string, { network: Network; sources: Source[] }>> {
  const result = new Map<string, { network: Network; sources: Source[] }>();
  for (const source of sources) {
    const query = `SELECT DISTINCT ${source.tokenColumn} AS address FROM ${source.table} t WHERE t.${source.tokenColumn} IS NOT NULL AND NULLIF(t.${source.logoColumn}, '') IS NULL AND NOT EXISTS (SELECT 1 FROM token_logo_attempts a WHERE a.network = $1 AND a.token_address = LOWER(t.${source.tokenColumn}) AND a.next_retry_at > NOW())`;
    let rows;
    try { rows = await pg.query<{ address: string }>(query, [source.network]); }
    catch (error: any) {
      if (error?.code === '42P01' || error?.code === '42703') continue;
      throw error;
    }
    for (const row of rows.rows) {
      if (!row.address) continue;
      const key = `${source.network}:${row.address.toLowerCase()}`;
      const current = result.get(key) ?? { network: source.network, sources: [] };
      current.sources.push(source);
      result.set(key, current);
    }
  }
  return result;
}

async function recordAttempt(pg: Pool, network: Network, address: string, error: unknown): Promise<void> {
  await pg.query(`INSERT INTO token_logo_attempts (network,token_address,attempts,next_retry_at,last_error,updated_at) VALUES ($1,$2,1,NOW()+($3 * INTERVAL '1 millisecond'),$4,NOW()) ON CONFLICT (network,token_address) DO UPDATE SET attempts=token_logo_attempts.attempts+1,next_retry_at=NOW()+($3 * INTERVAL '1 millisecond'),last_error=$4,updated_at=NOW()`, [network, address.toLowerCase(), retryDelayMs, String(error)]);
}

async function updateLogo(pg: Pool, source: Source, address: string, logo: Logo): Promise<number> {
  const result = await pg.query(`UPDATE ${source.table} SET ${source.logoColumn} = $1 WHERE LOWER(${source.tokenColumn}) = LOWER($2) AND NULLIF(${source.logoColumn}, '') IS NULL`, [logo.url, address]);
  return result.rowCount ?? 0;
}

export async function runTokenLogoWorker(once = false): Promise<void> {
  const pg = new Pool({ connectionString: postgresUrl, connectionTimeoutMillis: 10000, statement_timeout: 15000 });
  try {
    console.log(`[token-logo-worker] database=${postgresUrl.replace(/:\/\/[^@]+@/, '://***@')}`);
    await ensureLogoColumns(pg);
    do {
      const cycleStarted = Date.now();
      let cycleProcessed = 0;
      let cycleUpdated = 0;
      let cycleFailed = 0;
      while (true) {
        const pending = await pendingTokens(pg);
        const entries = [...pending.entries()].slice(0, batchSize);
        if (entries.length === 0) break;
        console.log(`[token-logo-worker] pending tokens=${pending.size}; processing=${entries.length}`);
        for (let index = 0; index < entries.length; index += concurrency) {
          await Promise.all(entries.slice(index, index + concurrency).map(async ([key, value]) => {
            const address = key.slice(`${value.network}:`.length);
            try {
              const logo = await fetchLogo(value.network, address);
              if (logo) {
                let updatedRows = 0;
                for (const source of value.sources) updatedRows += await updateLogo(pg, source, address, logo);
                if (updatedRows > 0) cycleUpdated += 1;
                else { await recordAttempt(pg, value.network, address, 'Logo found but no database row was updated'); cycleFailed += 1; }
              } else {
                await recordAttempt(pg, value.network, address, 'No logo returned by DexScreener or GeckoTerminal');
                cycleFailed += 1;
              }
            } catch (error) {
              await recordAttempt(pg, value.network, address, error);
              cycleFailed += 1;
            } finally { cycleProcessed += 1; }
          }));
          await sleep(delayMs);
        }
        console.log(`[token-logo-worker] batch complete: processed=${cycleProcessed} updated=${cycleUpdated} failed=${cycleFailed}`);
      }
      const elapsed = ((Date.now() - cycleStarted) / 1000).toFixed(1);
      console.log(`[token-logo-worker] cycle complete: processed=${cycleProcessed} updated=${cycleUpdated} failed=${cycleFailed} duration=${elapsed}s`);
      if (!once) {
        console.log(`[token-logo-worker] no eligible tokens remain; next scan in ${Math.round(intervalMs / 60000)} minutes`);
        await sleep(intervalMs);
      }
    } while (!once);
  } finally { await pg.end(); }
}

if (process.argv[1]?.endsWith('token_logo_worker.ts') || process.argv[1]?.endsWith('token_logo_worker.js')) {
  await runTokenLogoWorker(process.argv.includes('--once'));
}
