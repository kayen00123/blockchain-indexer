import 'dotenv/config';
import { Pool } from 'pg';
import { PoolTracker } from './tracker.js';

const tracker = new PoolTracker({
  windowMs: 15 * 60 * 1000,
  minThreshold: 1000,
  minUniqueWallets: 10,
  staleMs: 60 * 1000,
});

const pgPool = new Pool({
  connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres',
});

async function main() {
  const poolAddress = 'SmokePoolTestAddress1234567890';
  const now = Date.now();

  for (let i = 0; i < 1000; i += 1) {
    tracker.addSwap(poolAddress, `wallet-${i % 10}`, 1.23, now - 60000 + i * 10);
  }

  const eligible = tracker.shouldPromote(poolAddress, now);
  console.log('tracker_should_promote:', eligible);

  if (!eligible) {
    throw new Error('Smoke test failed: threshold logic did not promote the test pool.');
  }

  const payload = {
    address: poolAddress,
    pool_type: 'pumpswap',
    program_id: 'smoke-test',
    network: 'solana',
    base_mint: 'base-mint',
    base_symbol: 'BASE',
    base_decimals: 9,
    base_logo_url: null,
    quote_mint: 'quote-mint',
    quote_symbol: 'QUOTE',
    quote_decimals: 9,
    quote_logo_url: null,
    lp_mint: 'lp-mint',
    pool_base_token_account: 'base-account',
    pool_quote_token_account: 'quote-account',
    creator: 'smoke-creator',
    coin_creator: 'smoke-coin-creator',
    pool_index: 0,
    updated_slot: 0,
    discovered_at: new Date(now - 60000).toISOString(),
    indexed_at: new Date(now).toISOString(),
  };

  const result = await pgPool.query(
    `
      INSERT INTO pools (
        address, pool_type, program_id, network, base_mint, base_symbol, base_decimals,
        base_logo_url, quote_mint, quote_symbol, quote_decimals, quote_logo_url,
        lp_mint, pool_base_token_account, pool_quote_token_account, creator, coin_creator,
        pool_index, updated_slot, discovered_at, indexed_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
        $18, $19, $20, NOW()
      )
      ON CONFLICT (address) DO UPDATE SET
        pool_type = EXCLUDED.pool_type,
        program_id = EXCLUDED.program_id,
        network = EXCLUDED.network,
        base_mint = EXCLUDED.base_mint,
        base_symbol = EXCLUDED.base_symbol,
        base_decimals = EXCLUDED.base_decimals,
        base_logo_url = EXCLUDED.base_logo_url,
        quote_mint = EXCLUDED.quote_mint,
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
      payload.base_symbol,
      payload.base_decimals,
      payload.base_logo_url,
      payload.quote_mint,
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

  if (result.rowCount === null || result.rowCount < 1) {
    throw new Error('Postgres smoke insert reported no rows affected.');
  }

  console.log('supabase_insert_ok:', poolAddress);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
