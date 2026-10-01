BEGIN;

CREATE TABLE IF NOT EXISTS pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  base_mint TEXT NOT NULL,
  base_name TEXT,
  base_symbol TEXT,
  base_decimals INTEGER NOT NULL,
  base_logo_url TEXT,
  quote_mint TEXT NOT NULL,
  quote_name TEXT,
  quote_symbol TEXT,
  quote_decimals INTEGER NOT NULL,
  quote_logo_url TEXT,
  lp_mint TEXT NOT NULL,
  pool_base_token_account TEXT NOT NULL,
  pool_quote_token_account TEXT NOT NULL,
  creator TEXT NOT NULL,
  coin_creator TEXT NOT NULL,
  pool_index INTEGER NOT NULL,
  updated_slot BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE pools ADD COLUMN IF NOT EXISTS base_name TEXT;
ALTER TABLE pools ADD COLUMN IF NOT EXISTS quote_name TEXT;
CREATE INDEX IF NOT EXISTS pools_base_mint_idx ON pools(base_mint);
CREATE INDEX IF NOT EXISTS pools_quote_mint_idx ON pools(quote_mint);
CREATE INDEX IF NOT EXISTS pools_updated_slot_idx ON pools(updated_slot);

CREATE TABLE IF NOT EXISTS latest_prices (
  pool_address TEXT PRIMARY KEY,
  price DOUBLE PRECISION,
  inverse_price DOUBLE PRECISION,
  price_change DOUBLE PRECISION,
  price_change_percent DOUBLE PRECISION,
  price_change_direction TEXT,
  fdv_usd DOUBLE PRECISION,
  token_price_usd DOUBLE PRECISION,
  total_supply DOUBLE PRECISION,
  supply_basis TEXT,
  base_reserve NUMERIC NOT NULL,
  quote_reserve NUMERIC NOT NULL,
  updated_slot BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE latest_prices ADD COLUMN IF NOT EXISTS high_24h DOUBLE PRECISION;
ALTER TABLE latest_prices ADD COLUMN IF NOT EXISTS low_24h DOUBLE PRECISION;
ALTER TABLE latest_prices ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC;

CREATE TABLE IF NOT EXISTS price_candles (
  pool_address TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  bucket_start BIGINT NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pool_address, timeframe, bucket_start)
);
CREATE INDEX IF NOT EXISTS price_candles_lookup_idx ON price_candles(pool_address, timeframe, bucket_start);

CREATE TABLE IF NOT EXISTS evm_price_history (
  pool_address TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  inverse_price DOUBLE PRECISION,
  updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS evm_price_history_lookup_idx ON evm_price_history(pool_address, updated_at);

CREATE TABLE IF NOT EXISTS raydium_pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  amm_config TEXT NOT NULL,
  owner TEXT NOT NULL,
  token_mint_0 TEXT NOT NULL,
  token_mint_0_symbol TEXT,
  token_mint_0_decimals INTEGER NOT NULL,
  token_mint_0_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_mint_0_logo_url TEXT,
  token_mint_1 TEXT NOT NULL,
  token_mint_1_symbol TEXT,
  token_mint_1_decimals INTEGER NOT NULL,
  token_mint_1_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_mint_1_logo_url TEXT,
  token_vault_0 TEXT NOT NULL,
  token_vault_1 TEXT NOT NULL,
  observation_key TEXT NOT NULL,
  tick_spacing INTEGER NOT NULL,
  sqrt_price_x64 NUMERIC NOT NULL,
  tick_current INTEGER NOT NULL,
  updated_slot BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS raydium_pools_mint_0_idx ON raydium_pools(token_mint_0);
CREATE INDEX IF NOT EXISTS raydium_pools_mint_1_idx ON raydium_pools(token_mint_1);

CREATE TABLE IF NOT EXISTS raydium_cpmm_pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  amm_config TEXT NOT NULL,
  pool_creator TEXT NOT NULL,
  token0 TEXT NOT NULL,
  token0_symbol TEXT,
  token0_logo_url TEXT,
  token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL,
  token1_symbol TEXT,
  token1_logo_url TEXT,
  token1_decimals INTEGER NOT NULL,
  token0_vault TEXT NOT NULL,
  token1_vault TEXT NOT NULL,
  lp_mint TEXT NOT NULL,
  updated_slot BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token0_symbol TEXT;
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token1_symbol TEXT;
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;
CREATE INDEX IF NOT EXISTS raydium_cpmm_token0_idx ON raydium_cpmm_pools(token0);
ALTER TABLE raydium_pools ADD COLUMN IF NOT EXISTS token_mint_0_name TEXT;
ALTER TABLE raydium_pools ADD COLUMN IF NOT EXISTS token_mint_1_name TEXT;
CREATE INDEX IF NOT EXISTS raydium_cpmm_token1_idx ON raydium_cpmm_pools(token1);

CREATE TABLE IF NOT EXISTS orca_whirlpools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  whirlpools_config TEXT NOT NULL,
  token_mint_a TEXT NOT NULL,
  token_mint_a_symbol TEXT,
  token_mint_a_decimals INTEGER NOT NULL,
  token_mint_a_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_mint_a_logo_url TEXT,
  token_mint_b TEXT NOT NULL,
  token_mint_b_symbol TEXT,
  token_mint_b_decimals INTEGER NOT NULL,
  token_mint_b_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_mint_b_logo_url TEXT,
  token_vault_a TEXT NOT NULL,
  token_vault_b TEXT NOT NULL,
  tick_spacing INTEGER NOT NULL,
  fee_rate INTEGER NOT NULL,
  protocol_fee_rate INTEGER NOT NULL,
  liquidity NUMERIC NOT NULL,
  sqrt_price_x64 NUMERIC NOT NULL,
  tick_current_index INTEGER NOT NULL,
  updated_slot BIGINT NOT NULL,
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token0_name TEXT;
ALTER TABLE raydium_cpmm_pools ADD COLUMN IF NOT EXISTS token1_name TEXT;
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS orca_whirlpools_mint_a_idx ON orca_whirlpools(token_mint_a);
CREATE INDEX IF NOT EXISTS orca_whirlpools_mint_b_idx ON orca_whirlpools(token_mint_b);

CREATE TABLE IF NOT EXISTS meteora_damm_v2_pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  creator TEXT NOT NULL,
  token_a_mint TEXT NOT NULL,
  token_a_symbol TEXT,
  token_a_decimals INTEGER NOT NULL,
  token_a_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_a_logo_url TEXT,
  token_b_mint TEXT NOT NULL,
  token_b_symbol TEXT,
  token_b_decimals INTEGER NOT NULL,
  token_b_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_b_logo_url TEXT,
  token_a_vault TEXT NOT NULL,
  token_b_vault TEXT NOT NULL,
  token_a_amount NUMERIC NOT NULL,
  token_b_amount NUMERIC NOT NULL,
  sqrt_price NUMERIC NOT NULL,
  activation_point NUMERIC NOT NULL,
  pool_mode INTEGER NOT NULL,
  updated_slot BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS meteora_damm_v2_mint_a_idx ON meteora_damm_v2_pools(token_a_mint);
CREATE INDEX IF NOT EXISTS meteora_damm_v2_mint_b_idx ON meteora_damm_v2_pools(token_b_mint);
ALTER TABLE meteora_damm_v2_pools ADD COLUMN IF NOT EXISTS token_a_name TEXT;
ALTER TABLE meteora_damm_v2_pools ADD COLUMN IF NOT EXISTS token_b_name TEXT;

CREATE TABLE IF NOT EXISTS meteora_dlmm_pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  program_id TEXT NOT NULL,
  network TEXT NOT NULL,
  creator TEXT NOT NULL,
  token_x_mint TEXT NOT NULL,
  token_x_symbol TEXT,
  token_x_decimals INTEGER NOT NULL,
  token_x_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_x_logo_url TEXT,
  token_y_mint TEXT NOT NULL,
  token_y_symbol TEXT,
  token_y_decimals INTEGER NOT NULL,
  token_y_total_supply_raw NUMERIC NOT NULL DEFAULT 0,
  token_y_logo_url TEXT,
  reserve_x TEXT NOT NULL,
  reserve_y TEXT NOT NULL,
  oracle TEXT NOT NULL,
  active_id INTEGER NOT NULL,
  bin_step INTEGER NOT NULL,
  activation_point NUMERIC NOT NULL,
  updated_slot BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS meteora_dlmm_mint_x_idx ON meteora_dlmm_pools(token_x_mint);
CREATE INDEX IF NOT EXISTS meteora_dlmm_mint_y_idx ON meteora_dlmm_pools(token_y_mint);
ALTER TABLE meteora_dlmm_pools ADD COLUMN IF NOT EXISTS token_x_name TEXT;
ALTER TABLE meteora_dlmm_pools ADD COLUMN IF NOT EXISTS token_y_name TEXT;

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_v2_pools (
  address TEXT PRIMARY KEY,
  pool_type TEXT NOT NULL,
  chain TEXT NOT NULL,
  factory TEXT NOT NULL,
  token0 TEXT NOT NULL,
  token0_symbol TEXT,
  token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL,
  token1_symbol TEXT,
  token1_decimals INTEGER NOT NULL,
  pair_index NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_v2_token0_idx ON bsc_pancakeswap_v2_pools(token0);
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_v2_token1_idx ON bsc_pancakeswap_v2_pools(token1);
ALTER TABLE bsc_pancakeswap_v2_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE bsc_pancakeswap_v2_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_v3_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_v3_token0_idx ON bsc_pancakeswap_v3_pools(token0);
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_v3_token1_idx ON bsc_pancakeswap_v3_pools(token1);
ALTER TABLE bsc_pancakeswap_v3_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE bsc_pancakeswap_v3_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS bsc_uniswap_v3_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS bsc_uniswap_v3_token0_idx ON bsc_uniswap_v3_pools(token0);
CREATE INDEX IF NOT EXISTS bsc_uniswap_v3_token1_idx ON bsc_uniswap_v3_pools(token1);
ALTER TABLE bsc_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE bsc_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS bsc_uniswap_v3_prices (
  pool_address TEXT PRIMARY KEY, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL, quote_token TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bsc_uniswap_v4_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, manager TEXT NOT NULL, pool_id TEXT NOT NULL,
  currency0 TEXT NOT NULL, currency0_symbol TEXT, currency0_decimals INTEGER NOT NULL,
  currency1 TEXT NOT NULL, currency1_symbol TEXT, currency1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, hooks TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  tick INTEGER NOT NULL, transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS bsc_uniswap_v4_currency0_idx ON bsc_uniswap_v4_pools(currency0);
CREATE INDEX IF NOT EXISTS bsc_uniswap_v4_currency1_idx ON bsc_uniswap_v4_pools(currency1);
ALTER TABLE bsc_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency0_logo_url TEXT;
ALTER TABLE bsc_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS bsc_uniswap_v4_prices (
  pool_id TEXT PRIMARY KEY, pool_address TEXT NOT NULL, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_currency TEXT NOT NULL, quote_currency TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, fee INTEGER NOT NULL,
  updated_block BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS base_uniswap_v4_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, manager TEXT NOT NULL, pool_id TEXT NOT NULL,
  currency0 TEXT NOT NULL, currency0_symbol TEXT, currency0_decimals INTEGER NOT NULL,
  currency1 TEXT NOT NULL, currency1_symbol TEXT, currency1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, hooks TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  tick INTEGER NOT NULL, transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS base_uniswap_v4_currency0_idx ON base_uniswap_v4_pools(currency0);
CREATE INDEX IF NOT EXISTS base_uniswap_v4_currency1_idx ON base_uniswap_v4_pools(currency1);
ALTER TABLE base_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency0_logo_url TEXT;
ALTER TABLE base_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS base_uniswap_v4_prices (
  pool_id TEXT PRIMARY KEY, pool_address TEXT NOT NULL, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_currency TEXT NOT NULL, quote_currency TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, fee INTEGER NOT NULL,
  updated_block BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS base_uniswap_v3_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS base_uniswap_v3_token0_idx ON base_uniswap_v3_pools(token0);
CREATE INDEX IF NOT EXISTS base_uniswap_v3_token1_idx ON base_uniswap_v3_pools(token1);
ALTER TABLE base_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE base_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS base_uniswap_v3_prices (
  pool_address TEXT PRIMARY KEY, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL, quote_token TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS base_uniswap_v2_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS base_uniswap_v2_token0_idx ON base_uniswap_v2_pools(token0);
CREATE INDEX IF NOT EXISTS base_uniswap_v2_token1_idx ON base_uniswap_v2_pools(token1);
ALTER TABLE base_uniswap_v2_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE base_uniswap_v2_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS base_uniswap_v2_prices (
  pool_address TEXT PRIMARY KEY, reserve0 NUMERIC NOT NULL, reserve1 NUMERIC NOT NULL,
  price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL, quote_token TEXT NOT NULL, updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS aerodrome_slipstream_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE aerodrome_slipstream_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE aerodrome_slipstream_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;
CREATE INDEX IF NOT EXISTS aerodrome_slipstream_token0_idx ON aerodrome_slipstream_pools(token0);
CREATE INDEX IF NOT EXISTS aerodrome_slipstream_token1_idx ON aerodrome_slipstream_pools(token1);

CREATE TABLE IF NOT EXISTS aerodrome_slipstream_prices (
  pool_address TEXT PRIMARY KEY, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL, quote_token TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_infinity_cl_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, manager TEXT NOT NULL, pool_id TEXT NOT NULL,
  currency0 TEXT NOT NULL, currency0_symbol TEXT, currency0_decimals INTEGER NOT NULL,
  currency1 TEXT NOT NULL, currency1_symbol TEXT, currency1_decimals INTEGER NOT NULL,
  hooks TEXT NOT NULL, fee INTEGER NOT NULL, parameters TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  tick INTEGER NOT NULL, transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE bsc_pancakeswap_infinity_cl_pools ADD COLUMN IF NOT EXISTS currency0_logo_url TEXT;
ALTER TABLE bsc_pancakeswap_infinity_cl_pools ADD COLUMN IF NOT EXISTS currency1_logo_url TEXT;
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_infinity_cl_currency0_idx ON bsc_pancakeswap_infinity_cl_pools(currency0);
CREATE INDEX IF NOT EXISTS bsc_pancakeswap_infinity_cl_currency1_idx ON bsc_pancakeswap_infinity_cl_pools(currency1);

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_infinity_cl_prices (
  pool_id TEXT PRIMARY KEY, pool_address TEXT NOT NULL, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_currency TEXT NOT NULL, quote_currency TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, fee INTEGER NOT NULL,
  updated_block BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_v3_prices (
  pool_address TEXT PRIMARY KEY,
  price DOUBLE PRECISION,
  inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL,
  quote_token TEXT NOT NULL,
  sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL,
  tick INTEGER NOT NULL,
  updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS bsc_pancakeswap_v2_prices (
  pool_address TEXT PRIMARY KEY,
  reserve0 NUMERIC NOT NULL,
  reserve1 NUMERIC NOT NULL,
  price DOUBLE PRECISION,
  inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL,
  quote_token TEXT NOT NULL,
  updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v2_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  pair_index TEXT NOT NULL, transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE robinhood_uniswap_v2_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE robinhood_uniswap_v2_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v2_token0_idx ON robinhood_uniswap_v2_pools(token0);
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v2_token1_idx ON robinhood_uniswap_v2_pools(token1);

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v2_prices (
  pool_address TEXT PRIMARY KEY, reserve0 NUMERIC NOT NULL, reserve1 NUMERIC NOT NULL,
  price DOUBLE PRECISION, inverse_price DOUBLE PRECISION, base_token TEXT NOT NULL,
  quote_token TEXT NOT NULL, updated_block BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v3_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, factory TEXT NOT NULL,
  token0 TEXT NOT NULL, token0_symbol TEXT, token0_decimals INTEGER NOT NULL,
  token1 TEXT NOT NULL, token1_symbol TEXT, token1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, transaction_hash TEXT NOT NULL,
  block_number BIGINT NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v3_token0_idx ON robinhood_uniswap_v3_pools(token0);
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v3_token1_idx ON robinhood_uniswap_v3_pools(token1);
ALTER TABLE robinhood_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token0_logo_url TEXT;
ALTER TABLE robinhood_uniswap_v3_pools ADD COLUMN IF NOT EXISTS token1_logo_url TEXT;

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v3_prices (
  pool_address TEXT PRIMARY KEY, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_token TEXT NOT NULL, quote_token TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, updated_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v4_pools (
  address TEXT PRIMARY KEY, pool_type TEXT NOT NULL, chain TEXT NOT NULL, manager TEXT NOT NULL, pool_id TEXT NOT NULL,
  currency0 TEXT NOT NULL, currency0_symbol TEXT, currency0_decimals INTEGER NOT NULL,
  currency1 TEXT NOT NULL, currency1_symbol TEXT, currency1_decimals INTEGER NOT NULL,
  fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, hooks TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  tick INTEGER NOT NULL, transaction_hash TEXT NOT NULL, block_number BIGINT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL, indexed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE robinhood_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency0_logo_url TEXT;
ALTER TABLE robinhood_uniswap_v4_pools ADD COLUMN IF NOT EXISTS currency1_logo_url TEXT;
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v4_currency0_idx ON robinhood_uniswap_v4_pools(currency0);
CREATE INDEX IF NOT EXISTS robinhood_uniswap_v4_currency1_idx ON robinhood_uniswap_v4_pools(currency1);

CREATE TABLE IF NOT EXISTS robinhood_uniswap_v4_prices (
  pool_id TEXT PRIMARY KEY, pool_address TEXT NOT NULL, price DOUBLE PRECISION, inverse_price DOUBLE PRECISION,
  base_currency TEXT NOT NULL, quote_currency TEXT NOT NULL, sqrt_price_x96 NUMERIC NOT NULL,
  liquidity NUMERIC NOT NULL, tick INTEGER NOT NULL, fee INTEGER NOT NULL,
  updated_block BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);

COMMIT;

-- Migration: Add price_change_24h column for industry-standard 24-hour price change
-- ALTER TABLE latest_prices ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC;
