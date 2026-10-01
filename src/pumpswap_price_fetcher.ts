import { appendFile } from 'node:fs/promises';
import type { Pool as PgPool } from 'pg';
import { PUMPSWAP_PROGRAM_ID, getPumpAmmSwapMeta, type SolanaTxLike } from './pumpswap_decoder.js';
import { getReferencePrice } from './reference_prices.js';

const PRICE_EVENT_FILE = process.env.PUMPSWAP_PRICE_EVENT_FILE ?? 'pumpswap-price-events.jsonl';
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SOL_PRICE_REFRESH_MS = 3 * 60 * 1000;

export type PumpswapPricePool = {
  poolAddress: string;
  baseMint: string;
  quoteMint: string;
  baseDecimals: number;
  quoteDecimals: number;
  baseVault: string;
  quoteVault: string;
};

export type RegisterPumpswapPricePool = (pool: PumpswapPricePool) => void;

export function createPumpswapPriceFetcher(pgPool: PgPool): {
  registerPricePool: RegisterPumpswapPricePool;
  processPumpswapPrices: (tx: SolanaTxLike, slot: number) => Promise<void>;
} {
  const pools = new Map<string, PumpswapPricePool>();
  let solUsdPrice: number | null = null;
  let solPriceUpdatedAt: number | null = null;

  const recordPriceEvent = async (event: Record<string, unknown>) => {
    try {
      await appendFile(
        PRICE_EVENT_FILE,
        `${JSON.stringify({ timestamp: new Date().toISOString(), ...event })}\n`,
        'utf8'
      );
    } catch (error) {
      console.error('[pumpswap-price] Failed to write price event record:', error);
    }
  };

  const refreshSolUsdPrice = async () => {
    try {
      const value = await getReferencePrice('sol');
      if (value === null) throw new Error('No SOL reference price available');
      solUsdPrice = value;
      solPriceUpdatedAt = Date.now();
      console.log(`[pumpswap-price] SOL/USD=${value}`);
    } catch (error) {
      console.warn('[pumpswap-price] SOL/USD reference refresh failed; retaining last good value:', error);
      void recordPriceEvent({ type: 'error', stage: 'solPriceReference', error: String(error) });
    }
  };

  const processPumpswapPrices = async (tx: SolanaTxLike, slot: number) => {
    // Refresh SOL price if needed
    if (!solPriceUpdatedAt || Date.now() - solPriceUpdatedAt > SOL_PRICE_REFRESH_MS) {
      await refreshSolUsdPrice();
    }

    // Decode PumpSwap swap from transaction
    const swapMeta = getPumpAmmSwapMeta(tx, PUMPSWAP_PROGRAM_ID);
    if (!swapMeta) return;

    const pool = pools.get(swapMeta.poolAddress);
    if (!pool) return; // Pool not registered for price tracking

    // Token balance accountIndex values reference the full message key list.
    const accounts = tx.transaction?.message?.accountKeys ?? [];
    const baseVaultIndex = accounts.indexOf(pool.baseVault);
    const quoteVaultIndex = accounts.indexOf(pool.quoteVault);

    if (baseVaultIndex === -1 || quoteVaultIndex === -1) {
      return;
    }

    // Decode token amounts from account data
    const baseData = (tx.meta as any)?.postTokenBalances?.find((balance: any) => {
      return accounts[balance?.accountIndex] === pool.baseVault;
    });
    const quoteData = (tx.meta as any)?.postTokenBalances?.find((balance: any) => {
      return accounts[balance?.accountIndex] === pool.quoteVault;
    });

    if (!baseData || !quoteData) return;

    const baseAmount = (baseData as any)?.tokenAmount?.uiAmount ?? 0;
    const quoteAmount = (quoteData as any)?.tokenAmount?.uiAmount ?? 0;

    if (baseAmount <= 0 || quoteAmount <= 0) return;

    let basePrice: number | null = null;
    let quotePrice: number | null = null;

    // Calculate price in terms of quote token per base token
    const priceQuotePerBase = quoteAmount / baseAmount;

    // Determine USD prices based on mint pairs
    if (pool.quoteMint === SOL_MINT && solUsdPrice) {
      quotePrice = solUsdPrice;
      basePrice = priceQuotePerBase * solUsdPrice;
    } else if (pool.baseMint === SOL_MINT && solUsdPrice) {
      basePrice = solUsdPrice;
      quotePrice = baseAmount > 0 ? solUsdPrice / priceQuotePerBase : null;
    } else if (pool.quoteMint === USDC_MINT) {
      quotePrice = 1; // Assume USDC ≈ $1
      basePrice = priceQuotePerBase;
    } else if (pool.baseMint === USDC_MINT) {
      basePrice = 1;
      quotePrice = baseAmount > 0 ? 1 / priceQuotePerBase : null;
    }

    if (!basePrice || !quotePrice) return;

    const event = {
      type: 'pumpswap_price_snapshot',
      pool: swapMeta.poolAddress,
      slot,
      signature: tx.transaction?.signatures?.[0] ?? 'unknown',
      baseMint: pool.baseMint,
      quoteMint: pool.quoteMint,
      baseAmount,
      quoteAmount,
      priceQuotePerBase,
      baseUsdPrice: basePrice,
      quoteUsdPrice: quotePrice,
    };

    await recordPriceEvent(event);

    if (basePrice > 0 && quotePrice > 0) {
      const client = await pgPool.connect();
      try {
        await client.query('BEGIN');
        await client.query(
          `INSERT INTO latest_prices (
             pool_address, price, inverse_price, price_change, price_change_percent,
             price_change_direction, fdv_usd, token_price_usd, total_supply, supply_basis,
             base_reserve, quote_reserve, updated_slot, updated_at
           ) VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, $4, NULL, 'pool_reserves', $5, $6, $7, NOW())
           ON CONFLICT (pool_address) DO UPDATE SET
             price = EXCLUDED.price,
             inverse_price = EXCLUDED.inverse_price,
             token_price_usd = EXCLUDED.token_price_usd,
             price_change = EXCLUDED.price - latest_prices.price,
             price_change_percent = CASE WHEN latest_prices.price IS NULL OR latest_prices.price = 0 THEN NULL
               ELSE ((EXCLUDED.price - latest_prices.price) / latest_prices.price) * 100 END,
             price_change_direction = CASE WHEN latest_prices.price IS NULL THEN NULL
               WHEN EXCLUDED.price > latest_prices.price THEN 'up'
               WHEN EXCLUDED.price < latest_prices.price THEN 'down'
               ELSE 'flat' END,
             price_change_24h = (
               SELECT CASE
                 WHEN ref.open IS NULL OR ref.open = 0 THEN NULL
                 ELSE ((EXCLUDED.price - ref.open) / ref.open) * 100
               END
               FROM (
                 SELECT open
                 FROM price_candles
                 WHERE pool_address = EXCLUDED.pool_address
                   AND timeframe = '1h'
                   AND bucket_start >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '25 hours')) * 1000
                 ORDER BY bucket_start ASC
                 LIMIT 1
               ) ref
             ),
             base_reserve = EXCLUDED.base_reserve,
             quote_reserve = EXCLUDED.quote_reserve,
             updated_slot = EXCLUDED.updated_slot,
             updated_at = EXCLUDED.updated_at`,
          [swapMeta.poolAddress, priceQuotePerBase, 1 / priceQuotePerBase, basePrice, baseAmount, quoteAmount, slot]
        );
        await client.query('COMMIT');
      } catch (error) {
        console.error('[pumpswap-price] Failed to insert price snapshot:', error);
        await client.query('ROLLBACK').catch(() => undefined);
      } finally {
        client.release();
      }
    }
  };

  const registerPricePool: RegisterPumpswapPricePool = (pool: PumpswapPricePool) => {
    pools.set(pool.poolAddress, pool);
    console.log(`[pumpswap-price] Registered pool ${pool.poolAddress} for price tracking`);
  };

  // Initialize SOL price on startup
  void refreshSolUsdPrice();

  return {
    registerPricePool,
    processPumpswapPrices,
  };
}
