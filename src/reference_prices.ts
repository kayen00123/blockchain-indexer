type Asset = 'sol' | 'bnb' | 'weth';

const defaults: Record<Asset, string> = {
  sol: 'https://api.geckoterminal.com/api/v2/simple/networks/solana/token_price/So11111111111111111111111111111111111111112',
  bnb: 'https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
  weth: 'https://api.geckoterminal.com/api/v2/simple/networks/base/token_price/0x4200000000000000000000000000000000000006',
};

const cache = new Map<Asset, { value: number; updatedAt: number }>();
const inflight = new Map<Asset, Promise<number | null>>();
const ttlMs = Number(process.env.REFERENCE_PRICE_TTL_MS ?? 180_000);
const timeoutMs = Number(process.env.REFERENCE_PRICE_TIMEOUT_MS ?? 10_000);

export async function getReferencePrice(asset: Asset): Promise<number | null> {
  const cached = cache.get(asset);
  if (cached && Date.now() - cached.updatedAt < ttlMs) return cached.value;
  const active = inflight.get(asset);
  if (active) return active;
  const request = (async () => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const url = asset === 'sol' ? process.env.SOL_PRICE_URL || defaults.sol : asset === 'bnb' ? process.env.BNB_PRICE_URL || defaults.bnb : process.env.BASE_PRICE_URL || defaults.weth;
      const response = await fetch(url, { headers: { accept: 'application/json' }, signal: controller.signal });
      if (!response.ok) throw new Error(`Reference price returned HTTP ${response.status}`);
      const body = await response.json() as any;
      const address = asset === 'sol' ? 'So11111111111111111111111111111111111111112' : asset === 'bnb' ? '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' : '0x4200000000000000000000000000000000000006';
      const value = Number(body.data?.attributes?.token_prices?.[address] ?? body.data?.attributes?.token_prices?.[address.toLowerCase()]);
      if (!Number.isFinite(value) || value <= 0) throw new Error('Reference price was invalid');
      cache.set(asset, { value, updatedAt: Date.now() });
      return value;
    } catch (error) {
      console.warn(`[reference-price] ${asset} refresh failed; retaining cached value:`, error);
      return cache.get(asset)?.value ?? null;
    } finally {
      clearTimeout(timer);
      inflight.delete(asset);
    }
  })();
  inflight.set(asset, request);
  return request;
}
