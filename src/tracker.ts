export type SwapEvent = {
  timestamp: number;
  wallet: string;
  volume: number;
};

export type PoolState = {
  poolAddress: string;
  baseMint?: string;
  quoteMint?: string;
  poolBaseTokenAccount?: string;
  poolQuoteTokenAccount?: string;
  events: SwapEvent[];
  promoted: boolean;
  promotedAt?: number;
  lastSeenAt: number;
  createdAt: number;
};

export class PoolTracker {
  private readonly pools = new Map<string, PoolState>();
  private readonly windowMs: number;
  private readonly minThreshold: number;
  private readonly minUniqueWallets: number;
  private readonly staleMs: number;

  constructor(config: {
    windowMs: number;
    minThreshold: number;
    minUniqueWallets: number;
    staleMs: number;
  }) {
    this.windowMs = config.windowMs;
    this.minThreshold = config.minThreshold;
    this.minUniqueWallets = config.minUniqueWallets;
    this.staleMs = config.staleMs;
  }

  reset(): void {
    this.pools.clear();
  }

  addSwap(
    poolAddress: string,
    wallet: string,
    volume: number,
    now = Date.now(),
    mints?: {
      baseMint?: string;
      quoteMint?: string;
      poolBaseTokenAccount?: string;
      poolQuoteTokenAccount?: string;
    },
  ): PoolState {
    const current = this.pools.get(poolAddress) ?? {
      poolAddress,
      events: [],
      promoted: false,
      lastSeenAt: now,
      createdAt: now,
    };

    current.events.push({ timestamp: now, wallet, volume });
    current.baseMint = mints?.baseMint ?? current.baseMint;
    current.quoteMint = mints?.quoteMint ?? current.quoteMint;
    current.poolBaseTokenAccount = mints?.poolBaseTokenAccount ?? current.poolBaseTokenAccount;
    current.poolQuoteTokenAccount = mints?.poolQuoteTokenAccount ?? current.poolQuoteTokenAccount;
    current.lastSeenAt = now;
    current.createdAt = current.createdAt ?? now;

    current.events = current.events.filter((event) => event.timestamp >= now - this.windowMs);

    this.pools.set(poolAddress, current);
    return current;
  }

  shouldPromote(poolAddress: string, now = Date.now()): boolean {
    const pool = this.pools.get(poolAddress);
    if (!pool || pool.promoted) {
      return false;
    }

    const eligibleEvents = pool.events.filter((event) => event.timestamp >= now - this.windowMs);
    if (eligibleEvents.length < this.minThreshold) {
      return false;
    }

    const uniqueWallets = new Set(eligibleEvents.map((event) => event.wallet));
    const stale = now - pool.lastSeenAt > this.staleMs;

    return uniqueWallets.size >= this.minUniqueWallets && !stale;
  }

  markPromoted(poolAddress: string, now = Date.now()) {
    const pool = this.pools.get(poolAddress);
    if (!pool) {
      return;
    }

    pool.promoted = true;
    pool.promotedAt = now;
  }

  getPool(poolAddress: string): PoolState | undefined {
    return this.pools.get(poolAddress);
  }

  listCandidates(): PoolState[] {
    return [...this.pools.values()]
      .filter((pool) => !pool.promoted)
      .sort((a, b) => b.lastSeenAt - a.lastSeenAt);
  }
}
