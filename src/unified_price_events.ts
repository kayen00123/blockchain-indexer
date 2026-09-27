import { appendFile, mkdir, open, stat } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const DEFAULT_PRICE_FILES = [
  'price-events.jsonl',
  'pumpswap-price-events.jsonl',
  'raydium-price-events.jsonl',
  'meteora-dlmm-price-events.jsonl',
  'meteora-damm-v2-price-events.jsonl',
  'orca-whirlpool-price-events.jsonl',
  'pancakeswap-v2-price-events.jsonl',
  'pancakeswap-v3-price-events.jsonl',
  'pancakeswap-infinity-price-events.jsonl',
  'uniswap-v3-bsc-price-events.jsonl',
  'uniswap-v4-base-price-events.jsonl',
  'robinhood-uniswap-v4-price-events.jsonl',
  'robinhood-uniswap-v3-price-events.jsonl',
  'uniswap-v3-base-price-events.jsonl',
  'uniswap-v2-base-price-events.jsonl',
  'uniswap-v4-bsc-price-events.jsonl',
];

const unifiedFile = process.env.UNIFIED_PRICE_EVENT_FILE ?? 'unified-price-events.jsonl';
const pollMs = Number(process.env.UNIFIED_PRICE_EVENT_POLL_MS ?? 1000);

type TailState = { offset: number; remainder: string };

export async function startUnifiedPriceEventMirror(): Promise<void> {
  const configured = process.env.UNIFIED_PRICE_SOURCE_FILES?.split(/[\r\n,]+/).map((value) => value.trim()).filter(Boolean);
  const sourceFiles = [...new Set(configured?.length ? configured : DEFAULT_PRICE_FILES)].map((file) => resolve(file));
  const states = new Map<string, TailState>();
  await mkdir(dirname(resolve(unifiedFile)), { recursive: true });

  for (const file of sourceFiles) {
    try { states.set(file, { offset: (await stat(file)).size, remainder: '' }); } catch { states.set(file, { offset: 0, remainder: '' }); }
  }

  let queue = Promise.resolve();
  const poll = async () => {
    for (const file of sourceFiles) {
      const state = states.get(file)!;
      try {
        const size = (await stat(file)).size;
        if (size < state.offset) state.offset = 0;
        if (size === state.offset) continue;
        const handle = await open(file, 'r');
        const buffer = Buffer.alloc(size - state.offset);
        await handle.read(buffer, 0, buffer.length, state.offset);
        await handle.close();
        const chunk = buffer.toString('utf8');
        state.offset = size;
        const lines = `${state.remainder}${chunk}`.split('\n');
        state.remainder = lines.pop() ?? '';
        const events = lines.flatMap((line) => {
          try {
            const event = JSON.parse(line);
            return event.type === 'price' ? [event] : [];
          } catch { return []; }
        });
        if (events.length > 0) {
          queue = queue.then(() => appendFile(unifiedFile, events.map((event) => `${JSON.stringify(event)}\n`).join(''), 'utf8'));
        }
      } catch { /* Source files may not exist until their indexer receives its first price. */ }
    }
    await queue;
  };

  console.log(`[unified-prices] writing main-run price events to ${unifiedFile}`);
  setInterval(() => void poll(), pollMs);
}
