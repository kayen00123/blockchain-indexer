// PumpSwap decoder for websocket transaction fetching
import bs58 from 'bs58';

export const PUMPSWAP_PROGRAM_ID = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

const PUMP_AMM_BUY_DISCRIMINATOR = Buffer.from([102, 6, 61, 18, 1, 218, 235, 234]);
const PUMP_AMM_SELL_DISCRIMINATOR = Buffer.from([51, 230, 133, 164, 1, 127, 131, 173]);

export type PumpAmmSwapMeta = {
  poolAddress: string;
  wallet: string;
  baseMint: string;
  quoteMint: string;
  poolBaseTokenAccount: string;
  poolQuoteTokenAccount: string;
};

type SolanaAccountKey = { toBase58?: () => string; pubkey?: string } | string;
export type SolanaTxLike = {
  transaction?: {
    signatures?: string[];
    message?: {
      accountKeys?: SolanaAccountKey[];
      instructions?: any[];
    };
  };
  meta?: {
    innerInstructions?: any[];
  };
};

const KNOWN_SYSTEM_ADDRESSES = new Set([
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // SPL Token
  '11111111111111111111111111111111',             // System Program
  'SysvarRent111111111111111111111111111111111', // Rent Sysvar
  'MemoSq4gDiYM2tgSjUim33LpCasJ2j6sSJF4vqmVvnN', // Memo
  'WormholeWrappedSOL',
  'ComputeBudget111111111111111111111111111111', // Compute Budget
  '1nc1nerator11111111111111111111111111111111', // Incinerator (burn)
  'Vote111111111111111111111111111111111111111', // Vote
  'SysvarC1ock11111111111111111111111111111111', // Clock
]);

function normalizeAccountAddress(value: SolanaAccountKey | undefined): string {
  if (!value) return '';
  if (typeof value === 'string') return value;
  return value.pubkey ?? value.toBase58?.() ?? '';
}

function isKnownSystemOrProgramAddress(address: string): boolean {
  return KNOWN_SYSTEM_ADDRESSES.has(address) || !address;
}

function decodeInstructionData(data: unknown): Buffer | null {
  if (typeof data === 'string') {
    try {
      return Buffer.from(bs58.decode(data));
    } catch {
      return null;
    }
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

/**
 * Decode PumpSwap swap metadata from a WebSocket transaction.
 * Searches both outer and inner instructions for BUY/SELL discriminators.
 */
export function getPumpAmmSwapMeta(tx: SolanaTxLike, programAddress?: string): PumpAmmSwapMeta | null {
  const actualProgramId = programAddress ?? PUMPSWAP_PROGRAM_ID;
  
  const message = tx?.transaction?.message;
  const accountKeys = Array.isArray(message?.accountKeys)
    ? message.accountKeys.map((key: SolanaAccountKey) => normalizeAccountAddress(key))
    : [];

  if (!accountKeys.includes(actualProgramId)) {
    return null;
  }

  const programIndex = accountKeys.indexOf(actualProgramId);
  const outerInstructions = Array.isArray(message?.instructions) ? message.instructions : [];
  const innerInstructions = Array.isArray(tx?.meta?.innerInstructions)
    ? tx.meta.innerInstructions.flatMap((entry: any) => Array.isArray(entry?.instructions) ? entry.instructions : [])
    : [];
  const instructions = [...outerInstructions, ...innerInstructions];

  for (const instruction of instructions) {
    const instructionProgram = normalizeAccountAddress(instruction?.programId);
    const hasMatchingProgramIndex = instruction?.programIdIndex === programIndex;
    const hasMatchingProgramAddress = instructionProgram === actualProgramId;
    if (!hasMatchingProgramIndex && !hasMatchingProgramAddress) {
      continue;
    }

    const dataBuffer = decodeInstructionData(instruction?.data);
    if (!dataBuffer || dataBuffer.length < 8) {
      continue;
    }

    const discriminator = dataBuffer.subarray(0, 8);
    const isBuy = discriminator.equals(PUMP_AMM_BUY_DISCRIMINATOR);
    const isSell = discriminator.equals(PUMP_AMM_SELL_DISCRIMINATOR);
    if (!isBuy && !isSell) {
      continue;
    }

    const instructionAccountKeys = getInstructionAccountKeys(instruction, accountKeys);
    if (instructionAccountKeys.length < 17) {
      continue;
    }

    const poolAddress = instructionAccountKeys[0];
    const wallet = instructionAccountKeys[1];

    if (!poolAddress || !wallet || poolAddress === wallet || isKnownSystemOrProgramAddress(poolAddress) || isKnownSystemOrProgramAddress(wallet)) {
      continue;
    }

    const baseMint = instructionAccountKeys[3];
    const quoteMint = instructionAccountKeys[4];
    const poolBaseTokenAccount = instructionAccountKeys[7];
    const poolQuoteTokenAccount = instructionAccountKeys[8];
    if (!baseMint || !quoteMint || baseMint === quoteMint || !poolBaseTokenAccount || !poolQuoteTokenAccount) {
      continue;
    }

    return { poolAddress, wallet, baseMint, quoteMint, poolBaseTokenAccount, poolQuoteTokenAccount };
  }

  return null;
}
