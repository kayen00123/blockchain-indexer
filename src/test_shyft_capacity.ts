import 'dotenv/config';
import WebSocket from 'ws';
import { PublicKey } from '@solana/web3.js';
import { Client } from 'pg';

const endpoint = process.argv[2] ?? process.env.SHYFT_WS_URL;
const requestedPools = Number(process.argv[3] ?? 25);
const timeoutMs = 15000;

if (!endpoint) throw new Error('Set SHYFT_WS_URL or pass a websocket URL as the first argument.');

const client = new Client({ connectionString: process.env.POSTGRES_URL ?? 'postgresql://postgres:postgres@127.0.0.1:54322/postgres' });
await client.connect();
const result = await client.query<{ pool_base_token_account: string; pool_quote_token_account: string }>(
  `SELECT pool_base_token_account, pool_quote_token_account FROM pools
   WHERE pool_base_token_account <> 'unknown' AND pool_quote_token_account <> 'unknown'
   LIMIT $1`,
  [requestedPools],
);
await client.end();

const accounts = [...new Set(result.rows.flatMap((pool) => [pool.pool_base_token_account, pool.pool_quote_token_account]))];
const validAccounts = accounts.filter((account) => {
  try {
    new PublicKey(account);
    return true;
  } catch {
    return false;
  }
});
if (validAccounts.length === 0) {
  console.log(JSON.stringify({ requestedPools, accounts: 0, skippedInvalidAccounts: accounts.length, acceptedSubscriptions: 0, errors: [], timedOut: false, message: 'No valid promoted-pool vault accounts found.' }, null, 2));
  process.exit(0);
}
const socket = new WebSocket(endpoint);
const accepted = new Set<number>();
const errors: unknown[] = [];
const startedAt = Date.now();
const timer = setTimeout(() => {
  console.log(JSON.stringify({ requestedPools, accounts: validAccounts.length, skippedInvalidAccounts: accounts.length - validAccounts.length, acceptedSubscriptions: accepted.size, errors, timedOut: true, elapsedMs: Date.now() - startedAt }, null, 2));
  socket.close();
}, timeoutMs);

socket.on('open', () => {
  validAccounts.forEach((account, index) => socket.send(JSON.stringify({
    jsonrpc: '2.0', id: index + 1, method: 'accountSubscribe', params: [account, { commitment: 'confirmed', encoding: 'base64' }],
  })));
});

socket.on('message', (raw) => {
  const payload = JSON.parse(raw.toString());
  if (payload.error) errors.push(payload.error);
  if (typeof payload.id === 'number' && typeof payload.result === 'number') accepted.add(payload.id);
  if (accepted.size + errors.length >= validAccounts.length) {
    clearTimeout(timer);
    console.log(JSON.stringify({ requestedPools, accounts: validAccounts.length, skippedInvalidAccounts: accounts.length - validAccounts.length, acceptedSubscriptions: accepted.size, errors, timedOut: false, elapsedMs: Date.now() - startedAt }, null, 2));
    socket.close();
  }
});

socket.on('error', (error) => {
  clearTimeout(timer);
  console.error(error);
  process.exitCode = 1;
});
