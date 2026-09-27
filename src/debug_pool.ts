import 'dotenv/config';

const address = process.argv[2] ?? process.env.DEBUG_MINT_ADDRESS;
const endpoint = process.argv[3] ?? process.env.METAPLEX_RPC_URL;

if (!address) {
  throw new Error('Usage: npx tsx src/debug_pool.ts <mint-or-asset-address> [das-http-url]');
}

if (!endpoint) {
  throw new Error('Provide a DAS HTTP URL as the second argument or set METAPLEX_RPC_URL.');
}

const request = {
  jsonrpc: '2.0',
  id: 'indexer-request',
  method: 'getAsset',
  params: { id: address },
};

console.log('endpoint:', new URL(endpoint).origin);
console.log('asset:', address);
console.log('request:', JSON.stringify(request));

try {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });

  const body = await response.text();
  let parsed: unknown = body;
  try {
    parsed = JSON.parse(body);
  } catch {
    // Keep non-JSON provider responses visible.
  }

  console.log('http_status:', response.status);
  console.log(JSON.stringify(parsed, null, 2));
} catch (error) {
  console.error('request_failed:', error);
  process.exitCode = 1;
}
