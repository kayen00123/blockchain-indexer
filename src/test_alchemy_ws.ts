import WebSocket from 'ws';

const endpoint = process.argv[2] ?? 'wss://rpc.shyft.to?api_key=iAbHuOpwyYZ5Wze1';
const pumpAmmProgram = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const accountToTest = 'So11111111111111111111111111111111111111112';
const timeoutMs = 15000;

function testSubscription(
  name: string,
  method: string,
  params: unknown[],
): Promise<void> {
  return new Promise((resolve) => {
    const socket = new WebSocket(endpoint);
    const startedAt = Date.now();
    let settled = false;

    const finish = (message: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      console.log(`[${name}] ${message}`);
      socket.close();
      resolve();
    };

    const timer = setTimeout(() => finish(`TIMEOUT after ${timeoutMs}ms`), timeoutMs);

    socket.on('open', () => {
      console.log(`[${name}] OPEN`);
      socket.send(JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method,
        params,
      }));
    });

    socket.on('message', (raw) => {
      let payload: any;
      try {
        payload = JSON.parse(raw.toString());
      } catch {
        finish('INVALID_JSON');
        return;
      }

      if (payload.error) {
        finish(`ERROR ${JSON.stringify(payload.error)}`);
        return;
      }

      if (payload.id === 1 && payload.result !== undefined) {
        console.log(`[${name}] ACCEPTED subscription=${payload.result}`);
        return;
      }

      if (payload.method?.endsWith('Notification')) {
        const elapsed = Date.now() - startedAt;
        finish(`NOTIFICATION after ${elapsed}ms ${JSON.stringify(payload).slice(0, 600)}`);
      }
    });

    socket.on('error', (error) => finish(`WS_ERROR ${String(error)}`));
    socket.on('close', (code, reason) => {
      if (!settled) finish(`CLOSED code=${code} reason=${reason.toString()}`);
    });
  });
}

console.log(`Testing Alchemy websocket: ${new URL(endpoint).origin}`);
await testSubscription('blockSubscribe', 'blockSubscribe', [
  { mentionsAccountOrProgram: pumpAmmProgram },
  {
    commitment: 'confirmed',
    encoding: 'jsonParsed',
    transactionDetails: 'none',
    maxSupportedTransactionVersion: 0,
  },
]);

await testSubscription('accountSubscribe', 'accountSubscribe', [
  accountToTest,
  { commitment: 'confirmed', encoding: 'jsonParsed' },
]);
