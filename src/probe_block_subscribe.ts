import WebSocket from 'ws';

const endpoints = [
  {
    name: 'quicknode',
    wss: 'wss://sleek-withered-leaf.solana-mainnet.quiknode.pro/610a91d4320d470bde140d9b15662902dfbc6ad1/',
    http: 'https://sleek-withered-leaf.solana-mainnet.quiknode.pro/610a91d4320d470bde140d9b15662902dfbc6ad1/'
  },
  {
    name: 'publicnode',
    wss: 'wss://solana-rpc.publicnode.com',
    http: 'https://solana-rpc.publicnode.com'
  },
  {
    name: 'chainstack-54ce',
    wss: 'wss://solana-mainnet.core.chainstack.com/54ce8267c02c230db8cf40ae8c432e1e',
    http: 'https://solana-mainnet.core.chainstack.com/54ce8267c02c230db8cf40ae8c432e1e'
  },
  {
    name: 'chainstack-d367',
    wss: 'wss://solana-mainnet.core.chainstack.com/d367c1187485443d0f826f06ff52c072',
    http: 'https://solana-mainnet.core.chainstack.com/d367c1187485443d0f826f06ff52c072'
  },
  {
    name: 'public-mainnet',
    wss: 'wss://api.mainnet-beta.solana.com',
    http: 'https://api.mainnet-beta.solana.com'
  }
];

function probeEndpoint(entry: { name: string; wss: string; http: string }) {
  return new Promise<void>((resolve) => {
    const socket = new WebSocket(entry.wss);
    const startedAt = Date.now();

    const timeout = setTimeout(() => {
      console.log(`[${entry.name}] TIMEOUT`);
      socket.close();
      resolve();
    }, 12000);

    socket.on('open', () => {
      console.log(`[${entry.name}] OPEN`);

      const subscribeMessage = JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'blockSubscribe',
        params: [
          'all',
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed'
          }
        ]
      });

      socket.send(subscribeMessage);
    });

    socket.on('message', (raw) => {
      const text = raw.toString();
      const data = JSON.parse(text);

      if (data.result && data.result.subscription) {
        console.log(`[${entry.name}] BLOCK_SUBSCRIBE_SUPPORTED`);
        console.log(`[${entry.name}] subscription=${data.result.subscription}`);
        clearTimeout(timeout);
        socket.close();
        resolve();
        return;
      }

      if (data.error) {
        console.log(`[${entry.name}] ERROR ${JSON.stringify(data.error)}`);
        clearTimeout(timeout);
        socket.close();
        resolve();
        return;
      }

      console.log(`[${entry.name}] MESSAGE ${text.slice(0, 150)}`);
      clearTimeout(timeout);
      socket.close();
      resolve();
    });

    socket.on('error', (err) => {
      console.log(`[${entry.name}] WS_ERROR ${String(err)}`);
      clearTimeout(timeout);
      socket.close();
      resolve();
    });

    socket.on('close', (code, reason) => {
      const elapsed = Date.now() - startedAt;
      console.log(`[${entry.name}] CLOSE code=${code} reason=${reason.toString()} elapsed=${elapsed}ms`);
    });
  });
}

async function main() {
  console.log('Testing Solana websocket block subscription support...');

  for (const entry of endpoints) {
    console.log(`\n=== ${entry.name} ===`);
    await probeEndpoint(entry);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
