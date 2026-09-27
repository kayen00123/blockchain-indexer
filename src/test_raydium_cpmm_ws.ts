import WebSocket from 'ws';

const endpoint = process.argv[2] ?? 'wss://solana-mainnet.core.chainstack.com/eaaeb12632a5bd64c67d03521775c491';
const program = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';
const socket = new WebSocket(endpoint);
const timer = setTimeout(() => { console.log('TIMEOUT'); socket.close(); }, 20000);

socket.on('open', () => {
  console.log('OPEN');
  socket.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'blockSubscribe', params: [
    { mentionsAccountOrProgram: program },
    { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 },
  ] }));
});

socket.on('message', (raw) => {
  const payload = JSON.parse(raw.toString());
  if (payload.error) { console.log('ERROR', JSON.stringify(payload.error)); clearTimeout(timer); socket.close(); return; }
  if (payload.id === 1) { console.log('SUBSCRIBED', payload.result); return; }
  if (payload.method === 'blockNotification') {
    const block = payload.params?.result?.value?.block;
    const count = block?.transactions?.length ?? 0;
    console.log('BLOCK', JSON.stringify({ slot: payload.params?.result?.context?.slot, transactions: count }));
    clearTimeout(timer); socket.close();
  }
});

socket.on('error', (error) => { console.log('WS_ERROR', String(error)); clearTimeout(timer); });
socket.on('close', () => console.log('CLOSE'));
