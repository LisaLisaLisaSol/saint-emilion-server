const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const AIS_KEY = process.env.AIS_KEY || '';
const BOUNDING_BOX = [[39.5, -75.5], [42.5, -71.5]];

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Saint Emilion AIS Relay OK');
});

const wss = new WebSocketServer({ server: httpServer });
let aisWs = null;
let reconnectTimer = null;
let isConnecting = false;
let browserClients = new Set();

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const c of browserClients)
    if (c.readyState === WebSocket.OPEN) c.send(msg);
}

// Build subscription as a Buffer so it's ready to send the instant socket opens
const SUBSCRIPTION = Buffer.from(JSON.stringify({
  Apikey: AIS_KEY,
  BoundingBoxes: [BOUNDING_BOX],
  FilterMessageTypes: ['PositionReport']
}));

function connectToAIS() {
  if (isConnecting) return;
  if (aisWs && aisWs.readyState === WebSocket.OPEN) return;
  if (!AIS_KEY) { console.error('No AIS_KEY'); return; }

  isConnecting = true;
  console.log('Connecting to aisstream.io...');

  aisWs = new WebSocket('wss://stream.aisstream.io/v0/stream');

  // Send subscription IMMEDIATELY on open — before any other async work
  aisWs.on('open', () => {
    // Send synchronously with no delay — must arrive within 3s per aisstream docs
    aisWs.send(SUBSCRIPTION, (err) => {
      if (err) {
        console.error('Subscription send failed:', err.message);
      } else {
        console.log('Subscription sent ✓');
        isConnecting = false;
        broadcast({ type: 'status', status: 'connected' });
      }
    });
  });

  aisWs.on('message', (data) => {
    broadcast({ type: 'ais', data: data.toString() });
  });

  aisWs.on('close', (code) => {
    isConnecting = false;
    aisWs = null;
    console.log(`aisstream disconnected (${code})`);
    broadcast({ type: 'status', status: 'reconnecting' });
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectToAIS();
      }, 8000);
    }
  });

  aisWs.on('error', (err) => {
    isConnecting = false;
    console.error('aisstream error:', err.message);
  });
}

wss.on('connection', (client) => {
  browserClients.add(client);
  console.log(`Client connected (${browserClients.size} total)`);
  const status = aisWs && aisWs.readyState === WebSocket.OPEN ? 'connected' : 'reconnecting';
  client.send(JSON.stringify({ type: 'status', status }));
  client.on('close', () => { browserClients.delete(client); });
  client.on('error', () => { browserClients.delete(client); });
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  connectToAIS();
});
