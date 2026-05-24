const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const AIS_KEY = process.env.AIS_KEY || '';

// Only Saint Emilion — tiny data volume, won't overwhelm the queue
const SUBSCRIPTION = {
  Apikey: AIS_KEY,
  BoundingBoxes: [[[-90, -180], [90, 180]]], // world box but filtered by MMSI
  FiltersShipMMSI: ['367399980'],
  FilterMessageTypes: ['PositionReport', 'ShipStaticData']
};

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

function connectToAIS() {
  if (isConnecting) return;
  if (aisWs && aisWs.readyState === WebSocket.OPEN) return;
  if (!AIS_KEY) { console.error('No AIS_KEY'); return; }

  isConnecting = true;
  console.log('Connecting to aisstream.io...');

  aisWs = new WebSocket('wss://stream.aisstream.io/v0/stream');

  aisWs.on('open', () => {
    // Send subscription immediately — must be within 3 seconds
    const subMsg = JSON.stringify(SUBSCRIPTION);
    aisWs.send(subMsg, (err) => {
      if (err) {
        console.error('Subscription send failed:', err.message);
      } else {
        isConnecting = false;
        console.log('Subscribed to MMSI 367399980 (Saint Emilion only)');
        broadcast({ type: 'status', status: 'connected' });
      }
    });
  });

  aisWs.on('message', (data) => {
    const text = data.toString();
    console.log('AIS message received:', text.slice(0, 80));
    broadcast({ type: 'ais', data: text });
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
  console.log(`Browser client connected (${browserClients.size} total)`);
  const status = aisWs && aisWs.readyState === WebSocket.OPEN ? 'connected' : 'reconnecting';
  client.send(JSON.stringify({ type: 'status', status }));
  client.on('close', () => browserClients.delete(client));
  client.on('error', () => browserClients.delete(client));
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  connectToAIS();
});
