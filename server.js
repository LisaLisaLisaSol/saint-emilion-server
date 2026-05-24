// Saint Emilion AIS Relay Server
// Connects to aisstream.io server-side (allowed), forwards to browser clients

const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const AIS_KEY = process.env.AIS_KEY || '';
const BOUNDING_BOX = [[39.5, -75.5], [42.5, -71.5]];

// Simple HTTP server (Railway needs an HTTP port to stay alive)
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Saint Emilion AIS Relay — OK');
});

// WebSocket server that browser clients connect to
const wss = new WebSocketServer({ server: httpServer });

console.log(`Relay server starting on port ${PORT}`);

let aisWs = null;
let browserClients = new Set();
let reconnectTimer = null;

// ── Connect to aisstream.io ──────────────────────────────
function connectToAIS() {
  if (!AIS_KEY) {
    console.error('No AIS_KEY environment variable set!');
    return;
  }

  console.log('Connecting to aisstream.io...');
  aisWs = new WebSocket('wss://stream.aisstream.io/v0/stream');

  aisWs.on('open', () => {
    console.log('Connected to aisstream.io');
    aisWs.send(JSON.stringify({
      Apikey: AIS_KEY,
      BoundingBoxes: [BOUNDING_BOX],
      FilterMessageTypes: ['PositionReport']
    }));
    broadcast({ type: 'status', status: 'connected' });
  });

  aisWs.on('message', (data) => {
    // Forward raw AIS message to all connected browser clients
    const text = data.toString();
    broadcast({ type: 'ais', data: text });
  });

  aisWs.on('close', (code) => {
    console.log(`aisstream.io disconnected (${code}) — retrying in 5s`);
    broadcast({ type: 'status', status: 'reconnecting' });
    reconnectTimer = setTimeout(connectToAIS, 5000);
  });

  aisWs.on('error', (err) => {
    console.error('aisstream error:', err.message);
  });
}

// ── Broadcast to all browser clients ────────────────────
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of browserClients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

// ── Handle browser client connections ───────────────────
wss.on('connection', (clientWs, req) => {
  console.log(`Browser client connected (${browserClients.size + 1} total)`);
  browserClients.add(clientWs);

  // Send current AIS connection status immediately
  const status = aisWs && aisWs.readyState === WebSocket.OPEN ? 'connected' : 'reconnecting';
  clientWs.send(JSON.stringify({ type: 'status', status }));

  clientWs.on('close', () => {
    browserClients.delete(clientWs);
    console.log(`Browser client disconnected (${browserClients.size} remaining)`);
  });

  clientWs.on('error', () => {
    browserClients.delete(clientWs);
  });
});

// ── Start ────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`HTTP + WS server listening on port ${PORT}`);
  connectToAIS();
});
