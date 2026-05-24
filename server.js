// Saint Emilion AIS Relay Server
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const AIS_KEY = process.env.AIS_KEY || '';
const BOUNDING_BOX = [[39.5, -75.5], [42.5, -71.5]];

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Saint Emilion AIS Relay — OK');
});

const wss = new WebSocketServer({ server: httpServer });

let aisWs = null;
let reconnectTimer = null;
let isConnecting = false;
let browserClients = new Set();

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of browserClients) {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  }
}

function connectToAIS() {
  // Prevent duplicate connections
  if (isConnecting || (aisWs && aisWs.readyState === WebSocket.OPEN)) {
    console.log('Already connected or connecting — skipping');
    return;
  }
  if (!AIS_KEY) { console.error('No AIS_KEY set'); return; }

  isConnecting = true;
  console.log('Connecting to aisstream.io...');

  try {
    aisWs = new WebSocket('wss://stream.aisstream.io/v0/stream', {
      handshakeTimeout: 10000
    });
  } catch(e) {
    console.error('Failed to create WebSocket:', e.message);
    isConnecting = false;
    scheduleReconnect();
    return;
  }

  aisWs.on('open', () => {
    isConnecting = false;
    console.log('Connected to aisstream.io ✓');
    broadcast({ type: 'status', status: 'connected' });

    // Send subscription
    aisWs.send(JSON.stringify({
      Apikey: AIS_KEY,
      BoundingBoxes: [BOUNDING_BOX],
      FilterMessageTypes: ['PositionReport']
    }));

    // Keep-alive ping every 30s to prevent idle timeout
    aisWs._pingInterval = setInterval(() => {
      if (aisWs && aisWs.readyState === WebSocket.OPEN) {
        aisWs.ping();
      }
    }, 30000);
  });

  aisWs.on('message', (data) => {
    const text = data.toString();
    broadcast({ type: 'ais', data: text });
  });

  aisWs.on('ping', () => {
    if (aisWs) aisWs.pong();
  });

  aisWs.on('close', (code, reason) => {
    isConnecting = false;
    if (aisWs && aisWs._pingInterval) {
      clearInterval(aisWs._pingInterval);
    }
    console.log(`aisstream.io disconnected (${code}) ${reason || ''}`);
    broadcast({ type: 'status', status: 'reconnecting' });
    aisWs = null;
    scheduleReconnect();
  });

  aisWs.on('error', (err) => {
    isConnecting = false;
    console.error('aisstream error:', err.message);
    // close handler will fire after this and schedule reconnect
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return; // already scheduled
  console.log('Reconnecting in 8s...');
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToAIS();
  }, 8000);
}

wss.on('connection', (clientWs) => {
  console.log(`Browser client connected (${browserClients.size + 1} total)`);
  browserClients.add(clientWs);

  // Tell client current status
  const status = aisWs && aisWs.readyState === WebSocket.OPEN ? 'connected' : 'reconnecting';
  clientWs.send(JSON.stringify({ type: 'status', status }));

  clientWs.on('close', () => {
    browserClients.delete(clientWs);
    console.log(`Browser client disconnected (${browserClients.size} remaining)`);
  });
  clientWs.on('error', () => browserClients.delete(clientWs));
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  connectToAIS();
});
