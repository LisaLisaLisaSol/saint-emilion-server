// Saint Emilion — Proxy server with aisstream.io primary + fallbacks
const http  = require('http');
const https = require('https');
const WebSocket = require('ws');

const PORT          = process.env.PORT          || 3000;
const AISSTREAM_KEY = process.env.AISSTREAM_KEY || '';
const VESSELAPI_KEY = process.env.VESSELAPI_KEY || '';
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || '';
const MMSI          = '367399980';

// ── AISstream cached position ─────────────────────────────────────────────────
let aisCache = null;          // { lat, lng, sog, cog, nav, updated, ts }
let aisConnected = false;
let aisWs = null;
let aisReconnectTimer = null;

const NAV = {
  0:'Underway',1:'At Anchor',2:'Not Under Command',3:'Restricted',
  5:'Moored',7:'Fishing',8:'Sailing',15:'Unknown'
};

function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// ── AISstream persistent connection ──────────────────────────────────────────
function connectAisstream() {
  if (!AISSTREAM_KEY) {
    console.log('No AISSTREAM_KEY — skipping aisstream');
    return;
  }
  if (aisWs) {
    try { aisWs.terminate(); } catch(_) {}
    aisWs = null;
  }

  console.log('Connecting to aisstream.io…');
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
  aisWs = ws;

  ws.on('open', () => {
    aisConnected = true;
    console.log('aisstream connected — subscribing MMSI ' + MMSI);
    ws.send(JSON.stringify({
      Apikey: AISSTREAM_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],  // world bbox — MMSI filter handles the rest
      FiltersShipMMSI: [MMSI],
      FilterMessageTypes: ['PositionReport', 'ShipStaticData']
    }));
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.error || msg.Error) {
        console.error('aisstream error:', msg.error || msg.Error);
        return;
      }

      if (msg.MessageType === 'PositionReport') {
        const pr   = msg.Message.PositionReport;
        const meta = msg.MetaData || {};
        const lat  = pr.Latitude;
        const lng  = pr.Longitude;

        // Guard against invalid positions
        if (!lat || !lng || lat === 0 || lng === 0) return;

        aisCache = {
          lat:  parseFloat(lat.toFixed(5)),
          lng:  parseFloat(lng.toFixed(5)),
          sog:  parseFloat((pr.Sog || 0).toFixed(1)),
          cog:  parseFloat((pr.Cog || 0).toFixed(0)),
          nav:  NAV[pr.NavigationalStatus] || 'Unknown',
          location: `${lat.toFixed(4)}°N ${Math.abs(lng).toFixed(4)}°W`,
          updated: meta.time_utc || new Date().toISOString(),
          ts: Date.now(),
          source: 'aisstream.io'
        };
        aisCache.summary = `Saint Emilion: ${aisCache.location} · ${aisCache.sog} kn · ${aisCache.cog}° · ${aisCache.nav}`;
        console.log('aisstream position:', aisCache.summary);
      }
    } catch(e) {
      console.error('aisstream parse error:', e.message);
    }
  });

  ws.on('close', (code) => {
    aisConnected = false;
    aisWs = null;
    // aisstream periodically drops connections — always reconnect
    const delay = (code === 4001 || code === 4003 || code === 1008) ? 60000 : 8000;
    if (code === 4001 || code === 4003 || code === 1008) {
      console.error('aisstream auth rejected (code', code, ') — check AISSTREAM_KEY');
    } else {
      console.log(`aisstream closed (${code}) — reconnecting in ${delay/1000}s`);
    }
    aisReconnectTimer = setTimeout(connectAisstream, delay);
  });

  ws.on('error', (err) => {
    console.error('aisstream ws error:', err.message);
    aisConnected = false;
  });
}

// ── VesselAPI quota state ────────────────────────────────────────────────────
let vesselApiKilled = false;
let vesselApiRetryAfter = 0;

// ── HTTP Server ──────────────────────────────────────────────────────────────
const httpServer = http.createServer(async (req, res) => {
  setCORS(res);

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (req.method === 'GET') {
    const status = {
      aisstream: aisConnected ? 'connected' : 'disconnected',
      aisCacheAge: aisCache ? Math.round((Date.now() - aisCache.ts) / 1000) + 's ago' : 'none',
      vesselApiKilled,
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(status));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/fetch') {
    res.writeHead(404); res.end('Not found'); return;
  }

  console.log('Fetch request received');

  try {
    // ── 1. aisstream cache (best — real-time, free) ──────────────────────────
    // If connected but no cache yet, wait up to 12s for first message
    if (aisConnected && !aisCache) {
      console.log('aisstream connected but no data yet — waiting up to 12s…');
      const waited = await new Promise(resolve => {
        const deadline = Date.now() + 12000;
        const check = setInterval(() => {
          if (aisCache || Date.now() >= deadline) {
            clearInterval(check);
            resolve(!!aisCache);
          }
        }, 500);
      });
      if (waited) console.log('aisstream data arrived during wait');
      else console.log('aisstream wait timed out — trying fallbacks');
    }

    if (aisCache && (Date.now() - aisCache.ts) < 600000) {  // within 10 min
      console.log('Serving from aisstream cache, age:', Math.round((Date.now()-aisCache.ts)/1000)+'s');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(aisCache) }] }));
      return;
    }

    // ── 2. VesselAPI ─────────────────────────────────────────────────────────
    if (VESSELAPI_KEY && !vesselApiKilled && Date.now() >= vesselApiRetryAfter) {
      console.log('Trying VesselAPI…');
      const url = `https://api.vesselapi.com/v1/vessel/${MMSI}/position?filter.idType=mmsi`;
      const r = await new Promise((resolve, reject) => {
        https.get(url, {
          headers: { 'Authorization': `Bearer ${VESSELAPI_KEY}`, 'User-Agent': 'SaintEmilionTracker/1.0' }
        }, (res) => {
          let data = '';
          res.on('data', c => data += c);
          res.on('end', () => resolve({ status: res.statusCode, body: data }));
        }).on('error', reject);
      });

      if (r.status === 200) {
        const d = JSON.parse(r.body);
        const v = d.vesselPosition || d;
        const lat = v.latitude || v.lat;
        const lng = v.longitude || v.lng || v.lon;
        if (lat && lng) {
          const result = {
            lat: parseFloat(lat), lng: parseFloat(lng),
            sog: parseFloat(v.sog || v.speed || 0),
            cog: parseFloat(v.cog || v.course || 0),
            nav: NAV[v.navigationalStatus] || 'Underway',
            location: `${parseFloat(lat).toFixed(4)}°N ${Math.abs(parseFloat(lng)).toFixed(4)}°W`,
            updated: v.timestamp || new Date().toISOString(),
            source: 'VesselAPI'
          };
          result.summary = `Saint Emilion: ${result.location} · ${result.sog} kn`;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(result) }] }));
          return;
        }
      }
      if (r.status === 429 || r.status === 403) {
        vesselApiRetryAfter = Date.now() + 3600000;
        console.log('VesselAPI quota hit — cooling down 1h');
      }
    }

    // ── 3. Anthropic web search fallback ─────────────────────────────────────
    if (ANTHROPIC_KEY && false) {  // disabled — aisstream is primary, Anthropic credits not needed
      console.log('Falling back to Anthropic web search…');
      const payload = JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1000,
        tools: [{ type: 'web_search_20250305', name: 'web_search' }],
        messages: [{
          role: 'user',
          content: `Use web search to find the current AIS position of tugboat SAINT EMILION MMSI 367399980 IMO 8741832 operated by Poling-Cutler Marine on the Hudson River. Try these searches in order: 1) fetch https://www.vesseltracker.com/en/Ships/Saint-Emilion-9353333.html 2) search "MMSI 367399980 position" 3) search "saint emilion poling cutler hudson river". Find actual decimal latitude and longitude coordinates. Return ONLY valid JSON, no markdown: {"lat":40.0000,"lng":-74.0000,"sog":0.0,"cog":0,"nav":"Underway","location":"description","updated":"ISO timestamp","summary":"one sentence","source":"website name"} or {"error":"not found","summary":"what was tried"}`
        }]
      });
      const result = await new Promise((resolve, reject) => {
        const options = {
          hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': ANTHROPIC_KEY,
            'anthropic-version': '2023-06-01',
            'Content-Length': Buffer.byteLength(payload)
          }
        };
        const proxyReq = https.request(options, (proxyRes) => {
          let data = '';
          proxyRes.on('data', c => data += c);
          proxyRes.on('end', () => resolve({ status: proxyRes.statusCode, body: data }));
        });
        proxyReq.on('error', reject);
        proxyReq.write(payload);
        proxyReq.end();
      });
      res.writeHead(result.status, { 'Content-Type': 'application/json' });
      res.end(result.body);
      return;
    }

    // All sources exhausted — return a clean "waiting" response
    const waitMsg = aisConnected
      ? 'Connected to AIS network — waiting for vessel broadcast'
      : 'Position not available — AIS not connected';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ content: [{ type: 'text', text: JSON.stringify({ error: 'waiting', summary: waitMsg }) }] }));

  } catch(e) {
    console.error('Error:', e.message);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
  }
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  console.log('AISSTREAM_KEY:', AISSTREAM_KEY ? '✓ set' : '✗ missing');
  console.log('VESSELAPI_KEY:', VESSELAPI_KEY ? '✓ set' : '✗ missing');
  console.log('ANTHROPIC_KEY:', ANTHROPIC_KEY ? '✓ set' : '✗ missing');
  // Start aisstream connection
  connectAisstream();
});
