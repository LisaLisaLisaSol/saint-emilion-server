// Saint Emilion — VesselAPI proxy
const http = require('http');
const https = require('https');

const PORT = process.env.PORT || 3000;
const VESSELAPI_KEY = process.env.VESSELAPI_KEY || '';
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || '';
const MMSI = '367399980';

const NAV = {
  0:'Underway',1:'At Anchor',2:'Not Under Command',3:'Restricted',
  5:'Moored',7:'Fishing',8:'Sailing',15:'Unknown'
};

function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'SaintEmilionTracker/1.0' } }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    }).on('error', reject);
  });
}

const httpServer = http.createServer(async (req, res) => {
  setCORS(res);

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Saint Emilion proxy OK'); return;
  }

  if (req.method !== 'POST' || req.url !== '/fetch') {
    res.writeHead(404); res.end('Not found'); return;
  }

  console.log('Fetch request received');

  try {
    // ── Try VesselAPI first ──────────────────────────────
    if (VESSELAPI_KEY) {
      console.log('Trying VesselAPI...');
      // Correct VesselAPI endpoint: /v1/vessel/{id}/position?filter.idType=mmsi
      const url = `https://api.vesselapi.com/v1/vessel/${MMSI}/position?filter.idType=mmsi`;
      const r = await new Promise((resolve, reject) => {
        https.get(url, {
          headers: {
            'Authorization': `Bearer ${VESSELAPI_KEY}`,
            'User-Agent': 'SaintEmilionTracker/1.0'
          }
        }, (res) => {
          let data = '';
          res.on('data', chunk => data += chunk);
          res.on('end', () => resolve({ status: res.statusCode, body: data }));
        }).on('error', reject);
      });
      console.log('VesselAPI status:', r.status, r.body.slice(0, 200));

      if (r.status === 200) {
        const d = JSON.parse(r.body);
        // VesselAPI response structure
        const lat = d.latitude || d.lat;
        const lng = d.longitude || d.lng || d.lon;
        const sog = d.speed || d.sog || 0;
        const cog = d.course || d.cog || 0;
        const nav = NAV[d.navigationalStatus || d.navStatus || d.nav_status] || d.status || 'Unknown';
        const ts  = d.timestamp || d.updated || new Date().toISOString();

        if (lat && lng) {
          const result = {
            lat: parseFloat(lat),
            lng: parseFloat(lng),
            sog: parseFloat(sog),
            cog: parseFloat(cog),
            nav,
            location: `${parseFloat(lat).toFixed(4)}°N ${Math.abs(parseFloat(lng)).toFixed(4)}°W`,
            updated: ts,
            summary: `Saint Emilion is at ${parseFloat(lat).toFixed(4)}°N ${Math.abs(parseFloat(lng)).toFixed(4)}°W making ${parseFloat(sog).toFixed(1)} kn on course ${parseFloat(cog).toFixed(0)}°.`,
            source: 'VesselAPI'
          };
          console.log('VesselAPI success:', result);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(result) }] }));
          return;
        }
      }

      // Log non-200 for debugging
      if (r.status !== 200) {
        console.log('VesselAPI failed:', r.status, r.body.slice(0, 300));
      }
    }

    // ── Fallback to Anthropic web search ─────────────────
    if (ANTHROPIC_KEY) {
      console.log('Falling back to Anthropic search...');
      const payload = JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1000,
        tools: [{ type: 'web_search_20250305', name: 'web_search' }],
        messages: [{
          role: 'user',
          content: `Fetch https://www.myshiptracking.com/vessels/saint-emilion-mmsi-367399980-imo-8741832 and extract the current position of SAINT EMILION MMSI 367399980. Return ONLY this JSON: {"lat":0.0,"lng":0.0,"sog":0.0,"cog":0,"nav":"status","location":"description","updated":"time","summary":"one sentence"} or {"error":"not found","summary":"explanation"}`
        }]
      });

      const result = await new Promise((resolve, reject) => {
        const options = {
          hostname: 'api.anthropic.com',
          path: '/v1/messages',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': ANTHROPIC_KEY,
            'anthropic-version': '2023-06-01',
            'Content-Length': Buffer.byteLength(payload)
          }
        };
        const proxyReq = https.request(options, (proxyRes) => {
          let data = '';
          proxyRes.on('data', chunk => data += chunk);
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

    // No keys configured
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'No API keys configured' }));

  } catch (e) {
    console.error('Error:', e.message);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
  }
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  console.log('VesselAPI key:', VESSELAPI_KEY ? '✓ set' : '✗ missing');
  console.log('Anthropic key:', ANTHROPIC_KEY ? '✓ set' : '✗ missing');
});
