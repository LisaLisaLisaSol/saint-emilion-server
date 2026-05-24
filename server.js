
// Saint Emilion — Anthropic API proxy
// Simple HTTP server that forwards requests to Anthropic API
// No WebSockets, no AIS — just a secure API proxy
 
const http = require('http');
const https = require('https');
 
const PORT = process.env.PORT || 3000;
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || '';
 
const httpServer = http.createServer((req, res) => {
 
  // CORS headers so the browser can call this from GitHub Pages
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
 
  // Handle preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204); res.end(); return;
  }
 
  // Health check
  if (req.method === 'GET') {
    res.writeHead(200, {'Content-Type':'text/plain'});
    res.end('Saint Emilion proxy OK'); return;
  }
 
  // Only allow POST to /fetch
  if (req.method !== 'POST' || req.url !== '/fetch') {
    res.writeHead(404); res.end('Not found'); return;
  }
 
  if (!ANTHROPIC_KEY) {
    res.writeHead(500, {'Content-Type':'application/json'});
    res.end(JSON.stringify({error:'No ANTHROPIC_KEY set on server'})); return;
  }
 
  // Read request body
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    // Forward to Anthropic
    const payload = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      tools: [{type: 'web_search_20250305', name: 'web_search'}],
      messages: [{
        role: 'user',
        content: `Fetch this exact URL and extract the vessel position data from it:
https://www.myshiptracking.com/vessels/saint-emilion-mmsi-367399980-imo-8741832
 
Also try:
https://shipfinder.co/about/ship/?mmsi=367399980
https://www.marinetraffic.com/en/ais/details/ships/mmsi:367399980
 
Look for: latitude, longitude, speed (SOG), course (COG), navigational status, and last update time for SAINT EMILION MMSI 367399980.
 
Today's date is ${new Date().toLocaleDateString()}. The vessel is actively underway right now so the data should be very recent.
 
Respond ONLY with this exact JSON, no markdown, no explanation:
{"lat":40.78,"lng":-74.01,"sog":7.0,"cog":359,"nav":"Underway","location":"current location description","updated":"timestamp","summary":"one sentence"}
 
If you truly cannot get coordinates respond ONLY with:
{"error":"not found","summary":"exact explanation of what each site showed"}`
      }]
    });
 
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
      proxyRes.on('end', () => {
        res.writeHead(proxyRes.statusCode, {'Content-Type':'application/json'});
        res.end(data);
      });
    });
 
    proxyReq.on('error', (e) => {
      console.error('Anthropic request error:', e.message);
      res.writeHead(502, {'Content-Type':'application/json'});
      res.end(JSON.stringify({error: e.message}));
    });
 
    proxyReq.write(payload);
    proxyReq.end();
  });
});
 
httpServer.listen(PORT, () => {
  console.log(`Proxy server listening on port ${PORT}`);
  if (!ANTHROPIC_KEY) console.warn('WARNING: ANTHROPIC_KEY not set');
});
