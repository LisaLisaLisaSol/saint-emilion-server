// Saint Emilion — Relay server
// Source priority:
//   1. MyShipTracking API (primary for first 10 days — 1 credit/call)
//   2. aisstream.io cache (free WebSocket, passive — serves if fresh)
//   3. MyShipTracking page scrape (after API expires, every 15min max)
//   4. VesselAPI (fallback, quota-limited)

const http      = require('http');
const https     = require('https');
const WebSocket = require('ws');

const PORT             = process.env.PORT             || 3000;
const AISSTREAM_KEY    = process.env.AISSTREAM_KEY    || '';
const VESSELAPI_KEY    = process.env.VESSELAPI_KEY    || '';
const MYSHIPTRACK_KEY  = process.env.MYSHIPTRACK_KEY  || '';
const MMSI             = '367399980';

// ── API key install date — used to switch from API to scrape after 10 days ──
// Set this to when you added the MYSHIPTRACK_KEY to Render (ISO date string)
const API_START_DATE   = process.env.API_START_DATE || '2026-05-25';
const API_EXPIRY_MS    = 10 * 24 * 60 * 60 * 1000; // 10 days

function apiIsActive() {
  if (!MYSHIPTRACK_KEY) return false;
  const start = new Date(API_START_DATE).getTime();
  return (Date.now() - start) < API_EXPIRY_MS;
}

const NAV_STATUS = {
  0:'Underway', 1:'At Anchor', 2:'Not Under Command', 3:'Restricted Manoeuvrability',
  5:'Moored', 7:'Fishing', 8:'Sailing', 15:'Unknown'
};

function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function respond(res, data) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(data) }] }));
}

// ── aisstream passive WebSocket cache ────────────────────────────────────────
let aisCache     = null;
let aisConnected = false;
let aisWs        = null;

function connectAisstream() {
  if (!AISSTREAM_KEY) return;
  if (aisWs) { try { aisWs.terminate(); } catch(_) {} aisWs = null; }

  console.log('Connecting to aisstream.io…');
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
  aisWs = ws;

  ws.on('open', () => {
    aisConnected = true;
    console.log('aisstream connected — subscribing MMSI ' + MMSI);
    ws.send(JSON.stringify({
      Apikey: AISSTREAM_KEY,
      BoundingBoxes: [[[-90,-180],[90,180]]],
      FiltersShipMMSI: [MMSI],
      FilterMessageTypes: ['PositionReport']
    }));
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.error || msg.Error) return;
      if (msg.MessageType === 'PositionReport') {
        const pr  = msg.Message.PositionReport;
        const lat = pr.Latitude, lng = pr.Longitude;
        if (!lat || !lng || lat === 0 || lng === 0) return;
        aisCache = {
          lat:      parseFloat(lat.toFixed(5)),
          lng:      parseFloat(lng.toFixed(5)),
          sog:      parseFloat((pr.Sog || 0).toFixed(1)),
          cog:      parseFloat((pr.Cog || 0).toFixed(0)),
          nav:      NAV_STATUS[pr.NavigationalStatus] || 'Unknown',
          location: `${lat.toFixed(4)}°N ${Math.abs(lng).toFixed(4)}°W`,
          updated:  (msg.MetaData || {}).time_utc || new Date().toISOString(),
          ts:       Date.now(),
          source:   'aisstream.io'
        };
        aisCache.summary = `Saint Emilion: ${aisCache.location} · ${aisCache.sog} kn · ${aisCache.nav}`;
        console.log('aisstream position:', aisCache.summary);
      }
    } catch(e) { console.error('aisstream parse error:', e.message); }
  });

  ws.on('close', (code) => {
    aisConnected = false; aisWs = null;
    const delay = (code === 4001 || code === 4003) ? 60000 : 8000;
    if (code === 4001 || code === 4003) console.error('aisstream auth rejected — check key');
    else console.log(`aisstream closed (${code}) — reconnecting in ${delay/1000}s`);
    setTimeout(connectAisstream, delay);
  });

  ws.on('error', (err) => { console.error('aisstream error:', err.message); aisConnected = false; });
}

// ── MyShipTracking API ────────────────────────────────────────────────────────
async function fetchMyShipTrackingAPI() {
  if (!MYSHIPTRACK_KEY) return null;
  console.log('Fetching MyShipTracking API…');
  try {
    const r = await httpGet(
      `https://api.myshiptracking.com/api/v2/vessel?mmsi=${MMSI}`,
      { 'Authorization': `Bearer ${MYSHIPTRACK_KEY}`, 'User-Agent': 'SaintEmilionTracker/1.0' }
    );
    if (r.status !== 200) {
      console.log('MyShipTracking API error:', r.status, r.body.slice(0,100));
      return null;
    }
    const d = JSON.parse(r.body);
    if (d.status !== 'success' || !d.data || !d.data.lat) return null;
    const v = d.data;
    const result = {
      lat:      parseFloat(v.lat),
      lng:      parseFloat(v.lng),
      sog:      parseFloat(v.speed || 0),
      cog:      parseFloat(v.course || 0),
      nav:      parseNavStatus(v.nav_status),
      location: `${parseFloat(v.lat).toFixed(4)}°N ${Math.abs(parseFloat(v.lng)).toFixed(4)}°W`,
      updated:  v.received || new Date().toISOString(),
      ts:       Date.now(),
      source:   'MyShipTracking API'
    };
    result.summary = `Saint Emilion: ${result.location} · ${result.sog} kn · ${result.nav}`;
    console.log('MyShipTracking API result:', result.summary);
    return result;
  } catch(e) {
    console.error('MyShipTracking API fetch error:', e.message);
    return null;
  }
}

// ── MyShipTracking page scrape — two URLs, alternates to spread load ──────────
let scrapeUrlIndex      = 0;
let lastScrapeTs        = 0;
const SCRAPE_MIN_GAP_MS = 14 * 60 * 1000; // 14 min minimum between scrapes
const SCRAPE_URLS = [
  `https://www.myshiptracking.com/?mmsi=${MMSI}`,
  `https://www.myshiptracking.com/vessels/saint-emilion-mmsi-${MMSI}-imo-8741832`
];
const SCRAPE_USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
];

async function fetchMyShipTrackingScrape() {
  const now = Date.now();
  if ((now - lastScrapeTs) < SCRAPE_MIN_GAP_MS) {
    console.log('Scrape throttled — too soon since last scrape');
    return null;
  }
  const url = SCRAPE_URLS[scrapeUrlIndex % SCRAPE_URLS.length];
  const ua  = SCRAPE_USER_AGENTS[Math.floor(Math.random() * SCRAPE_USER_AGENTS.length)];
  scrapeUrlIndex++;
  console.log(`Scraping ${url} …`);

  try {
    const r = await httpGet(url, {
      'User-Agent': ua,
      'Accept': 'text/html,application/xhtml+xml',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cache-Control': 'no-cache'
    });

    if (r.status !== 200) {
      console.log('Scrape HTTP error:', r.status);
      return null;
    }

    lastScrapeTs = Date.now();
    const html = r.body;

    // Pattern 1: meta description "coordinates 42.04046° / -73.93031° and speed 7.7 knots"
    const metaCoord = html.match(/coordinates\s+([\d.-]+)°\s*\/\s*([\d.-]+)°\s*and speed\s+([\d.]+)\s*knots\s*as reported on\s*([\d\-: ]+)/i);
    if (metaCoord) {
      const lat = parseFloat(metaCoord[1]);
      const lng = parseFloat(metaCoord[2]);
      const sog = parseFloat(metaCoord[3]);
      const updated = metaCoord[4].trim();
      if (lat && lng) {
        const result = {
          lat, lng, sog, cog: 0, nav: 'Underway',
          location: `${lat.toFixed(4)}°N ${Math.abs(lng).toFixed(4)}°W`,
          updated, ts: Date.now(), source: 'myshiptracking.com'
        };
        result.summary = `Saint Emilion: ${result.location} · ${sog} kn (scraped ${updated})`;
        console.log('Scrape result:', result.summary);
        return result;
      }
    }

    // Pattern 2: body text "coordinates 42.04046° / -73.93031°" (vessel page)
    const bodyCoord = html.match(/coordinates\s*<\/?\w*>\s*<\/?\w*[^>]*>\s*([\d.-]+)°\s*\/\s*([\d.-]+)°/i)
                   || html.match(/\*\*([\d.-]+)°\s*\/\s*([\d.-]+)°\*\*/);
    if (bodyCoord) {
      const lat = parseFloat(bodyCoord[1]);
      const lng = parseFloat(bodyCoord[2]);
      // Try to get speed from nearby text
      const speedMatch = html.match(/speed is\s*<[^>]*>\s*([\d.]+)\s*Knots/i)
                      || html.match(/Speed\s*[|:]\s*([\d.]+)\s*Knots/i);
      const sog = speedMatch ? parseFloat(speedMatch[1]) : 0;
      if (lat && lng) {
        const result = {
          lat, lng, sog, cog: 0, nav: 'Underway',
          location: `${lat.toFixed(4)}°N ${Math.abs(lng).toFixed(4)}°W`,
          updated: new Date().toISOString(),
          ts: Date.now(), source: 'myshiptracking.com'
        };
        result.summary = `Saint Emilion: ${result.location} · ${sog} kn`;
        console.log('Scrape result (body):', result.summary);
        return result;
      }
    }

    console.log('Scrape: could not parse coordinates from page');
    return null;
  } catch(e) {
    console.error('Scrape error:', e.message);
    return null;
  }
}

// ── VesselAPI fallback ────────────────────────────────────────────────────────
let vesselApiRetryAfter = 0;
async function fetchVesselAPI() {
  if (!VESSELAPI_KEY || Date.now() < vesselApiRetryAfter) return null;
  console.log('Trying VesselAPI…');
  try {
    const r = await httpGet(
      `https://api.vesselapi.com/v1/vessel/${MMSI}/position?filter.idType=mmsi`,
      { 'Authorization': `Bearer ${VESSELAPI_KEY}`, 'User-Agent': 'SaintEmilionTracker/1.0' }
    );
    if (r.status === 429 || r.status === 403) {
      vesselApiRetryAfter = Date.now() + 3600000;
      console.log('VesselAPI quota hit — cooling 1h');
      return null;
    }
    if (r.status !== 200) return null;
    const d = JSON.parse(r.body);
    const v = d.vesselPosition || d;
    const lat = v.latitude || v.lat;
    const lng = v.longitude || v.lng || v.lon;
    if (!lat || !lng) return null;
    const result = {
      lat: parseFloat(lat), lng: parseFloat(lng),
      sog: parseFloat(v.sog || v.speed || 0),
      cog: parseFloat(v.cog || v.course || 0),
      nav: NAV_STATUS[v.navigationalStatus] || 'Underway',
      location: `${parseFloat(lat).toFixed(4)}°N ${Math.abs(parseFloat(lng)).toFixed(4)}°W`,
      updated: v.timestamp || new Date().toISOString(),
      ts: Date.now(), source: 'VesselAPI'
    };
    result.summary = `Saint Emilion: ${result.location} · ${result.sog} kn`;
    return result;
  } catch(e) {
    console.error('VesselAPI error:', e.message);
    return null;
  }
}

// ── Nav status normalizer ────────────────────────────────────────────────────
function parseNavStatus(raw) {
  if (raw === null || raw === undefined) return 'Unknown';
  if (typeof raw === 'number') return NAV_STATUS[raw] || 'Unknown';
  // MyShipTracking returns strings like "Under way using engine", "Moored", etc.
  const s = String(raw).toLowerCase();
  if (s.includes('under way') || s.includes('underway') || s.includes('engine')) return 'Underway';
  if (s.includes('anchor')) return 'At Anchor';
  if (s.includes('moor')) return 'Moored';
  if (s.includes('fishing')) return 'Fishing';
  if (s.includes('sailing')) return 'Sailing';
  if (s.includes('not under command')) return 'Not Under Command';
  if (s.includes('restricted')) return 'Restricted';
  if (s.includes('pushing') || s.includes('towing') || s.includes('aground')) return 'Underway';
  if (s === '0') return 'Underway';
  // If it's a readable string just return it trimmed
  return raw.length < 30 ? raw : 'Underway';
}

// ── HTTP helper ───────────────────────────────────────────────────────────────
function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const options = {
      hostname: u.hostname, path: u.pathname + u.search, method: 'GET',
      headers: { ...headers }
    };
    https.get(options, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    }).on('error', reject);
  });
}


// ── Voyage data cache ─────────────────────────────────────────────────────────
let voyageCache = null;
let voyageCacheTs = 0;
const VOYAGE_CACHE_MS = 10 * 60 * 1000; // 10 min

async function fetchVoyageData() {
  const now = Date.now();
  if (voyageCache && (now - voyageCacheTs) < VOYAGE_CACHE_MS) {
    console.log('Serving cached voyage data');
    return voyageCache;
  }

  console.log('Fetching voyage data from myshiptracking vessel page...');
  const ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

  try {
    const r = await httpGet(
      `https://www.myshiptracking.com/vessels/saint-emilion-mmsi-${MMSI}-imo-8741832`,
      { 'User-Agent': ua, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' }
    );

    if (r.status !== 200) {
      console.log('Voyage page fetch failed:', r.status);
      return null;
    }

    const html = r.body;
    const data = {};

    // ── Current position coordinates ──────────────────────────────
    const coordMatch = html.match(/coordinates\s+([\d.-]+)°\s*\/\s*([\d.-]+)°/i);
    if (coordMatch) {
      data.lat = parseFloat(coordMatch[1]);
      data.lng = parseFloat(coordMatch[2]);
    }

    // ── Speed ─────────────────────────────────────────────────────
    const speedMatch = html.match(/Speed[^<]*<[^>]+>\s*([\d.]+)\s*Knots/i) ||
                       html.match(/speed is\s*<[^>]*>\s*([\d.]+)\s*Knots/i);
    if (speedMatch) data.sog = parseFloat(speedMatch[1]);

    // ── Course ────────────────────────────────────────────────────
    const courseMatch = html.match(/Course[^<]*:\s*([\d.]+)°/i);
    if (courseMatch) data.cog = parseFloat(courseMatch[1]);

    // ── Departure port + time ─────────────────────────────────────
    const deptMatch = html.match(/PORT DEPARTURE[^|]*\|\s*([^\|]+)\s*\|\s*([\d-]+ [\d:]+)/i) ||
                      html.match(/PORT DEPARTURE.*?([A-Z][A-Z ]+)\s*\|\s*([\d-]+ [\d:]+)/);
    if (deptMatch) {
      data.departurePort = deptMatch[1].trim();
      data.departureTime = deptMatch[2].trim() + ' UTC';
    }

    // ── Departure via ATD field ───────────────────────────────────
    const atdMatch = html.match(/ATD[\s\S]{0,50}?([\d-]+ [\d:]+)/i);
    if (atdMatch && !data.departureTime) {
      data.departureTime = atdMatch[1].trim() + ' UTC';
    }

    // ── Departure port via "PORT DEPARTURE" event row ─────────────
    const deptPortMatch = html.match(/PORT DEPARTURE[\s\S]{0,200}?([A-Z]{2}[A-Z ]{2,30})/);
    if (deptPortMatch && !data.departurePort) {
      data.departurePort = deptPortMatch[1].trim();
    }

    // ── Albany departure specifically ─────────────────────────────
    const albanyMatch = html.match(/ALBANY[\s\S]{0,100}?([\d-]+ [\d:]+)\s*(?:\(UTC\))?/i);
    if (albanyMatch) {
      data.departurePort = data.departurePort || 'ALBANY';
      data.departureTime = data.departureTime || albanyMatch[1].trim() + ' UTC';
    }

    // ── Distance traveled ─────────────────────────────────────────
    const distMatch = html.match(/Distance Travelled[^<]*[\s\S]{0,50}?([\d.]+)\s*nm/i);
    if (distMatch) data.distanceTraveled = parseFloat(distMatch[1]);

    // ── Average speed ─────────────────────────────────────────────
    const avgMatch = html.match(/AVG Speed[^<]*[\s\S]{0,50}?([\d.]+)\s*Knots/i);
    if (avgMatch) data.avgSpeed = parseFloat(avgMatch[1]);

    // ── Max speed ─────────────────────────────────────────────────
    const maxMatch = html.match(/MAX Speed[^<]*[\s\S]{0,50}?([\d.]+)\s*Knots/i);
    if (maxMatch) data.maxSpeed = parseFloat(maxMatch[1]);

    // ── Time travelled ────────────────────────────────────────────
    const timeMatch = html.match(/Time Travelled[^<]*[\s\S]{0,80}?([\d]+\s*h[^<,]{0,20})/i);
    if (timeMatch) data.timeTraveled = timeMatch[1].trim();

    // ── Last port calls (extract table rows) ──────────────────────
    const portCalls = [];
    const pcSection = html.match(/Last Port Calls[\s\S]{0,3000}/i);
    if (pcSection) {
      const rowMatches = pcSection[0].matchAll(/ALBANY|NEW YORK|YONKERS|BAYONNE|PERTH AMBOY|LINDEN/gi);
      const seen = new Set();
      for (const m of rowMatches) {
        if (!seen.has(m[0].toUpperCase())) {
          seen.add(m[0].toUpperCase());
          portCalls.push(m[0].toUpperCase());
        }
      }
    }
    if (portCalls.length) {
      data.recentPorts = portCalls;
      // Use first recent port as departure if not already found
      if (!data.departurePort && portCalls.length > 0) {
        data.departurePort = portCalls[0];
      }
    }

    // ── Nav status ────────────────────────────────────────────────
    const navMatch = html.match(/Pushing Ahead|Under way|Moored|At Anchor|Underway/i);
    if (navMatch) data.nav = parseNavStatus(navMatch[0]);

    // ── Draught ───────────────────────────────────────────────────
    const draughtMatch = html.match(/Draught[^<]*[\s\S]{0,50}?([\d.]+)\s*m/i);
    if (draughtMatch) data.draught = parseFloat(draughtMatch[1]);

    // Derive departure time from timeTraveled if not found directly
    if (!data.departureTime && data.timeTraveled) {
      const hoursMatch = data.timeTraveled.match(/([\d.]+)\s*h/);
      if (hoursMatch) {
        const hrs = parseFloat(hoursMatch[1]);
        const depTime = new Date(now - hrs * 3600000);
        data.departureTime = depTime.toISOString().replace('T',' ').slice(0,16) + ' UTC';
        data.departureTimeDerived = true; // flag that it was calculated not scraped
      }
    }

    data.source = 'myshiptracking.com';
    data.fetchedAt = new Date().toISOString();
    data.fetchedAtTs = now;

    console.log('Voyage data:', JSON.stringify(data));
    voyageCache = data;
    voyageCacheTs = now;
    return data;

  } catch(e) {
    console.error('Voyage fetch error:', e.message);
    return null;
  }
}


// ── Voyage history from track API ────────────────────────────────────────────
let voyageHistoryCache = null;
let voyageHistoryCacheTs = 0;
const HISTORY_CACHE_MS = 60 * 60 * 1000; // 1 hour — expensive endpoint

async function fetchVoyageHistory() {
  const now = Date.now();
  if (voyageHistoryCache && (now - voyageHistoryCacheTs) < HISTORY_CACHE_MS) {
    return voyageHistoryCache;
  }
  if (!MYSHIPTRACK_KEY) return null;

  console.log('Fetching voyage history from MyShipTracking track API...');

  try {
    // Fetch last 30 days, one position per hour to minimize credits
    const toDate = new Date().toISOString().split('T')[0];
    const fromDate = new Date(now - 30*24*3600000).toISOString().split('T')[0];
    const url = `https://api.myshiptracking.com/api/v2/vessel/track?mmsi=${MMSI}&timegroup=60&dtstart=${fromDate}&dtend=${toDate}`;

    const r = await httpGet(url, {
      'Authorization': `Bearer ${MYSHIPTRACK_KEY}`,
      'User-Agent': 'SaintEmilionTracker/1.0'
    });

    if (r.status !== 200) {
      console.log('Track API error:', r.status, r.body.slice(0,100));
      return null;
    }

    const data = JSON.parse(r.body);
    if (data.status !== 'success' || !data.data || !data.data.length) {
      console.log('Track API: no data');
      return null;
    }

    const positions = data.data;
    console.log(`Track API: ${positions.length} positions received`);

    // Parse into voyages by detecting mooring events
    // A voyage starts when speed goes from 0 → >1 kn
    // A voyage ends when speed goes from >1 kn → 0 for sustained period
    const voyages = [];
    let currentVoyage = null;
    let mooredCount = 0;

    const HUDSON_PORTS = [
      {name:'Albany Oil Terminal', lat:42.6512, lng:-73.7550},
      {name:'Yonkers Anchorage', lat:40.9340, lng:-73.8950},
      {name:'Bayonne Terminal', lat:40.6574, lng:-74.1130},
      {name:'Gowanus Bay Terminal', lat:40.6520, lng:-74.0170},
      {name:'Phillips 66 Linden Terminal', lat:40.6200, lng:-74.2300},
      {name:'Port Imperial Weehawken', lat:40.7680, lng:-74.0200},
      {name:'GW Bridge Anchorage', lat:40.8510, lng:-73.9520},
      {name:'Haverstraw Bay Anchorage', lat:41.2100, lng:-73.9500},
      {name:'Kingston Oil Dock', lat:41.9230, lng:-73.9750},
      {name:'New Hamburg Dock', lat:41.5800, lng:-73.9700},
      {name:'Newburgh Waterfront', lat:41.5030, lng:-74.0080},
    ];

    function nearestPort(lat, lng) {
      let best = null, bestDist = 999;
      HUDSON_PORTS.forEach(p => {
        const d = Math.sqrt((p.lat-lat)**2+(p.lng-lng)**2)*60;
        if (d < bestDist) { bestDist = d; best = p; }
      });
      return bestDist < 5 ? best : null; // within 5nm
    }

    for (let i = 0; i < positions.length; i++) {
      const p = positions[i];
      const sog = parseFloat(p.speed || p.sog || 0);
      const lat = parseFloat(p.lat);
      const lng = parseFloat(p.lng || p.lon);
      const ts = new Date(p.received || p.timestamp).getTime();

      if (sog > 1) {
        mooredCount = 0;
        if (!currentVoyage) {
          // Start new voyage
          currentVoyage = {
            startTs: ts,
            startLat: lat, startLng: lng,
            startPort: nearestPort(lat, lng),
            positions: [{lat, lng, ts, sog}],
            totalNm: 0
          };
        } else {
          // Add to current voyage
          const prev = currentVoyage.positions[currentVoyage.positions.length-1];
          const segNm = Math.sqrt((lat-prev.lat)**2+(lng-prev.lng)**2)*60;
          currentVoyage.totalNm += segNm;
          currentVoyage.positions.push({lat, lng, ts, sog});
          currentVoyage.endTs = ts;
          currentVoyage.endLat = lat;
          currentVoyage.endLng = lng;
        }
      } else {
        mooredCount++;
        if (currentVoyage && mooredCount >= 2) {
          // Vessel has been moored for 2+ hours — end voyage
          const endPort = nearestPort(currentVoyage.endLat || lat, currentVoyage.endLng || lng);
          const durationHrs = ((currentVoyage.endTs || ts) - currentVoyage.startTs) / 3600000;
          const avgSpeed = durationHrs > 0 ? currentVoyage.totalNm / durationHrs : 0;

          if (currentVoyage.totalNm > 5) { // ignore tiny movements
            const startDate = new Date(currentVoyage.startTs);
            const endDate = new Date(currentVoyage.endTs || ts);
            voyages.push({
              name: `${currentVoyage.startPort?.name || 'Unknown'} → ${endPort?.name || 'Unknown'}`,
              startPort: currentVoyage.startPort?.name || 'Unknown',
              endPort: endPort?.name || 'Unknown',
              startTs: currentVoyage.startTs,
              endTs: currentVoyage.endTs || ts,
              date: startDate.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'}),
              dateEnd: endDate.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'}),
              distNm: Math.round(currentVoyage.totalNm * 10) / 10,
              avgSpeed: Math.round(avgSpeed * 10) / 10,
              color: '#0095b0',
              track: currentVoyage.positions.map(p => [p.lat, p.lng])
            });
          }
          currentVoyage = null;
          mooredCount = 0;
        }
      }
    }

    // Add in-progress voyage if vessel is currently underway
    if (currentVoyage && currentVoyage.totalNm > 1) {
      voyages.push({
        name: `${currentVoyage.startPort?.name || 'Unknown'} → In Progress`,
        startPort: currentVoyage.startPort?.name || 'Unknown',
        endPort: null,
        startTs: currentVoyage.startTs,
        date: new Date(currentVoyage.startTs).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'}),
        distNm: Math.round(currentVoyage.totalNm * 10) / 10,
        live: true,
        color: '#16a050',
        track: currentVoyage.positions.map(p => [p.lat, p.lng])
      });
    }

    // Most recent first
    voyages.reverse();
    console.log(`Parsed ${voyages.length} voyages from track data`);

    voyageHistoryCache = voyages;
    voyageHistoryCacheTs = now;
    return voyages;

  } catch(e) {
    console.error('Voyage history error:', e.message);
    return null;
  }
}

// ── HTTP Server ───────────────────────────────────────────────────────────────
const httpServer = http.createServer(async (req, res) => {
  setCORS(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Status endpoint
  if (req.method === 'GET') {
    const usingAPI = apiIsActive();
    const daysLeft = Math.max(0, Math.round(
      (API_EXPIRY_MS - (Date.now() - new Date(API_START_DATE).getTime())) / 86400000
    ));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      mode: usingAPI ? `MyShipTracking API (${daysLeft} days left)` : 'scrape fallback',
      aisstream: aisConnected ? 'connected' : 'disconnected',
      aisCacheAge: aisCache ? Math.round((Date.now()-aisCache.ts)/1000)+'s ago' : 'none',
      lastScrape: lastScrapeTs ? new Date(lastScrapeTs).toISOString() : 'never'
    }));
    return;
  }

  // Voyage history endpoint
  if (req.method === 'GET' && req.url === '/history') {
    try {
      const data = await fetchVoyageHistory();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data || []));
    } catch(e) {
      res.writeHead(500); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // Voyage data endpoint
  if (req.method === 'GET' && req.url === '/voyage') {
    try {
      const data = await fetchVoyageData();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data || { error: 'no data' }));
    } catch(e) {
      res.writeHead(500); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  if (req.method !== 'POST' || req.url !== '/fetch') {
    res.writeHead(404); res.end('Not found'); return;
  }

  console.log('--- Fetch request ---');

  try {
    // 1. aisstream cache — if fresh (<10min) serve immediately regardless of other sources
    if (aisCache && (Date.now() - aisCache.ts) < 600000) {
      console.log('Serving fresh aisstream cache');
      return respond(res, aisCache);
    }

    // 2. MyShipTracking API (primary for first 10 days)
    if (apiIsActive()) {
      const r = await fetchMyShipTrackingAPI();
      if (r) return respond(res, r);
    }

    // 3. Stale aisstream cache — better than nothing, flag as stale
    if (aisCache) {
      const ageMins = Math.round((Date.now() - aisCache.ts) / 60000);
      console.log(`Serving stale aisstream cache (${ageMins}min old)`);
      return respond(res, {
        ...aisCache, stale: true, staleMinutes: ageMins,
        summary: `Last known ${ageMins} min ago: ${aisCache.location} · ${aisCache.sog} kn · ${aisCache.nav}`
      });
    }

    // 4. MyShipTracking scrape (after API expires, throttled to 15min)
    if (!apiIsActive()) {
      const r = await fetchMyShipTrackingScrape();
      if (r) return respond(res, r);
    }

    // 5. VesselAPI last resort
    const r = await fetchVesselAPI();
    if (r) return respond(res, r);

    // Nothing worked
    const waiting = aisConnected
      ? 'AIS connected — waiting for vessel broadcast'
      : 'Position not available — no data sources responding';
    respond(res, { error: 'waiting', summary: waiting });

  } catch(e) {
    console.error('Server error:', e.message);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
  }
});

httpServer.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  console.log('AISSTREAM_KEY:   ', AISSTREAM_KEY   ? '✓ set' : '✗ missing');
  console.log('MYSHIPTRACK_KEY: ', MYSHIPTRACK_KEY  ? '✓ set' : '✗ missing');
  console.log('VESSELAPI_KEY:   ', VESSELAPI_KEY    ? '✓ set' : '✗ missing');
  console.log('API active:      ', apiIsActive() ? 'YES' : 'NO — using scrape');
  connectAisstream();
  // Pre-fetch voyage data on startup
  setTimeout(fetchVoyageData, 3000);
});
