#!/usr/bin/env node
/**
 * ms-bridge-redis.js
 * Consume pyEfi Megasquirt realtime JSON from Redis Pub/Sub and expose a HUDIY-friendly WS/HTTP API.
 *
 * WebSocket:
 *   ws://0.0.0.0:8765/v1/stream
 *
 * HTTP:
 *   GET /v1/channels  -> {"fields": {...}}
 *   GET /v1/snapshot  -> last frame
 *   GET /v1/health    -> status (connected, lastDataAgoMs, clients, channel)
 *
 * Run examples:
 *   # UNIX socket (pyEfi default)
 *   node ms-bridge-redis.js --redis-sock=/var/run/redis/redis.sock
 *
 *   # TCP
 *   node ms-bridge-redis.js --redis-host=127.0.0.1 --redis-port=6379
 *
 *   # Custom channel and broadcast rate
 *   node ms-bridge-redis.js --channel=ms2:realtime --hz=20
 */

import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';

// ---------------- CLI / ENV ----------------
const args = Object.fromEntries(
  process.argv.slice(2).map(a => {
    const [k, v] = a.split('=');
    return [k.replace(/^--/, ''), v ?? true];
  })
);

const HZ = Number(args.hz || process.env.HZ || 20);
const WS_HOST = args.wshost || process.env.WS_HOST || '0.0.0.0';
const WS_PORT = Number(args.wsport || process.env.WS_PORT || 8765);
const CHANNEL = args.channel || process.env.REDIS_CHANNEL || 'ms2:realtime';

// Redis: prefer UNIX socket, else TCP
const REDIS_SOCK = args['redis-sock'] || process.env.REDIS_SOCK || '';
const REDIS_HOST = args['redis-host'] || process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = Number(args['redis-port'] || process.env.REDIS_PORT || 6379);

// ---------------- Normalization helpers ----------------
function firstKey(obj, variants) {
  for (const key of variants) {
    if (obj[key] != null) return obj[key];
    const found = Object.keys(obj).find(k => k.toLowerCase() === String(key).toLowerCase());
    if (found && obj[found] != null) return obj[found];
  }
  return undefined;
}
function toFloat(x) {
  if (x == null) return undefined;
  const n = Number(x);
  return Number.isFinite(n) ? n : undefined;
}
function cToF(c) {
  if (c == null) return undefined;
  return c * 9 / 5 + 32;
}

// Map pyEfi payload to stable HUDIY keys
function normalizePayload(src) {
  // rpm
  const rpm = toFloat(firstKey(src, ['rpm', 'RPM', 'engine.rpm']));
  // map (kPa preferred)
  const map_kpa = toFloat(firstKey(src, ['map_kpa', 'map', 'MAP', 'engine.map']));
  // tps in %
  const tps = toFloat(firstKey(src, ['tps_pct', 'tps', 'TPS']));

  // CLT and MAT (accept °F directly or convert from °C)
  let clt_f = toFloat(firstKey(src, ['clt_f', 'CLT_f', 'Coolant_f', 'coolant_f', 'cltF']));
  if (clt_f == null) {
    const clt_c = toFloat(firstKey(src, ['clt', 'Coolant', 'CLT']));
    if (clt_c != null) clt_f = cToF(clt_c);
  }

  let mat_f = toFloat(firstKey(src, ['mat_f', 'IAT_f', 'matF', 'iat_f']));
  if (mat_f == null) {
    const mat_c = toFloat(firstKey(src, ['mat', 'IAT']));
    if (mat_c != null) mat_f = cToF(mat_c);
  }

  const afr = toFloat(firstKey(src, ['afr', 'AFR', 'lambda', 'Lambda']));
  const batt_v = toFloat(firstKey(src, ['batt_v', 'vBatt', 'bat', 'battery']));

  const out = {};
  if (rpm != null) out.rpm = Math.round(rpm);
  if (map_kpa != null) out.map_kpa = Number(map_kpa.toFixed(1));
  if (tps != null) out.tps_pct = Number(tps.toFixed(1));
  if (clt_f != null) out.clt_f = Number(clt_f.toFixed(1));
  if (afr != null) out.afr = Number(afr.toFixed(2));
  if (batt_v != null) out.batt_v = Number(batt_v.toFixed(2));
  if (mat_f != null) out.mat_f = Number(mat_f.toFixed(1));
  return out;
}

function parseRedisMessage(msg) {
  // ioredis 'message' gives (channel, messageString)
  if (typeof msg !== 'string' && !(msg instanceof Buffer)) return null;
  let s = msg;
  if (msg instanceof Buffer) s = msg.toString('utf8');
  s = String(s).trim();
  try {
    const parsed = JSON.parse(s);
    if (parsed && typeof parsed === 'object') return parsed;
  } catch {
    // not JSON, ignore
  }
  return null;
}

// ---------------- Bridge state ----------------
const fields = {
  rpm:     { unit: 'rpm' },
  map_kpa: { unit: 'kPa' },
  tps_pct: { unit: '%' },
  clt_f:   { unit: '°F' },
  afr:     { unit: '' },
  batt_v:  { unit: 'V' },
  mat_f:   { unit: '°F' }
};

let lastFrame = null;
let lastDataTs = 0;
const clients = new Set();

// ---------------- Redis subscriber ----------------
function makeRedisSubscriber() {
  const opts = REDIS_SOCK
    ? { path: REDIS_SOCK }
    : { host: REDIS_HOST, port: REDIS_PORT };

  const sub = new Redis(opts);

  sub.on('connect', () => {
    console.log(`[redis] connecting ${REDIS_SOCK ? REDIS_SOCK : `${REDIS_HOST}:${REDIS_PORT}`}`);
  });
  sub.on('ready', async () => {
    console.log(`[redis] ready, subscribing "${CHANNEL}"`);
    try { await sub.subscribe(CHANNEL); }
    catch (e) { console.error('[redis] subscribe error:', e.message); }
  });
  sub.on('close', () => console.warn('[redis] connection closed'));
  sub.on('reconnecting', (t) => console.warn(`[redis] reconnecting in ${t}ms`));
  sub.on('error', (e) => console.error('[redis] error:', e.message));

  sub.on('message', (_channel, message) => {
    const raw = parseRedisMessage(message);
    if (!raw) return;
    const data = normalizePayload(raw);
    if (!data || Object.keys(data).length === 0) return;

    lastDataTs = Date.now() / 1000;
    lastFrame = { type: 'frame', ts: lastDataTs, data };
  });

  return sub;
}

const subscriber = makeRedisSubscriber();

// ---------------- WS + HTTP server ----------------
const server = createServer((req, res) => {
  try {
    if (req.url === '/v1/channels') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ fields }));
      return;
    }
    if (req.url === '/v1/snapshot') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(lastFrame || { type: 'frame', ts: Date.now() / 1000, data: {} }));
      return;
    }
    if (req.url === '/v1/health') {
      const age = lastDataTs ? (Date.now() / 1000 - lastDataTs) : null;
      const ok = subscriber.status === 'ready' || subscriber.status === 'connecting' || subscriber.status === 'reconnecting' ? true : false;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        ok,
        connected: subscriber.status === 'ready',
        lastDataAgoMs: age != null ? Math.round(age * 1000) : null,
        clients: clients.size,
        channel: CHANNEL
      }));
      return;
    }

    res.writeHead(404);
    res.end();
  } catch (e) {
    res.writeHead(500);
    res.end(JSON.stringify({ error: e.message }));
  }
});

const wss = new WebSocketServer({ server, path: '/v1/stream' });
wss.on('connection', (ws) => {
  clients.add(ws);
  try {
    ws.send(JSON.stringify({ type: 'hello', note: 'pyEfi/Redis bridge' }));
    if (lastFrame) ws.send(JSON.stringify(lastFrame));
  } catch {}
  ws.on('close', () => clients.delete(ws));
});

// steady broadcast loop @ HZ (re-broadcasts last good data)
const periodMs = Math.max(5, Math.round(1000 / Math.max(1, HZ)));
setInterval(() => {
  if (!lastFrame || clients.size === 0) return;
  const payload = JSON.stringify(lastFrame);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) {
      try { ws.send(payload); } catch {}
    }
  }
}, periodMs);

server.listen(WS_PORT, WS_HOST, () => {
  console.log(`[ws] ws://${WS_HOST}:${WS_PORT}/v1/stream`);
  console.log(`[http] /v1/channels  /v1/snapshot  /v1/health`);
  console.log(`[redis] channel=${CHANNEL} via ${REDIS_SOCK ? REDIS_SOCK : `${REDIS_HOST}:${REDIS_PORT}`}`);
  console.log(`[rate] ${HZ} Hz`);
});

// graceful shutdown
function shutdown() {
  try { subscriber.quit(); } catch {}
  try { server.close(() => process.exit(0)); } catch { process.exit(0); }
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
