import { WebSocketServer } from "ws";
import http from "http";

// --- Config
const PORT = process.env.PORT || 8765;
const TICK_MS = 50; // ~20 Hz

// --- Demo signal generators (bounded + smooth)
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
let t0 = Date.now();

function demoFrame(nowMs) {
  const t = (nowMs - t0) / 1000;

  // Bounce RPM 0.8k↔6.5k
  const rpm = Math.round(3800 + 3000 * Math.sin(t * 0.9));

  // MAP 30↔95 kPa (idle↔load)
  const map_kpa = +(62 + 32 * Math.sin(t * 0.7)).toFixed(1);

  // TPS 0↔40%
  const tps_pct = +(20 + 20 * Math.sin(t * 1.7 + Math.PI / 3)).toFixed(1);

  // AFR 12.0↔15.5
  const afr = +(13.7 + 1.8 * Math.sin(t * 1.3 + Math.PI / 5)).toFixed(2);

  // CLT warmup 70F→195F, then hover
  const clt_f = clamp(70 + t * 6, 70, 195);

  // MAT wander 90F↔140F
  const mat_f = +(115 + 25 * Math.sin(t * 0.4)).toFixed(1);

  // IAC duty 10↔55%
  const iac_duty = +(32 + 22 * Math.sin(t * 0.8 + Math.PI / 7)).toFixed(1);

  // battery 13.6↔14.3V
  const batt_v = +(13.95 + 0.35 * Math.sin(t * 0.2)).toFixed(2);

  return {
    type: "frame",
    seq: Math.floor(t * (1000 / TICK_MS)),
    ts: Date.now() / 1000,
    data: { rpm, map_kpa, tps_pct, afr, clt_f, mat_f, iac_duty, batt_v }
  };
}

// --- Simple WS server (one endpoint)
const server = http.createServer((_req, res) => {
  if (_req.url === "/v1/channels") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      fields: {
        rpm: { unit: "rpm" },
        map_kpa: { unit: "kPa" },
        tps_pct: { unit: "%" },
        afr: { unit: "" },
        clt_f: { unit: "°F" },
        mat_f: { unit: "°F" },
        iac_duty: { unit: "%" },
        batt_v: { unit: "V" }
      }
    }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: "/v1/stream" });

// optional: “demo on/off” via query ?mode=static
function makeSender(ws, mode) {
  if (mode === "static") {
    // One frame only, no animation (useful to show “set needle programmatically”)
    ws.send(JSON.stringify({ type:"hello", note:"static demo" }));
    ws.send(JSON.stringify({ type:"frame", seq:0, ts:Date.now()/1000,
      data: { rpm: 2200, map_kpa: 45.0, tps_pct: 3.5, afr: 14.1, clt_f: 145.0, mat_f: 112.0, iac_duty: 28.0, batt_v: 14.0 }
    }));
    return null;
  }

  ws.send(JSON.stringify({ type:"hello", note:"animated demo @ ~20Hz" }));

  let timer = setInterval(() => {
    if (ws.readyState !== ws.OPEN) { clearInterval(timer); return; }
    ws.send(JSON.stringify(demoFrame(Date.now())));
  }, TICK_MS);

  return () => clearInterval(timer);
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const mode = url.searchParams.get("mode"); // "static" or null

  const stop = makeSender(ws, mode);

  ws.on("close", () => { if (stop) stop(); });
});

server.listen(PORT, () => {
  console.log(`ms-bridge demo up:
  WS stream:  ws://localhost:${PORT}/v1/stream
  Channels:   http://localhost:${PORT}/v1/channels
  Add '?mode=static' to get a single static update.`);
});
