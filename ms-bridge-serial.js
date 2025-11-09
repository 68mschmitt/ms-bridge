// ms-bridge-serial.js (resilient, RPM-first)
// Reads MS2 OutputChannels over serial and publishes JSON frames over WebSocket.
// Node 18.x on Pi: serialport is CJS → import default and destructure.

import fs from "fs";
import http from "http";
import { WebSocketServer } from "ws";
import ini from "ini";

import pkg from "serialport";
const { SerialPort, list: listSerial } = pkg;

// ------------------ CLI / ENV ------------------
const args = Object.fromEntries(process.argv.slice(2).map(a => {
  const [k, v] = a.split("=");
  return [k.replace(/^--/, ""), v ?? true];
}));

let SERIAL_PORT = args.port || process.env.MS_PORT || "";    // if empty, we auto-discover
const BAUD = Number(args.baud || process.env.MS_BAUD || 115200);
const INI_PATH = args.ini || process.env.MS_INI;
const WS_PORT = Number(args.wsport || process.env.WS_PORT || 8765);
const POLL_MS = Number(args.poll || process.env.POLL_MS || 50);  // ~20 Hz
const NO_DATA_TIMEOUT_MS = Number(args.nodata || 3000);
const MAX_BACKOFF_MS = 8000;
const DEBUG = !!args.debug;

// CAN id for %c substitution (usually 0)
const CAN_ID = Number(args.can || 0);

// RPM overrides (runtime tunable)
const RPM_OFFSET = (args.rpmOffset !== undefined) ? Number(args.rpmOffset) : 80;
const RPM_ENDIAN = (args.endian || "le").toLowerCase(); // "le" | "be"
const RPM_SCALE  = (args.rpmScale  !== undefined) ? Number(args.rpmScale)  : 0.625;

// Optional VID/PID filters for discovery
const USB_VID = (args.vid || process.env.MS_USB_VID || "").toLowerCase();
const USB_PID = (args.pid || process.env.MS_USB_PID || "").toLowerCase();

if (!INI_PATH) {
  console.error("Missing --ini=/path/to/mainController.ini");
  process.exit(1);
}

// ------------------ .ini essentials ------------------
const rawIni = fs.readFileSync(INI_PATH, "utf8");

// ochBlockSize
const ochBlockSize = (() => {
  const m = rawIni.match(/^\s*ochBlockSize\s*=\s*(\d+)/mi);
  if (!m) throw new Error("ochBlockSize not found in .ini");
  return parseInt(m[1], 10);
})();

// ochGetCommand (string with escapes and maybe %c)
const ochGetCmdStr = (() => {
  const m = rawIni.match(/^\s*ochGetCommand\s*=\s*"([^"]+)"/mi);
  if (!m) throw new Error("ochGetCommand not found in .ini");
  return m[1];
})();

// rpm definition (we still read it for info; we allow overrides)
const rpmDef = (() => {
  const re = /^\s*rpm\s*=\s*scalar\s*,\s*([SU]\d+)\s*,\s*(\d+)\s*,\s*"[^"]*"\s*,\s*([+-]?\d*\.?\d+)\s*,\s*([+-]?\d*\.?\d+)/mi;
  const m = rawIni.match(re);
  if (!m) return { type: "U16", offset: RPM_OFFSET, scale: 1, translate: 0 };
  return {
    type: m[1],
    offset: parseInt(m[2], 10),
    scale: parseFloat(m[3]),
    translate: parseFloat(m[4])
  };
})();

console.log(`[ini] ochBlockSize=${ochBlockSize}, ochGetCommand="${ochGetCmdStr}", canId=${CAN_ID}`);
console.log(`[ini] rpm(type=${rpmDef.type}, iniOffset=${rpmDef.offset}, iniScale=${rpmDef.scale}, iniTrans=${rpmDef.translate})`);
console.log(`[ini] overrides: rpmOffset=${RPM_OFFSET}, endian=${RPM_ENDIAN}, rpmScale=${RPM_SCALE}`);

// Build ochGetCommand: substitute %c/%d and decode \xNN
function buildOchGetCommand(fmt, canId) {
  let s = fmt.replace(/%c/g, String.fromCharCode(canId))
             .replace(/%d/g, String(canId))
             .replace(/\{tsCanId\}/g, String.fromCharCode(canId)); // tolerate alt token
  const out = [];
  for (let i = 0; i < s.length; i++) {
    if (s[i] === "\\") {
      const n = s[i + 1];
      if (n === "x") { out.push(parseInt(s.slice(i + 2, i + 4), 16)); i += 3; }
      else if (n === "n") { out.push(10); i++; }
      else if (n === "r") { out.push(13); i++; }
      else if (n === "\\") { out.push(92); i++; }
      else { out.push(n.charCodeAt(0)); i++; }
    } else out.push(s.charCodeAt(i));
  }
  return Uint8Array.from(out);
}
const ochGetCommandBytes = buildOchGetCommand(ochGetCmdStr, CAN_ID);
console.log(`[dbg] built cmd bytes: ${[...ochGetCommandBytes].map(b=>b.toString(16).padStart(2,"0")).join(" ")}`);

// ------------------ Serial lifecycle ------------------
let port = null;
let rxBuf = Buffer.alloc(0);
let lastFrame = null;
let lastDataTs = 0;
let pollTimer = null;
let openBackoff = 500;
let opening = false;

async function autoDiscoverPort() {
  const ports = await listSerial();
  if (USB_VID && USB_PID) {
    const byId = ports.filter(p =>
      (p.vendorId || "").toLowerCase() === USB_VID &&
      (p.productId || "").toLowerCase() === USB_PID
    );
    if (byId.length) return byId[0].path;
  }
  const candidates = ports.filter(p =>
    (p.path || "").includes("ttyUSB") || (p.path || "").includes("ttyACM")
  );
  if (candidates.length) return candidates[0].path;
  return ports[0]?.path || "";
}

async function ensurePortPath() {
  if (SERIAL_PORT) return SERIAL_PORT;
  SERIAL_PORT = await autoDiscoverPort();
  if (!SERIAL_PORT) console.error("[serial] no serial ports found (will retry).");
  else console.log(`[serial] auto-discovered port: ${SERIAL_PORT}`);
  return SERIAL_PORT;
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(() => {
    if (!port || !port.isOpen) return;
    try { port.write(ochGetCommandBytes); } catch (e) {
      console.error("[serial] write error:", e.message);
    }
    if (lastDataTs && (Date.now() - lastDataTs) > NO_DATA_TIMEOUT_MS) {
      console.warn(`[serial] no data in ${NO_DATA_TIMEOUT_MS}ms — resetting port.`);
      resetPort();
    }
  }, POLL_MS);
}
function stopPolling() { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } }

function resetPort() {
  try { if (port && port.isOpen) port.close(); } catch {}
  openWithRetry();
}

async function openWithRetry() {
  if (opening) return;
  opening = true;

  await ensurePortPath();
  if (!SERIAL_PORT) { opening = false; return setTimeout(openWithRetry, Math.min(openBackoff, MAX_BACKOFF_MS)); }

  const attemptPath = SERIAL_PORT;
  console.log(`[serial] opening ${attemptPath} @ ${BAUD} (backoff=${openBackoff}ms)`);

  try {
    port = new SerialPort({ path: attemptPath, baudRate: BAUD, autoOpen: false });
  } catch (e) {
    console.error("[serial] construct error:", e.message);
    opening = false;
    openBackoff = Math.min(openBackoff * 1.7, MAX_BACKOFF_MS);
    return setTimeout(openWithRetry, openBackoff);
  }

  port.on("open", () => {
    console.log(`[serial] open ${attemptPath} @ ${BAUD}`);
    openBackoff = 500;
    rxBuf = Buffer.alloc(0);
    lastDataTs = 0;
    startPolling();
  });

  // data handler
  port.on("data", (chunk) => {
    lastDataTs = Date.now();
    rxBuf = Buffer.concat([rxBuf, chunk]);

    while (rxBuf.length >= ochBlockSize) {
      const frame = rxBuf.subarray(0, ochBlockSize);
      rxBuf = rxBuf.subarray(ochBlockSize);

      // ---- live-frame watchdog (1 Hz)
      frameCount++;
      const now = Date.now();
      const h = hashFirst64(frame);
      if (now - lastReport > 1000) {
        const changed = (lastHash === null) ? true : (h !== lastHash);
        if (!changed) staleCount++;
        console.log(`[live] ${frameCount} frames/s, changed=${changed ? "yes" : "NO"} (stale=${staleCount})`);
        frameCount = 0; lastReport = now; lastHash = h;
      }

      // ---- RPM decode using overrides
      const rawLE = (frame[RPM_OFFSET] | (frame[RPM_OFFSET + 1] << 8)) >>> 0;
      const rawBE = ((frame[RPM_OFFSET] << 8) | frame[RPM_OFFSET + 1]) >>> 0;
      const raw   = RPM_ENDIAN === "be" ? rawBE : rawLE;
      const rpm   = Math.round(raw * RPM_SCALE);
      if (DEBUG) console.log(`[rpm] off=${RPM_OFFSET} ${RPM_ENDIAN.toUpperCase()} raw=${raw} rpm=${rpm}`);

      lastFrame = { type: "frame", ts: Date.now()/1000, data: { rpm } };
      broadcast(lastFrame);
    }
  });

  port.on("error", (e) => {
    console.error("[serial] error:", e.message);
    stopPolling();
    safeCloseThenRetry();
  });

  port.on("close", () => {
    console.warn("[serial] closed");
    stopPolling();
    safeCloseThenRetry();
  });

  port.open(err => {
    opening = false;
    if (err) {
      console.error("[serial] open failed:", err.message);
      openBackoff = Math.min(openBackoff * 1.7, MAX_BACKOFF_MS);
      return setTimeout(openWithRetry, openBackoff);
    }
  });
}

function safeCloseThenRetry() {
  try { if (port && port.isOpen) port.close(); } catch {}
  SERIAL_PORT = ""; // force rediscovery next attempt
  openBackoff = Math.min(openBackoff * 1.7, MAX_BACKOFF_MS);
  setTimeout(openWithRetry, openBackoff);
}

// Kick off
openWithRetry();

// ------------------ WS server ------------------
const server = http.createServer((req, res) => {
  if (req.url === "/v1/channels") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ fields: { rpm: { unit: "rpm" } } }));
    return;
  }
  if (req.url === "/v1/snapshot") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(lastFrame || { type: "frame", ts: Date.now()/1000, data: { rpm: 0 } }));
    return;
  }
  if (req.url === "/v1/health") {
    const healthy = !!(port && port.isOpen) && !!lastDataTs && (Date.now() - lastDataTs) < NO_DATA_TIMEOUT_MS * 2;
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      ok: healthy,
      port: port?.path || null,
      isOpen: !!(port && port.isOpen),
      lastDataAgoMs: lastDataTs ? (Date.now() - lastDataTs) : null
    }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, path: "/v1/stream" });
const clients = new Set();
wss.on("connection", (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: "hello", note: "ms2 serial bridge (rpm), resilient" }));
  if (lastFrame) ws.send(JSON.stringify(lastFrame));
  ws.on("close", () => clients.delete(ws));
});

function broadcast(msg) {
  const str = JSON.stringify(msg);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(str);
  }
}

server.listen(WS_PORT, () => {
  console.log(`[ws] ws://localhost:${WS_PORT}/v1/stream  (channels: /v1/channels, snapshot: /v1/snapshot, health: /v1/health)`);
});

// ------------------ Watchdog helpers ------------------
let frameCount = 0, lastReport = Date.now(), lastHash = null, staleCount = 0;
function hashFirst64(buf) {
  let h = 0, n = Math.min(64, buf.length);
  for (let i = 0; i < n; i++) h = ((h << 5) - h + buf[i]) | 0;
  return h;
}

// ------------------ Hardening ------------------
process.on("uncaughtException", (e) => {
  console.error("[fatal] uncaughtException:", e);
});
process.on("unhandledRejection", (e) => {
  console.error("[fatal] unhandledRejection:", e);
});
