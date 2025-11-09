// ms-bridge-serial.js (resilient version)
// - Opens MS2 serial, polls OutputChannels using ochGetCommand from the .ini,
// - Parses RPM (U16 BE) and publishes frames over WebSocket,
// - Auto-reconnects on failures and on "no data" timeouts.

import fs from "fs";
import http from "http";
import { WebSocketServer } from "ws";
import { SerialPort } from "serialport";
import { list as listSerial } from "serialport";
import ini from "ini";

// ------------------ CLI / ENV ------------------
const args = Object.fromEntries(process.argv.slice(2).map(a => {
  const [k, v] = a.split("=");
  return [k.replace(/^--/, ""), v ?? true];
}));

let SERIAL_PORT = args.port || process.env.MS_PORT || "";     // if empty, auto-discover
const BAUD = Number(args.baud || process.env.MS_BAUD || 115200);
const INI_PATH = args.ini || process.env.MS_INI;
const WS_PORT = Number(args.wsport || process.env.WS_PORT || 8765);
const POLL_MS = Number(args.poll || process.env.POLL_MS || 50);      // ~20Hz
const NO_DATA_TIMEOUT_MS = Number(args.nodata || 3000);              // watchdog for silent links
const MAX_BACKOFF_MS = 8000;

// Optional USB VID/PID filters for auto-discovery
const USB_VID = (args.vid || process.env.MS_USB_VID || "").toLowerCase();    // e.g. "0403"
const USB_PID = (args.pid || process.env.MS_USB_PID || "").toLowerCase();    // e.g. "6001"

if (!INI_PATH) {
  console.error("Missing --ini=/path/to/firmware.ini (MS2/Extra .ini with OutputChannels).");
  process.exit(1);
}

// ------------------ .ini parsing ------------------
const rawIni = fs.readFileSync(INI_PATH, "utf8");

// ochBlockSize
const ochBlockSize = (() => {
  const m = rawIni.match(/^\s*ochBlockSize\s*=\s*(\d+)/mi);
  if (!m) throw new Error("ochBlockSize not found in .ini");
  return parseInt(m[1], 10);
})();

// ochGetCommand (e.g. "a\x00\x06")
const ochGetCmdStr = (() => {
  const m = rawIni.match(/^\s*ochGetCommand\s*=\s*"([^"]+)"/mi);
  if (!m) throw new Error("ochGetCommand not found in .ini");
  return m[1];
})();

// ---- INI parsing for RPM (type, offset, scale, translate)
const rpmDef = (() => {
  // Example line:
  // rpm = scalar, U16, 6, "RPM", 1.000, 0.0
  const re = /^\s*rpm\s*=\s*scalar\s*,\s*([SU]\d+)\s*,\s*(\d+)\s*,\s*"[^"]*"\s*,\s*([+-]?\d*\.?\d+)\s*,\s*([+-]?\d*\.?\d+)/mi;
  const m = rawIni.match(re);
  if (!m) throw new Error("Couldn't find 'rpm = scalar, ...' in the INI.");
  return {
    type: m[1],                      // U16 / S16 (usually U16)
    offset: parseInt(m[2], 10),      // byte offset into outpc block
    scale: parseFloat(m[3]),         // multiply
    translate: parseFloat(m[4])      // add
  };
})();

const DEBUG = !!args.debug;

// Util readers
function u16be(buf, off) { return (buf[off] << 8) | buf[off + 1]; }
function u16le(buf, off) { return (buf[off + 1] << 8) | buf[off]; }
function s16be(buf, off) { let v = u16be(buf, off); return (v & 0x8000) ? v - 0x10000 : v; }
function s16le(buf, off) { let v = u16le(buf, off); return (v & 0x8000) ? v - 0x10000 : v; }

// Generic scalar extractor with fallback endianness check
function readScalar16(buf, def) {
  const signed = /^S/i.test(def.type);
  const be = signed ? s16be(buf, def.offset) : u16be(buf, def.offset);
  const beScaled = be * def.scale + def.translate;

  // Heuristic: valid RPM should be 0..12000. If BE decode is way off, try LE.
  if (beScaled >= 0 && beScaled <= 12000) return beScaled;

  const le = signed ? s16le(buf, def.offset) : u16le(buf, def.offset);
  const leScaled = le * def.scale + def.translate;
  // Prefer the one that's plausible
  if (leScaled >= 0 && leScaled <= 12000) return leScaled;

  // Fall back to BE result if both are “weird” (still return something)
  return beScaled;
}

console.log(`[ini] ochBlockSize=${ochBlockSize}, ochGetCommand="${ochGetCmdStr}"
[ini] rpm: type=${rpmDef.type}, offset=${rpmDef.offset}`);

// Decode "a\x00\x06" -> Uint8Array
function decodeEscapes(str) {
  const out = [];
  for (let i = 0; i < str.length; i++) {
    if (str[i] === "\\") {
      const n = str[i + 1];
      if (n === "x") { out.push(parseInt(str.slice(i + 2, i + 4), 16)); i += 3; }
      else if (n === "n") { out.push(10); i++; }
      else if (n === "r") { out.push(13); i++; }
      else if (n === "\\") { out.push(92); i++; }
      else { out.push(n.charCodeAt(0)); i++; }
    } else {
      out.push(str.charCodeAt(i));
    }
  }
  return Uint8Array.from(out);
}
const ochGetCommandBytes = decodeEscapes(ochGetCmdStr);

// Helpers
function readU16BE(buf, off) { return (buf[off] << 8) | buf[off + 1]; }

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
  // Prefer explicit /dev/ttyUSB*, /dev/ttyACM* if no VID/PID specified
  let candidates = ports.filter(p =>
    (p.path || "").includes("ttyUSB") || (p.path || "").includes("ttyACM")
  );

  if (USB_VID && USB_PID) {
    const byId = ports.filter(p =>
      (p.vendorId || "").toLowerCase() === USB_VID &&
      (p.productId || "").toLowerCase() === USB_PID
    );
    if (byId.length) return byId[0].path;
  }
  if (candidates.length) return candidates[0].path;

  // Fallback: first port
  if (ports.length) return ports[0].path;
  return "";
}

async function ensurePortPath() {
  if (SERIAL_PORT) return SERIAL_PORT;
  SERIAL_PORT = await autoDiscoverPort();
  if (!SERIAL_PORT) {
    console.error("[serial] no serial ports found (will retry).");
  } else {
    console.log(`[serial] auto-discovered port: ${SERIAL_PORT}`);
  }
  return SERIAL_PORT;
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(() => {
    if (!port || !port.isOpen) return;
    try { port.write(ochGetCommandBytes); } catch (e) {
      console.error("[serial] write error:", e.message);
    }
    // No-data watchdog
    if (lastDataTs && (Date.now() - lastDataTs) > NO_DATA_TIMEOUT_MS) {
      console.warn(`[serial] no data in ${NO_DATA_TIMEOUT_MS}ms — resetting port.`);
      resetPort();
    }
  }, POLL_MS);
}
function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

function resetPort() {
  try { if (port && port.isOpen) port.close(); } catch {}
  // openWithRetry will be triggered by 'close' or we call it explicitly:
  openWithRetry();
}

async function openWithRetry() {
  if (opening) return;
  opening = true;

  await ensurePortPath();
  if (!SERIAL_PORT) {
    // No port yet—retry discovery
    opening = false;
    return setTimeout(openWithRetry, Math.min(openBackoff, MAX_BACKOFF_MS));
  }

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

  port.on("data", (chunk) => {
    rxBuf = Buffer.concat([rxBuf, chunk]);
  
    while (rxBuf.length >= ochBlockSize) {
      const frame = rxBuf.subarray(0, ochBlockSize);
      rxBuf = rxBuf.subarray(ochBlockSize);
  
      // ---- DEBUG dump (once) ----
      if (DEBUG && !lastFrame) {
        const hex = [...frame]
          .slice(0, 64)
          .map(b => b.toString(16).padStart(2, "0"))
          .join(" ");
        console.log("[debug] first frame (first 64 bytes):", hex);
  
        const beRaw = (/^S/i.test(rpmDef.type) ? s16be(frame, rpmDef.offset) : u16be(frame, rpmDef.offset));
        const leRaw = (/^S/i.test(rpmDef.type) ? s16le(frame, rpmDef.offset) : u16le(frame, rpmDef.offset));
  
        console.log(`[debug] rpm raw bytes @ offset=${rpmDef.offset}`);
        console.log(`        big-endian:    ${beRaw}`);
        console.log(`        little-endian: ${leRaw}`);
        console.log(`        scale:         ${rpmDef.scale}`);
        console.log(`        translate:     ${rpmDef.translate}`);
      }
  
      // ---- Correct RPM decode using type + scale + transl. + endianness fallback ----
      const rpm = Math.round(readScalar16(frame, rpmDef));
  
      lastFrame = {
        type: "frame",
        ts: Date.now() / 1000,
        data: { rpm }
      };
  
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
  // re-discover in case tty device number changed
  SERIAL_PORT = ""; // force rediscovery next time
  openBackoff = Math.min(openBackoff * 1.7, MAX_BACKOFF_MS);
  setTimeout(openWithRetry, openBackoff);
}

// Kick it off
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
    res.end(JSON.stringify(lastFrame || { type:"frame", ts: Date.now()/1000, data: { rpm: 0 } }));
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
  ws.send(JSON.stringify({ type: "hello", note: "ms2 serial bridge (rpm only), resilient" }));
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

// ------------------ Hardening ------------------
process.on("uncaughtException", (e) => {
  console.error("[fatal] uncaughtException:", e);
  // keep process alive; serial will be re-opened by timers
});
process.on("unhandledRejection", (e) => {
  console.error("[fatal] unhandledRejection:", e);
});
