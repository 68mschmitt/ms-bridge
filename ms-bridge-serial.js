// ms-bridge-serial.js  —  MS2/Extra Serial → WebSocket (RPM) bridge
// - Robust: auto-discover, auto-reconnect, no-data watchdog
// - Decoding: uses mainController.ini (ochGetCommand, ochBlockSize, rpm offset/type/scale)
// - Endianness: defaults to INI's "big", auto-falls back to LE if implausible; flags to force
// - WebSocket: ws://<host>:<port>/v1/stream  (frames: {type:"frame", ts, data:{ rpm }})
// - HTTP:      /v1/channels, /v1/snapshot, /v1/health

import fs from "fs";
import http from "http";
import { WebSocketServer } from "ws";
import ini from "ini";

// serialport is CommonJS on many Pi builds; use default import & destructure:
import serialPkg from "serialport";
const { SerialPort } = serialPkg;
const listSerial = async () => SerialPort.list();

// ------------------ CLI / ENV ------------------
const args = Object.fromEntries(process.argv.slice(2).map(a => {
  const [k, v] = a.split("=");
  return [k.replace(/^--/, ""), v ?? true];
}));

let SERIAL_PORT = args.port || process.env.MS_PORT || "";     // auto-discover if empty
const BAUD = Number(args.baud || process.env.MS_BAUD || 115200);
const INI_PATH = args.ini || process.env.MS_INI;
const WS_PORT = Number(args.wsport || process.env.WS_PORT || 8765);
const POLL_MS = Number(args.poll || process.env.POLL_MS || 50);          // ~20 Hz
const NO_DATA_TIMEOUT_MS = Number(args.nodata || 3000);                  // watchdog for silent links
const MAX_BACKOFF_MS = 8000;

const FORCE_ENDIAN = (args.endian || "").toLowerCase(); // "le" | "be" | ""
const RPM_OFFSET_OVERRIDE = (args.rpmOffset !== undefined) ? Number(args.rpmOffset) : null;
const RPM_SCALE_OVERRIDE  = (args.rpmScale  !== undefined) ? Number(args.rpmScale)  : null;
const DEBUG = !!args.debug;

// Optional VID/PID filters for discovery
const USB_VID = (args.vid || process.env.MS_USB_VID || "").toLowerCase();
const USB_PID = (args.pid || process.env.MS_USB_PID || "").toLowerCase();

if (!INI_PATH) {
  console.error("Missing --ini=/path/to/mainController.ini");
  process.exit(1);
}

// ------------------ Parse INI ------------------
const rawIni = fs.readFileSync(INI_PATH, "utf8");

// ochBlockSize
const ochBlockSize = (() => {
  const m = rawIni.match(/^\s*ochBlockSize\s*=\s*(\d+)/mi);
  if (!m) throw new Error("ochBlockSize not found in INI");
  return parseInt(m[1], 10);
})();

// ochGetCommand (quoted, may include escapes like \x06)
const ochGetCmdStr = (() => {
  const m = rawIni.match(/^\s*ochGetCommand\s*=\s*"([^"]+)"/mi);
  if (!m) throw new Error("ochGetCommand not found in INI");
  return m[1];
})();

// INI-declared endianness (default to "big" if absent)
const iniEndian = (() => {
  const m = rawIni.match(/^\s*endianness\s*=\s*(big|little)/mi);
  return m ? m[1].toLowerCase() : "big";
})();

// RPM definition: rpm = scalar, U16|S16, <offset>, "label", <scale>, <translate>
const rpmDef = (() => {
  const re = /^\s*rpm\s*=\s*scalar\s*,\s*([SU]\d+)\s*,\s*(\d+)\s*,\s*"[^"]*"\s*,\s*([+-]?\d*\.?\d+)\s*,\s*([+-]?\d*\.?\d+)/mi;
  const m = rawIni.match(re);
  if (!m) throw new Error("Couldn't find 'rpm = scalar, ...' in INI OutputChannels.");
  return {
    type: m[1],                      // U16/S16
    offset: parseInt(m[2], 10),
    scale: parseFloat(m[3]),
    translate: parseFloat(m[4])
  };
})();

console.log(`[ini] ochBlockSize=${ochBlockSize}, ochGetCommand="${ochGetCmdStr}"`);
console.log(`[ini] endianness=${iniEndian}`);
console.log(`[ini] rpm: type=${rpmDef.type}, offset=${rpmDef.offset}, scale=${rpmDef.scale}, translate=${rpmDef.translate}`);

// Turn "A" or "a\x00\x06" into bytes
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

// ------------------ Helpers ------------------
function u16be(buf, off){ return (buf[off] << 8) | buf[off+1]; }
function u16le(buf, off){ return (buf[off+1] << 8) | buf[off]; }
function s16be(buf, off){ let v = u16be(buf, off); return (v & 0x8000) ? v - 0x10000 : v; }
function s16le(buf, off){ let v = u16le(buf, off); return (v & 0x8000) ? v - 0x10000 : v; }

function plausibleRpm(v){ return v >= 400 && v <= 8000; }

// Scan entire frame for LE/BE 16-bit values that look like RPM (~idle..redline)
function findRpmCandidates(buf, limit=10){
  const out = [];
  for (let off = 0; off <= buf.length-2; off++){
    const le = u16le(buf, off);
    const be = u16be(buf, off);
    if (plausibleRpm(le)) out.push({off, endian:"le", value:le});
    if (plausibleRpm(be)) out.push({off, endian:"be", value:be});
  }
  out.sort((a,b) => Math.abs(a.value-900) - Math.abs(b.value-900));
  return out.slice(0, limit);
}

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
  if (ports.length) return ports[0].path;
  return "";
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
  openWithRetry();
}

async function openWithRetry() {
  if (opening) return;
  opening = true;

  await ensurePortPath();
  if (!SERIAL_PORT) {
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
    lastDataTs = Date.now();
    rxBuf = Buffer.concat([rxBuf, chunk]);

    while (rxBuf.length >= ochBlockSize) {
      const frame = rxBuf.subarray(0, ochBlockSize);
      rxBuf = rxBuf.subarray(ochBlockSize);

      // ---- Decode RPM ----
      let rpm;
      const signed = /^S/i.test(rpmDef.type);
      const readBE = signed ? s16be : u16be;
      const readLE = signed ? s16le : u16le;

      // Choose routing: override > forced endian > INI default (with plausibility fallback) > scan
      if (RPM_OFFSET_OVERRIDE !== null) {
        const raw = (FORCE_ENDIAN==="le") ? readLE(frame, RPM_OFFSET_OVERRIDE)
                  : (FORCE_ENDIAN==="be") ? readBE(frame, RPM_OFFSET_OVERRIDE)
                  : readLE(frame, RPM_OFFSET_OVERRIDE); // default to LE if unspecified when overriding
        const scale = (RPM_SCALE_OVERRIDE!=null) ? RPM_SCALE_OVERRIDE : rpmDef.scale;
        rpm = Math.round(raw * scale + rpmDef.translate);

      } else {
        // Start from INI offset
        const beRaw = readBE(frame, rpmDef.offset);
        const leRaw = readLE(frame, rpmDef.offset);
        const beScaled = beRaw * rpmDef.scale + rpmDef.translate;
        const leScaled = leRaw * rpmDef.scale + rpmDef.translate;

        if (FORCE_ENDIAN==="le") {
          rpm = Math.round(leScaled);
        } else if (FORCE_ENDIAN==="be") {
          rpm = Math.round(beScaled);
        } else {
          // Default to INI's endianness if plausible, else try the other, else scan
          const iniScaled = (iniEndian === "big") ? beScaled : leScaled;
          if (plausibleRpm(iniScaled)) {
            rpm = Math.round(iniScaled);
          } else if (plausibleRpm(leScaled)) {
            rpm = Math.round(leScaled);
            if (!globalThis._rpmEndianHint) {
              console.log(`[hint] RPM appears little-endian at INI offset=${rpmDef.offset} (~${rpm}). You can lock with: --endian=le`);
              globalThis._rpmEndianHint = true;
            }
          } else if (plausibleRpm(beScaled)) {
            rpm = Math.round(beScaled);
            if (!globalThis._rpmEndianHint) {
              console.log(`[hint] RPM appears big-endian at INI offset=${rpmDef.offset} (~${rpm}). You can lock with: --endian=be`);
              globalThis._rpmEndianHint = true;
            }
          } else {
            const cands = findRpmCandidates(frame);
            if (cands.length) {
              const best = cands[0];
              rpm = best.value;
              if (!globalThis._rpmHintPrinted){
                console.log(`[hint] RPM likely at offset=${best.off} (${best.endian.toUpperCase()}) ~ ${best.value} rpm`);
                console.log(`       Run again with: --rpmOffset=${best.off} --endian=${best.endian}`);
                globalThis._rpmHintPrinted = true;
              }
            } else {
              rpm = 0;
            }
          }
        }
      }

      if (DEBUG && !globalThis._dumpedOnce) {
        const hex = [...frame].slice(0, 64).map(b => b.toString(16).padStart(2, "0")).join(" ");
        console.log("[debug] first frame (first 64 bytes):", hex);
        const beRaw = u16be(frame, rpmDef.offset);
        const leRaw = u16le(frame, rpmDef.offset);
        console.log(`[debug] rpm raw @ offset=${rpmDef.offset}: BE=${beRaw}, LE=${leRaw}, scale=${rpmDef.scale}, translate=${rpmDef.translate}`);
        globalThis._dumpedOnce = true;
      }

      lastFrame = { type:"frame", ts: Date.now()/1000, data: { rpm } };
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
  SERIAL_PORT = ""; // force rediscovery next time (tty index can change)
  openBackoff = Math.min(openBackoff * 1.7, MAX_BACKOFF_MS);
  setTimeout(openWithRetry, openBackoff);
}

// Kick it off
openWithRetry();

// ------------------ WS & HTTP ------------------
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

// Keep process alive on unexpected errors; reconnection logic will recover.
process.on("uncaughtException", (e) => {
  console.error("[fatal] uncaughtException:", e);
});
process.on("unhandledRejection", (e) => {
  console.error("[fatal] unhandledRejection:", e);
});
