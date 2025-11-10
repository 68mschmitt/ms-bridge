#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ms_bridge_redis.py
Consume pyEfi Megasquirt realtime data from Redis PubSub and expose a HUDIY-friendly WS/HTTP API.

WebSocket:
  ws://0.0.0.0:8765/v1/stream

HTTP:
  GET /v1/channels  -> {"fields": { ... }}
  GET /v1/snapshot  -> last frame
  GET /v1/health    -> status

Run examples:
  # UNIX socket (pyEfi default)
  python3 ms_bridge_redis.py --redis-sock /var/run/redis/redis.sock

  # TCP
  python3 ms_bridge_redis.py --redis-host 127.0.0.1 --redis-port 6379

  # Custom channel and publish rate
  python3 ms_bridge_redis.py --channel ms2:realtime --hz 20
"""

import argparse, asyncio, json, time, signal, contextlib
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple

from aiohttp import web, WSMsgType
import redis.asyncio as redis  # async redis-py

# ---------------------- Config ----------------------

@dataclass
class BridgeConfig:
    hz: float = 20.0
    ws_host: str = "0.0.0.0"
    ws_port: int = 8765
    channel: str = "ms2:realtime"
    redis_sock: Optional[str] = None
    redis_host: Optional[str] = None
    redis_port: int = 6379

    fields: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        "rpm":     {"unit": "rpm"},
        "map_kpa": {"unit": "kPa"},
        "tps_pct": {"unit": "%"},
        "clt_f":   {"unit": "°F"},
        "afr":     {"unit": ""},
        "batt_v":  {"unit": "V"},
        "mat_f":   {"unit": "°F"},
    })

# ---------------------- Normalization ----------------------

def _first(d: Dict[str, Any], *keys) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
        # case-insensitive lookup
        for kk in d.keys():
            if kk.lower() == k.lower() and d[kk] is not None:
                return d[kk]
    return None

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _c_to_f(val_c: Optional[float]) -> Optional[float]:
    if val_c is None:
        return None
    return val_c * 9.0 / 5.0 + 32.0

def normalize_payload(src: Dict[str, Any]) -> Dict[str, Any]:
    """Map pyEfi payload into stable keys expected by the HUDIY gauges."""
    # rpm
    rpm = _to_float(_first(src, "rpm", "RPM", "engine.rpm"))
    # map (prefer kPa)
    map_kpa = _to_float(_first(src, "map_kpa", "map", "MAP", "engine.map"))
    # tps (%)
    tps = _to_float(_first(src, "tps_pct", "tps", "TPS"))
    # CLT + MAT (accept either F already or C -> convert)
    clt = _to_float(_first(src, "clt_f", "CLT_f", "Coolant_f", "coolant_f", "cltF"))
    if clt is None:
        clt_c = _to_float(_first(src, "clt", "Coolant", "CLT"))
        clt = _c_to_f(clt_c) if clt_c is not None else None

    mat = _to_float(_first(src, "mat_f", "IAT_f", "matF", "iat_f"))
    if mat is None:
        mat_c = _to_float(_first(src, "mat", "IAT"))
        mat = _c_to_f(mat_c) if mat_c is not None else None

    afr = _to_float(_first(src, "afr", "AFR", "lambda", "Lambda"))
    batt_v = _to_float(_first(src, "batt_v", "vBatt", "bat", "battery"))

    out = {}
    if rpm is not None:     out["rpm"] = int(round(rpm))
    if map_kpa is not None: out["map_kpa"] = round(map_kpa, 1)
    if tps is not None:     out["tps_pct"] = round(tps, 1)
    if clt is not None:     out["clt_f"] = round(clt, 1)
    if afr is not None:     out["afr"] = round(afr, 2)
    if batt_v is not None:  out["batt_v"] = round(batt_v, 2)
    if mat is not None:     out["mat_f"] = round(mat, 1)
    return out

def parse_pubsub_message(msg) -> Optional[Dict[str, Any]]:
    """Handle bytes/str payload, return dict or None."""
    if msg["type"] != "message":
        return None
    payload = msg.get("data")
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(payload, str):
        payload = payload.strip()
        try:
            return json.loads(payload)
        except Exception:
            # sometimes pyEfi may already give dict (rare), or non-json: ignore
            return None
    if isinstance(payload, dict):
        return payload
    return None

# ---------------------- Redis consumer ----------------------

class RedisReader:
    def __init__(self, cfg: BridgeConfig):
        self.cfg = cfg
        self._r: Optional[redis.Redis] = None
        self._pubsub = None
        self.last_data_ts: float = 0.0
        self.connected: bool = False

    async def connect(self):
        if self._r:
            await self.close()
        if self.cfg.redis_sock:
            self._r = redis.Redis(unix_socket_path=self.cfg.redis_sock)
        else:
            host = self.cfg.redis_host or "127.0.0.1"
            port = int(self.cfg.redis_port or 6379)
            self._r = redis.Redis(host=host, port=port)
        self._pubsub = self._r.pubsub()
        await self._pubsub.subscribe(self.cfg.channel)
        self.connected = True

    async def close(self):
        try:
            if self._pubsub:
                await self._pubsub.close()
        finally:
            self._pubsub = None
        try:
            if self._r:
                await self._r.close()
        finally:
            self._r = None
            self.connected = False

    async def next_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Wait for next message (async); returns normalized dict or None on timeout."""
        if not (self._r and self._pubsub):
            return None
        try:
            msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
            if not msg:
                return None
            raw = parse_pubsub_message(msg)
            if not raw:
                return None
            self.last_data_ts = time.time()
            return normalize_payload(raw)
        except Exception:
            return None

# ---------------------- Bridge app (WS/HTTP) ----------------------

class BridgeApp:
    def __init__(self, cfg: BridgeConfig, reader: RedisReader):
        self.cfg = cfg
        self.reader = reader
        self.app = web.Application()
        self.app.add_routes([
            web.get("/v1/stream", self.ws_handler),
            web.get("/v1/channels", self.channels_handler),
            web.get("/v1/snapshot", self.snapshot_handler),
            web.get("/v1/health", self.health_handler),
        ])
        self.clients = set()
        self.last_frame: Optional[Dict[str, Any]] = None
        self._producer_task: Optional[asyncio.Task] = None

    async def start(self):
        self._producer_task = asyncio.create_task(self.producer_loop())

    async def channels_handler(self, request):
        return web.json_response({"fields": self.cfg.fields})

    async def snapshot_handler(self, request):
        return web.json_response(self.last_frame or {"type": "frame", "ts": time.time(), "data": {}})

    async def health_handler(self, request):
        age_ms = int(1000 * (time.time() - self.reader.last_data_ts)) if self.reader.last_data_ts else None
        return web.json_response({
            "ok": bool(self.reader.connected),
            "connected": self.reader.connected,
            "lastDataAgoMs": age_ms,
            "clients": len(self.clients),
            "channel": self.cfg.channel,
        })

    async def ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.clients.add(ws)
        try:
            await ws.send_str(json.dumps({"type": "hello", "note": "pyEfi/Redis bridge"}))
            if self.last_frame:
                await ws.send_str(json.dumps(self.last_frame))
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    if msg.data.strip().lower() == "ping":
                        await ws.send_str("pong")
                elif msg.type == WSMsgType.ERROR:
                    break
        finally:
            self.clients.discard(ws)
        return ws

    async def producer_loop(self):
        period = 1.0 / max(self.cfg.hz, 1.0)
        backoff = 0.5
        while True:
            try:
                if not self.reader.connected:
                    await self.reader.connect()
                    backoff = 0.5

                # Try to pull a fresh message; if none, reuse last data
                data = await self.reader.next_message(timeout=period)
                if data is None and self.last_frame:
                    # keep broadcasting last known data at steady Hz
                    data = self.last_frame["data"]

                if data is not None:
                    frame = {"type": "frame", "ts": time.time(), "data": data}
                    self.last_frame = frame
                    if self.clients:
                        payload = json.dumps(frame, separators=(",", ":"))
                        await asyncio.gather(*[
                            c.send_str(payload) for c in list(self.clients) if not c.closed
                        ], return_exceptions=True)

                await asyncio.sleep(period)

            except Exception:
                # On any error, close and backoff reconnect
                await self.reader.close()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 8.0)

# ---------------------- Main ----------------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hz", type=float, default=20.0, help="broadcast rate to WS clients")
    ap.add_argument("--wshost", default="0.0.0.0")
    ap.add_argument("--wsport", type=int, default=8765)
    ap.add_argument("--channel", default="ms2:realtime")

    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--redis-sock", help="UNIX socket path, e.g. /var/run/redis/redis.sock")
    grp.add_argument("--redis-host", help="Redis host (if not using UNIX socket)")
    ap.add_argument("--redis-port", type=int, default=6379)

    return ap.parse_args()

async def main_async():
    args = parse_args()
    cfg = BridgeConfig(
        hz=args.hz,
        ws_host=args.wshost, ws_port=args.wsport,
        channel=args.channel,
        redis_sock=args.redis_sock,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
    )

    reader = RedisReader(cfg)
    app = BridgeApp(cfg, reader)

    runner = web.AppRunner(app.app)
    await runner.setup()
    site = web.TCPSite(runner, host=cfg.ws_host, port=cfg.ws_port)
    await site.start()
    await app.start()

    print(f"[ws] ws://{cfg.ws_host}:{cfg.ws_port}/v1/stream")
    print(f"[http] /v1/channels  /v1/snapshot  /v1/health")
    print(f"[redis] channel={cfg.channel} via " + (cfg.redis_sock or f"{cfg.redis_host or '127.0.0.1'}:{cfg.redis_port}"))
    print(f"[rate] {cfg.hz} Hz")

    stop = asyncio.Future()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: (not stop.done()) and stop.set_result(True))
    await stop

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
