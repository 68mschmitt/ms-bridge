#!/usr/bin/env python3
"""
pyefi_supervisor.py
- Launches: ./pyefi.py run collectMS ...
- Ensures: serial device exists, INI exists, Redis reachable
- Restarts: on process exit or if no Redis messages for --stale-sec
"""

import argparse, os, sys, time, json, signal, subprocess, shutil
from pathlib import Path

try:
    import redis
except ImportError:
    print("ERROR: redis-py not installed. Activate your venv and: pip install redis")
    sys.exit(1)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ini", required=True, help="Path to MS2/Extra mainController.ini")
    ap.add_argument("--usb", required=True, help="Serial dev, e.g. /dev/ttyUSB0 or /dev/ms2")
    ap.add_argument("--channel", default="ms2:realtime", help="Redis pubsub channelKey")
    ap.add_argument("--pyefi", default="./pyefi.py", help="Path to pyEfi launcher")
    ap.add_argument("--redis-sock", default="/var/run/redis/redis.sock", help="Redis UNIX socket path (or use --redis-host/--redis-port)")
    ap.add_argument("--redis-host", default=None, help="Redis host if not using UNIX socket")
    ap.add_argument("--redis-port", type=int, default=6379, help="Redis port if not using UNIX socket")
    ap.add_argument("--stale-sec", type=float, default=5.0, help="Restart if no messages for this many seconds")
    ap.add_argument("--grace-sec", type=float, default=5.0, help="Startup grace before staleness check")
    ap.add_argument("--log-file", default=None, help="Optional: append child stdout/stderr here")
    ap.add_argument("--max-backoff", type=float, default=30.0, help="Max restart backoff seconds")
    return ap.parse_args()

def mk_redis_client(args):
    if args.redis_host:
        return redis.Redis(host=args.redis_host, port=args.redis_port)
    return redis.Redis(unix_socket_path=args.redis_sock)

def wait_for_path(path, desc, poll=0.5, timeout=None):
    print(f"[supervisor] Waiting for {desc}: {path}")
    start = time.monotonic()
    while not Path(path).exists():
        time.sleep(poll)
        if timeout and (time.monotonic() - start) > timeout:
            return False
    print(f"[supervisor] Found {desc}.")
    return True

def spawn_collector(args, log_handle):
    cmd = [
        sys.executable, args.pyefi, "run", "collectMS",
        "--iniFile", args.ini,
        "--usbLoc", args.usb,
        "--channelKey", args.channel
    ]
    env = os.environ.copy()
    # ensure venv PATH is honored if running under systemd without login shell
    env.setdefault("PYTHONUNBUFFERED", "1")
    print(f"[supervisor] Starting collector: {' '.join(cmd)}")
    return subprocess.Popen(
        cmd,
        stdout=log_handle or subprocess.PIPE,
        stderr=log_handle or subprocess.STDOUT,
        preexec_fn=os.setsid,  # so we can kill the whole group
        env=env
    )

def kill_collector(proc, reason=""):
    if proc and proc.poll() is None:
        print(f"[supervisor] Stopping collector (reason: {reason}) pid={proc.pid}")
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        # wait a moment, then SIGKILL if needed
        for _ in range(20):
            if proc.poll() is not None:
                return
            time.sleep(0.1)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass

def main():
    args = parse_args()

    # Pre-flight checks
    if not Path(args.pyefi).exists():
        print(f"ERROR: pyEfi launcher not found: {args.pyefi}")
        sys.exit(2)
    if not wait_for_path(args.ini, "INI file"):
        print(f"ERROR: INI not found: {args.ini}")
        sys.exit(3)
    if not wait_for_path(args.usb, "serial device"):
        print(f"ERROR: Serial device not found: {args.usb}")
        sys.exit(4)

    # Redis connectivity
    r = mk_redis_client(args)
    try:
        r.ping()
    except Exception as e:
        print(f"ERROR: Cannot connect to Redis ({e})")
        sys.exit(5)

    # Prepare PubSub
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe(args.channel)
    print(f"[supervisor] Subscribed to Redis channel: {args.channel}")

    # Optional log file
    log_handle = None
    if args.log_file:
        Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
        log_handle = open(args.log_file, "a", buffering=1)

    # Restart loop with backoff
    backoff = 1.0
    collector = None
    last_msg = 0.0
    start_time = 0.0

    try:
        while True:
            # Re-check serial device presence between restarts
            if not Path(args.usb).exists():
                print("[supervisor] Serial device missing; waiting...")
                wait_for_path(args.usb, "serial device")

            collector = spawn_collector(args, log_handle)
            start_time = time.monotonic()
            last_msg = 0.0
            backoff = 1.0  # reset backoff after a successful spawn

            # Monitor loop
            while True:
                # 1) if child died, break to restart
                ret = collector.poll()
                if ret is not None:
                    print(f"[supervisor] Collector exited with code {ret}")
                    break

                # 2) read any stdout to log if not using a file (keep pipe from filling)
                if log_handle is None and collector.stdout:
                    try:
                        line = collector.stdout.readline()
                        if line:
                            sys.stdout.write(f"[collector] {line.decode(errors='replace')}")
                    except Exception:
                        pass

                # 3) check for fresh Redis messages
                try:
                    msg = p.get_message(timeout=0.1)
                except Exception as e:
                    print(f"[supervisor] Redis read error: {e}")
                    msg = None

                if msg and msg.get("type") == "message":
                    last_msg = time.monotonic()

                # 4) staleness detection (allow grace on startup)
                now = time.monotonic()
                if last_msg == 0.0:
                    if (now - start_time) < (args.grace_sec + args.stale_sec):
                        time.sleep(0.05)
                        continue
                else:
                    if (now - last_msg) > args.stale_sec:
                        print(f"[supervisor] No messages for {args.stale_sec}s -> restarting")
                        kill_collector(collector, reason="stale")
                        break

                time.sleep(0.05)

            # Restart with backoff
            print(f"[supervisor] Restarting in {backoff:.1f}s ...")
            time.sleep(backoff)
            backoff = min(args.max_backoff, backoff * 2.0)

    except KeyboardInterrupt:
        print("[supervisor] Ctrl-C received; shutting down.")
    finally:
        kill_collector(collector, reason="shutdown")
        if log_handle:
            log_handle.close()
        try:
            p.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
