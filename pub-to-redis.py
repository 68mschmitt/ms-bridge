#!/usr/bin/env python3
import json
import time
import redis

# Use Redis UNIX socket (default for pyEfi)
r = redis.Redis(unix_socket_path="/var/run/redis/redis.sock")

CHANNEL = "test"   # match your collector key if needed
STEP = 200         # rpm step per update
DELAY = 0.03       # ~33 Hz update rate (~fast gauge movement)

rpm = 0
direction = 1  # 1 = increasing, -1 = decreasing

print(f"Publishing fake RPM on channel '{CHANNEL}' ...")
print("Bounce pattern 0 → 8000 → 0 → ...")

while True:
    # Update RPM
    rpm += STEP * direction

    if rpm >= 8000:
        rpm = 8000
        direction = -1  # reverse at top

    elif rpm <= 0:
        rpm = 0
        direction = 1   # reverse at bottom

    # Build payload — include other static values if you want
    payload = {
        "RPM": rpm,
        "AFR": 14.7,
        "CLT": 180,
        "MAP": 45,
        "TPS": 20,
        "ts": time.time()
    }

    # Publish
    r.publish(CHANNEL, json.dumps(payload))

    # Wait
    time.sleep(DELAY)
