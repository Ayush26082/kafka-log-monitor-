"""Simulated application log producer.

- Key = service name  -> all logs of one service land on the same partition,
  so per-service ordering is preserved (needed for the sliding-window rules).
- acks=all + idempotence -> no loss / no duplicates from producer retries.
- Periodically injects an "incident" (error burst, brute-force, critical)
  so alerts fire during a live demo.
"""
import json
import logging
import os
import random
import signal
import time
import uuid

from confluent_kafka import Producer

from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

SERVICES = ["auth-service", "payment-service", "api-gateway", "notification-service"]
NORMAL_MSGS = {
    "INFO": ["request completed", "cache hit", "user session refreshed"],
    "WARN": ["slow response", "retrying upstream call"],
    "ERROR": ["db connection timeout", "upstream 502", "payment gateway declined"],
}
RATE = float(os.getenv("PRODUCER_RATE", "5"))
INCIDENT_EVERY = float(os.getenv("INCIDENT_EVERY_SEC", "30"))
MALFORMED_RATIO = float(os.getenv("MALFORMED_RATIO", "0.01"))

running = True


def _stop(*_):
    global running
    running = False


def make_event(service=None, level=None, message=None, **extra) -> dict:
    level = level or random.choices(["INFO", "WARN", "ERROR"], weights=[85, 10, 5])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "ts": time.time(),
        "service": service or random.choice(SERVICES),
        "level": level,
        "message": message or random.choice(NORMAL_MSGS.get(level, ["event"])),
        **extra,
    }


def incident_events():
    kind = random.choice(["error_burst", "brute_force", "critical"])
    log.info("INJECTING INCIDENT: %s", kind)
    if kind == "error_burst":
        svc = random.choice(SERVICES)
        return [make_event(svc, "ERROR", "db connection timeout") for _ in range(7)]
    if kind == "brute_force":
        ip = f"203.0.113.{random.randint(1, 254)}"
        return [make_event("auth-service", "WARN", "login failed",
                           event_type="login_failed", ip=ip, user="admin") for _ in range(6)]
    return [make_event("payment-service", "CRITICAL", "payment database unreachable")]


def on_delivery(err, msg):
    if err:
        log.error("delivery failed: %s", err)


def main():
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
    })
    log.info("producing to '%s' at ~%s msg/s via %s", config.LOG_TOPIC, RATE, config.BOOTSTRAP_SERVERS)
    next_incident = time.time() + INCIDENT_EVERY

    while running:
        if random.random() < MALFORMED_RATIO:
            producer.produce(config.LOG_TOPIC, key=b"bad", value=b"{this is not valid json",
                             on_delivery=on_delivery)
        else:
            batch = [make_event()]
            if time.time() >= next_incident:
                batch += incident_events()
                next_incident = time.time() + INCIDENT_EVERY
            for ev in batch:
                producer.produce(config.LOG_TOPIC, key=ev["service"].encode(),
                                 value=json.dumps(ev).encode(), on_delivery=on_delivery)
        producer.poll(0)
        time.sleep(1.0 / RATE)

    log.info("shutting down, flushing...")
    producer.flush(10)


if __name__ == "__main__":
    main()
