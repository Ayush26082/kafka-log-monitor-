"""Second consumer group on the 'alerts' topic.

Shows that independent groups can read the same data: 'log-processor' and
'alert-notifier' each keep their own offsets. Prints alerts and appends them
to data/alerts.jsonl (swap for Slack / email / webhook in real life).
"""
import json
import logging
import os
import signal

from confluent_kafka import Consumer

from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [notifier] %(message)s")
log = logging.getLogger(__name__)
running = True
COLORS = {"CRITICAL": "\033[41m", "HIGH": "\033[91m"}


def _stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    os.makedirs(os.path.dirname(config.ALERTS_FILE) or ".", exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.NOTIFIER_GROUP,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([config.ALERT_TOPIC])
    log.info("waiting for alerts on '%s'...", config.ALERT_TOPIC)
    try:
        with open(config.ALERTS_FILE, "a") as fh:
            while running:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                alert = json.loads(msg.value())
                color = COLORS.get(alert["severity"], "")
                print(f"{color}[{alert['severity']}] {alert['type']}: {alert['detail']}\033[0m", flush=True)
                fh.write(json.dumps(alert) + "\n")
                fh.flush()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
