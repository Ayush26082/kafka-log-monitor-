"""Consumer-group processor: app-logs -> detection -> alerts (+ DLQ for bad messages).

Delivery semantics: AT-LEAST-ONCE.
  enable.auto.commit=False and offsets are committed only AFTER the resulting
  alerts have been flushed to Kafka. A crash can replay a few messages
  (possible duplicate alert) but never loses one.

Scaling: run N copies with the same group.id; Kafka splits the 3 partitions
among them. More consumers than partitions = idle consumers.
"""
import json
import logging
import signal

from confluent_kafka import Consumer, KafkaError, Producer

from . import config
from .detector import Detector, parse_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [processor] %(message)s")
log = logging.getLogger(__name__)

COMMIT_EVERY = 20
running = True


def _stop(*_):
    global running
    running = False


def on_assign(consumer, partitions):
    log.info("partitions assigned: %s", [p.partition for p in partitions])


def on_revoke(consumer, partitions):
    log.info("partitions revoked: %s", [p.partition for p in partitions])


def main():
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.PROCESSOR_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "acks": "all",
                         "enable.idempotence": True})
    consumer.subscribe([config.LOG_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    detector = Detector()
    processed = since_commit = 0
    log.info("group '%s' consuming '%s'", config.PROCESSOR_GROUP, config.LOG_TOPIC)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", msg.error())
                continue

            try:
                event = parse_event(msg.value())
            except ValueError as exc:
                log.warning("bad message at p%d@%d -> DLQ (%s)", msg.partition(), msg.offset(), exc)
                producer.produce(config.DLQ_TOPIC, key=msg.key(), value=msg.value(),
                                 headers={"error": str(exc).encode()})
                producer.poll(0)
            else:
                for alert in detector.process(event):
                    log.warning("ALERT %s [%s] %s", alert.type, alert.severity, alert.detail)
                    producer.produce(config.ALERT_TOPIC, key=alert.key.encode(),
                                     value=json.dumps(alert.to_dict()).encode())
                    producer.poll(0)

            processed += 1
            since_commit += 1
            if since_commit >= COMMIT_EVERY:
                producer.flush(5)                     # alerts durable first...
                consumer.commit(asynchronous=False)   # ...then commit offsets
                since_commit = 0
                log.info("processed=%d (offsets committed)", processed)
    finally:
        log.info("shutting down...")
        producer.flush(10)
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass
        consumer.close()  # leaves the group cleanly -> fast rebalance


if __name__ == "__main__":
    main()
