# Kafka Real-Time Log Monitoring & Alerting System

A small event-streaming pipeline: applications emit logs to **Apache Kafka**, a **consumer group** analyses them in real time, and **alerts** are published to a separate topic and picked up by a notifier. Everything runs locally with one command via **Docker Compose**.

## Architecture

```
 producer ──► [app-logs] (3 partitions) ──► log-processor (consumer group, scalable)
 (simulated        │                              │
  services)        │ malformed JSON               ├──► [alerts] ──► alert-notifier (own group) ──► console + data/alerts.jsonl
                   └──────────────────────────────┴──► [app-logs.dlq] (dead-letter queue)
```

| Component | File | Role |
|---|---|---|
| Producer | `src/producer.py` | Emits JSON logs for 4 services, keyed by service name; injects incidents + a few malformed messages for demo |
| Processor | `src/processor.py` | Consumer group `log-processor`; validates, runs detection rules, publishes alerts, routes bad messages to DLQ |
| Detector | `src/detector.py` | Pure Python rules (no Kafka) → fully unit-tested |
| Notifier | `src/alert_notifier.py` | Separate consumer group on `alerts`; prints + stores alerts |
| Broker | `docker-compose.yml` | Single-node Kafka in KRaft mode (no ZooKeeper) + one-shot topic creation |

## Detection rules

| Alert | Trigger |
|---|---|
| `CRITICAL_EVENT` | Any `CRITICAL` log |
| `HIGH_ERROR_RATE` | ≥ 5 `ERROR` logs from one service in 30 s (sliding window) |
| `BRUTE_FORCE_SUSPECT` | ≥ 5 `login_failed` events from one IP in 60 s |

A 60 s per-key cooldown prevents alert spam. All thresholds are env-configurable (`src/config.py`).

## Run it

Requirements: Docker + Docker Compose.

```bash
git clone <your-repo-url> && cd kafka-log-monitor
make up            # or: docker compose up -d --build
make logs          # watch processor + alerts live (incidents are injected every ~30 s)
```

Useful commands:

```bash
make scale         # 3 processor replicas -> Kafka splits the 3 partitions among them
make groups        # show partitions, offsets and LAG per consumer
make dlq           # read the dead-letter topic
cat data/alerts.jsonl
make down          # stop and delete volumes
```

Run the tests (no Kafka needed):

```bash
pip install -r requirements-dev.txt && pytest -q
```

## Design decisions (good interview talking points)

- **Message key = service name.** Same key → same partition → per-service ordering, which the sliding-window rules rely on.
- **At-least-once delivery.** Auto-commit is off; offsets are committed only *after* alerts are flushed. A crash may replay a few messages (duplicate alert possible) but never loses one.
- **Producer reliability.** `acks=all` + `enable.idempotence=true` → no loss or duplicates from producer retries.
- **Consumer groups.** `log-processor` scales horizontally up to the partition count; `alert-notifier` is an independent group with its own offsets reading a different topic.
- **Dead-letter queue.** Malformed messages are routed to `app-logs.dlq` with the error in a header instead of blocking the partition.
- **Event-time windows.** Rules use the event's timestamp, not wall-clock, so behaviour is deterministic and replayable in tests.
- **Graceful shutdown.** SIGTERM → flush, final commit, `consumer.close()` so the group rebalances quickly.
- **Two listeners** (`INTERNAL kafka:19092`, `EXTERNAL localhost:9092`) so both containers and host scripts can connect.

## Known limitations / next steps

- Window state is in-memory: after a rebalance or restart it is rebuilt only from new events. Production fix: Kafka Streams / Flink, or a state store (e.g. Redis).
- Single broker, replication factor 1. Production would use ≥ 3 brokers, RF 3, `min.insync.replicas=2`.
- Duplicate alerts are possible under at-least-once; could add idempotent alert IDs or Kafka transactions for exactly-once.
- Add Prometheus/Grafana for consumer lag and Slack/email in the notifier.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `NoBrokersAvailable` / connection refused from host | Use `localhost:9092` on host, `kafka:19092` inside containers (`KAFKA_BOOTSTRAP_SERVERS`) |
| Processor starts but gets nothing | Topics not created yet: `docker compose logs init-topics`; ensure it exited 0 |
| Consumer lag keeps growing | Check `make groups`; scale processors (max = 3 partitions) |
| Repeated rebalances | Processing too slow between `poll()` calls (`max.poll.interval.ms`) or container restarts |
| Idle replica when scaling to 4+ | More consumers than partitions → extras stay idle by design |
| Port 9092 already in use | Stop the other Kafka or change the host port mapping |
| Want to replay everything | `docker compose down -v` or reset offsets with `kafka-consumer-groups.sh --reset-offsets` |
