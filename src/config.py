"""Central config. Everything is overridable via environment variables."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

LOG_TOPIC = os.getenv("LOG_TOPIC", "app-logs")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alerts")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "app-logs.dlq")

PROCESSOR_GROUP = os.getenv("PROCESSOR_GROUP", "log-processor")
NOTIFIER_GROUP = os.getenv("NOTIFIER_GROUP", "alert-notifier")

# Detection rules
ERROR_WINDOW_SEC = int(os.getenv("ERROR_WINDOW_SEC", "30"))
ERROR_THRESHOLD = int(os.getenv("ERROR_THRESHOLD", "5"))
BRUTE_FORCE_WINDOW_SEC = int(os.getenv("BRUTE_FORCE_WINDOW_SEC", "60"))
BRUTE_FORCE_THRESHOLD = int(os.getenv("BRUTE_FORCE_THRESHOLD", "5"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "60"))

ALERTS_FILE = os.getenv("ALERTS_FILE", "data/alerts.jsonl")
