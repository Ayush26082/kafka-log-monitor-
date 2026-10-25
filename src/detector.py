"""Pure detection logic (no Kafka imports) so it is easy to unit test.

Rules:
  1. CRITICAL_EVENT      - any CRITICAL log fires immediately.
  2. HIGH_ERROR_RATE     - >= N ERROR logs from one service inside a sliding window.
  3. BRUTE_FORCE_SUSPECT - >= N login_failed events from one IP inside a sliding window.

Each alert type has a cooldown per key so one incident does not spam alerts.
Time is taken from the event timestamp (event time), not the wall clock,
which makes the logic deterministic and replayable.
"""
import json
from collections import defaultdict, deque
from dataclasses import dataclass, asdict

from . import config

VALID_LEVELS = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}
REQUIRED_FIELDS = ("ts", "service", "level", "message")


@dataclass
class Alert:
    type: str
    severity: str
    key: str
    detail: str
    ts: float

    def to_dict(self) -> dict:
        return asdict(self)


def parse_event(raw: bytes) -> dict:
    """Decode + validate a raw Kafka message. Raises ValueError if malformed."""
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        raise ValueError(f"missing fields: {missing}")
    if event["level"] not in VALID_LEVELS:
        raise ValueError(f"unknown level: {event['level']}")
    try:
        event["ts"] = float(event["ts"])
    except (TypeError, ValueError) as exc:
        raise ValueError("ts must be numeric") from exc
    return event


class Detector:
    def __init__(self):
        self._errors = defaultdict(deque)         # service -> timestamps of ERROR logs
        self._failed_logins = defaultdict(deque)  # ip -> timestamps of login_failed
        self._last_alert = {}                     # (type, key) -> ts of last alert

    @staticmethod
    def _slide(window: deque, now: float, size: int) -> None:
        while window and now - window[0] > size:
            window.popleft()

    def _cooldown_ok(self, alert_type: str, key: str, now: float) -> bool:
        last = self._last_alert.get((alert_type, key))
        if last is not None and now - last < config.ALERT_COOLDOWN_SEC:
            return False
        self._last_alert[(alert_type, key)] = now
        return True

    def process(self, event: dict) -> list:
        alerts = []
        now, service, level = event["ts"], event["service"], event["level"]

        if level == "CRITICAL" and self._cooldown_ok("CRITICAL_EVENT", service, now):
            alerts.append(Alert("CRITICAL_EVENT", "CRITICAL", service,
                                f"{service}: {event['message']}", now))

        if level in ("ERROR", "CRITICAL"):
            win = self._errors[service]
            win.append(now)
            self._slide(win, now, config.ERROR_WINDOW_SEC)
            if len(win) >= config.ERROR_THRESHOLD and \
               self._cooldown_ok("HIGH_ERROR_RATE", service, now):
                alerts.append(Alert(
                    "HIGH_ERROR_RATE", "HIGH", service,
                    f"{len(win)} errors in {config.ERROR_WINDOW_SEC}s from {service}", now))

        if event.get("event_type") == "login_failed" and event.get("ip"):
            ip = event["ip"]
            win = self._failed_logins[ip]
            win.append(now)
            self._slide(win, now, config.BRUTE_FORCE_WINDOW_SEC)
            if len(win) >= config.BRUTE_FORCE_THRESHOLD and \
               self._cooldown_ok("BRUTE_FORCE_SUSPECT", ip, now):
                alerts.append(Alert(
                    "BRUTE_FORCE_SUSPECT", "HIGH", ip,
                    f"{len(win)} failed logins from {ip} in {config.BRUTE_FORCE_WINDOW_SEC}s", now))

        return alerts
