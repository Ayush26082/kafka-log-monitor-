import json

import pytest

from src import config
from src.detector import Detector, parse_event


def ev(ts, service="api-gateway", level="INFO", **extra):
    return {"ts": ts, "service": service, "level": level, "message": "m", **extra}


def test_error_burst_triggers_once_then_cooldown():
    d = Detector()
    alerts = []
    for i in range(config.ERROR_THRESHOLD + 3):
        alerts += d.process(ev(1000 + i, level="ERROR"))
    assert [a.type for a in alerts] == ["HIGH_ERROR_RATE"]


def test_errors_outside_window_do_not_trigger():
    d = Detector()
    alerts = []
    for i in range(config.ERROR_THRESHOLD + 2):
        alerts += d.process(ev(1000 + i * (config.ERROR_WINDOW_SEC + 1), level="ERROR"))
    assert alerts == []


def test_errors_are_tracked_per_service():
    d = Detector()
    alerts = []
    for i in range(config.ERROR_THRESHOLD):
        alerts += d.process(ev(1000 + i, service=f"svc-{i}", level="ERROR"))
    assert alerts == []


def test_critical_fires_immediately():
    alerts = Detector().process(ev(1000, level="CRITICAL"))
    assert alerts[0].type == "CRITICAL_EVENT"


def test_brute_force_detected_by_ip():
    d = Detector()
    alerts = []
    for i in range(config.BRUTE_FORCE_THRESHOLD):
        alerts += d.process(ev(1000 + i, level="WARN", event_type="login_failed", ip="1.2.3.4"))
    assert [a.type for a in alerts] == ["BRUTE_FORCE_SUSPECT"]
    assert alerts[0].key == "1.2.3.4"


def test_cooldown_expires():
    d = Detector()
    for i in range(config.ERROR_THRESHOLD):
        d.process(ev(1000 + i, level="ERROR"))
    later = 1000 + config.ALERT_COOLDOWN_SEC + 5
    alerts = []
    for i in range(config.ERROR_THRESHOLD):
        alerts += d.process(ev(later + i, level="ERROR"))
    assert len(alerts) == 1


@pytest.mark.parametrize("raw", [b"{not json", b"[]", b'{"ts":1}',
                                 json.dumps(ev(1, level="LOUD")).encode(),
                                 json.dumps(ev("abc")).encode()])
def test_parse_event_rejects_bad_input(raw):
    with pytest.raises(ValueError):
        parse_event(raw)


def test_parse_event_accepts_good_input():
    assert parse_event(json.dumps(ev(5)).encode())["ts"] == 5.0
