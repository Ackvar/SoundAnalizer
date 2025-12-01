# sound_analyzer/actions/formatter.py
from __future__ import annotations

import json
from typing import Any, Dict

from models import Event
from env import Config
from utils.logging import get_logger


logger = get_logger("formatter")


def format_event(event: Event, config: Config) -> Dict[str, Any]:
    """
    Сформировать payload для отправки наружу.
    Возвращает dict, готовый к сериализации в JSON.
    """
    payload: Dict[str, Any] = {
        "type": event.type,
        "src": event.src,
        "ts_first": event.ts_first,
        "ts_last": event.ts_last,
        "samples": event.samples,
        "window_sec": event.window_sec,
    }

    if config.notify.include_levels:
        payload["levels"] = event.levels

    if config.notify.include_spectrum and event.src == "UMIK":
        payload["octaves"] = event.octaves

    payload["exceeded"] = event.exceeded
    payload["thresholds"] = event.thresholds

    return payload


def to_json(payload: Dict[str, Any]) -> str:
    """Преобразовать dict в JSON-строку (читаемый вывод)."""
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception as e:
        logger.error("JSON encode error: %s", e)
        return "{}"
