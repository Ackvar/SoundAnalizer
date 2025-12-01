# sound_analyzer/env.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional


# --- Константы схемы БД (октавные колонки для UMIK) ---
ALLOWED_BANDS = [
    "31.5_Hz", "63.0_Hz", "125.0_Hz", "250.0_Hz", "500.0_Hz",
    "1000.0_Hz", "2000.0_Hz", "4000.0_Hz", "8000.0_Hz",
]


# --- Утилиты парсинга ---
def _parse_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    v = val.strip().lower()
    return v in ("1", "true", "yes", "on", "y")


def _parse_int(val: Optional[str], default: int) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        raise ValueError(f"Invalid int value: {val!r}")


def _parse_float(val: Optional[str], default: Optional[float]) -> Optional[float]:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except Exception:
        raise ValueError(f"Invalid float value: {val!r}")


def _normalize_band_key(key: str) -> str:
    """
    Приводим ключи полос к виду из БД:
      - 'o31', '31', '31.5', '31.5_Hz' -> '31.5_Hz'
      - '1000', 'o1000', '1000.0', '1000.0_Hz' -> '1000.0_Hz'
    """
    k = key.strip()
    if k.endswith("_Hz"):
        return k
    if k.startswith(("o", "O")):
        k = k[1:]
    try:
        f = float(k)
    except Exception:
        return k
    if abs(f - round(f)) < 1e-9:
        return f"{int(round(f))}.0_Hz"
    s = ("%.1f" % f).rstrip("0").rstrip(".")
    if "." in s:
        left, right = s.split(".", 1)
        if len(right) == 1:
            s = f"{left}.{right}"
    return f"{s}_Hz"


def _parse_bands_json(val: Optional[str]) -> Dict[str, float]:
    """
    Ожидаем JSON-строку вида {"31.5_Hz": 40, "1000.0_Hz": 55} или с ключами 'o31', '1000', и т.п.
    Нормализуем ключи к ALLOWED_BANDS и отфильтровываем лишние.
    """
    if not val:
        return {}
    try:
        obj = json.loads(val)
        if not isinstance(obj, dict):
            raise ValueError("bands JSON must be an object (dict).")
    except Exception as e:
        raise ValueError(f"Invalid JSON for bands: {e}")

    result: Dict[str, float] = {}
    for k, v in obj.items():
        nk = _normalize_band_key(str(k))
        if nk not in ALLOWED_BANDS:
            continue
        try:
            result[nk] = float(v)
        except Exception:
            raise ValueError(f"Band threshold for {nk!r} must be numeric, got {v!r}")
    return result


def _read_env_file(path: Optional[str]) -> Dict[str, str]:
    """
    Простой парсер .env (без зависимостей). Возвращает словарь ключ->значение.
    Переменные окружения ОС имеют приоритет над .env.
    """
    if not path:
        return {}
    if not os.path.isfile(path):
        raise FileNotFoundError(f".env file not found: {path}")

    data: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            data[k] = v
    return data


def _get(env: Dict[str, str], key: str, default: Optional[str] = None) -> Optional[str]:
    """Достаём переменную: сначала из ОС, затем из .env словаря, иначе default."""
    if key in os.environ:
        return os.environ[key]
    return env.get(key, default)


# --- Конфиги/датаклассы ---
@dataclass
class HTTPConfig:
    enabled: bool
    url: str
    method: str
    token: Optional[str]


@dataclass
class UDPConfig:
    enabled: bool
    host: str
    port: int


@dataclass
class TCPConfig:
    enabled: bool
    host: str
    port: int


@dataclass
class FileConfig:
    enabled: bool
    path: str


@dataclass
class NotifyConfig:
    http: HTTPConfig
    udp: UDPConfig
    tcp: TCPConfig
    file: FileConfig
    include_spectrum: bool
    include_levels: bool
    send_recovery: bool


@dataclass
class Config:
    # база/планировщик
    db_path: str
    poll_interval_ms: int
    window_seconds: int
    limit_last_u: int
    limit_last_a: int

    # источники
    umik_enabled: bool
    analog_enabled: bool
    analog_weight_type: Optional[str]

    # пороги UMIK
    umik_thr_spl: Optional[float]
    umik_thr_leq_1s: Optional[float]
    umik_thr_leq_60s: Optional[float]
    umik_thr_lmax: Optional[float]

    # пороги ANALOG
    analog_thr_spl: Optional[float]
    analog_thr_leq: Optional[float]
    analog_thr_lmax: Optional[float]

    # устойчивость / антидребезг
    trigger_hold_ms: int
    recover_hold_ms: int
    cooldown_ms: int
    retrigger_gap_ms: int
    consecutive_required: int

    # логирование
    log_level: str

    # оповещения
    notify: NotifyConfig

    # ⚠️ поля с дефолтами должны идти после всех без дефолтов
    umik_thr_bands: Dict[str, float] = field(default_factory=dict)


def load_config(env_path: Optional[str] = None) -> Config:
    env_map = _read_env_file(env_path)

    # --- Общие настройки ---
    db_path = _get(env_map, "DB_PATH", "/home/geber/Project_Umik/sound_log.db")
    poll_interval_ms = _parse_int(_get(env_map, "POLL_INTERVAL_MS", "500"), 500)
    window_seconds = _parse_int(_get(env_map, "WINDOW_SECONDS", "5"), 5)
    limit_last_u = _parse_int(_get(env_map, "LIMIT_LAST_U", "200"), 200)
    limit_last_a = _parse_int(_get(env_map, "LIMIT_LAST_A", "200"), 200)

    # --- Источники ---
    umik_enabled = _parse_bool(_get(env_map, "UMIK_ENABLED", "true"), True)
    analog_enabled = _parse_bool(_get(env_map, "ANALOG_ENABLED", "true"), True)
    analog_weight_type = _get(env_map, "ANALOG_WEIGHT_TYPE")

    # --- Пороги UMIK ---
    umik_thr_spl = _parse_float(_get(env_map, "UMIK_THR_SPL", ""), None)
    umik_thr_leq_1s = _parse_float(_get(env_map, "UMIK_THR_LEQ_1S", ""), None)
    umik_thr_leq_60s = _parse_float(_get(env_map, "UMIK_THR_LEQ_60S", ""), None)
    umik_thr_lmax = _parse_float(_get(env_map, "UMIK_THR_LMAX", ""), None)
    umik_thr_bands = _parse_bands_json(_get(env_map, "UMIK_THR_BANDS", ""))

    # --- Пороги ANALOG ---
    analog_thr_spl = _parse_float(_get(env_map, "ANALOG_THR_SPL", ""), None)
    analog_thr_leq = _parse_float(_get(env_map, "ANALOG_THR_LEQ", ""), None)
    analog_thr_lmax = _parse_float(_get(env_map, "ANALOG_THR_LMAX", ""), None)

    # --- Устойчивость ---
    trigger_hold_ms = _parse_int(_get(env_map, "TRIGGER_HOLD_MS", "1500"), 1500)
    recover_hold_ms = _parse_int(_get(env_map, "RECOVER_HOLD_MS", "2000"), 2000)
    cooldown_ms = _parse_int(_get(env_map, "COOLDOWN_MS", "3000"), 3000)
    retrigger_gap_ms = _parse_int(_get(env_map, "RETRIGGER_GAP_MS", "5000"), 5000)
    consecutive_required = _parse_int(_get(env_map, "CONSECUTIVE_REQUIRED", "3"), 3)

    # --- Логирование ---
    log_level = (_get(env_map, "LOG_LEVEL", "INFO") or "INFO").upper()

    # --- Каналы оповещений ---
    http = HTTPConfig(
        enabled=_parse_bool(_get(env_map, "ALERT_HTTP_ENABLED", "false"), False),
        url=_get(env_map, "ALERT_HTTP_URL", "http://127.0.0.1:9000/alert") or "",
        method=(_get(env_map, "ALERT_HTTP_METHOD", "POST") or "POST").upper(),
        token=_get(env_map, "ALERT_HTTP_TOKEN"),
    )
    udp = UDPConfig(
        enabled=_parse_bool(_get(env_map, "ALERT_UDP_ENABLED", "false"), False),
        host=_get(env_map, "ALERT_UDP_HOST", "127.0.0.1") or "127.0.0.1",
        port=_parse_int(_get(env_map, "ALERT_UDP_PORT", "40123"), 40123),
    )
    tcp = TCPConfig(
        enabled=_parse_bool(_get(env_map, "ALERT_TCP_ENABLED", "false"), False),
        host=_get(env_map, "ALERT_TCP_HOST", "127.0.0.1") or "127.0.0.1",
        port=_parse_int(_get(env_map, "ALERT_TCP_PORT", "40124"), 40124),
    )
    filecfg = FileConfig(
        enabled=_parse_bool(_get(env_map, "ALERT_FILE_ENABLED", "true"), True),
        path=_get(env_map, "ALERT_FILE_PATH", "/var/log/sound_alerts.jsonl") or "/var/log/sound_alerts.jsonl",
    )

    include_spectrum = _parse_bool(_get(env_map, "INCLUDE_SPECTRUM", "true"), True)
    include_levels = _parse_bool(_get(env_map, "INCLUDE_LEVELS", "true"), True)
    send_recovery = _parse_bool(_get(env_map, "SEND_RECOVERY", "true"), True)

    notify = NotifyConfig(
        http=http, udp=udp, tcp=tcp, file=filecfg,
        include_spectrum=include_spectrum,
        include_levels=include_levels,
        send_recovery=send_recovery,
    )

    # --- Базовые валидации ---
    if not db_path:
        raise ValueError("DB_PATH is required")
    if http.enabled and not http.url:
        raise ValueError("ALERT_HTTP_ENABLED=true, но ALERT_HTTP_URL пуст")

    return Config(
        db_path=db_path,
        poll_interval_ms=poll_interval_ms,
        window_seconds=window_seconds,
        limit_last_u=limit_last_u,
        limit_last_a=limit_last_a,
        umik_enabled=umik_enabled,
        analog_enabled=analog_enabled,
        analog_weight_type=analog_weight_type,
        umik_thr_spl=umik_thr_spl,
        umik_thr_leq_1s=umik_thr_leq_1s,
        umik_thr_leq_60s=umik_thr_leq_60s,
        umik_thr_lmax=umik_thr_lmax,
        analog_thr_spl=analog_thr_spl,
        analog_thr_leq=analog_thr_leq,
        analog_thr_lmax=analog_thr_lmax,
        trigger_hold_ms=trigger_hold_ms,
        recover_hold_ms=recover_hold_ms,
        cooldown_ms=cooldown_ms,
        retrigger_gap_ms=retrigger_gap_ms,
        consecutive_required=consecutive_required,
        log_level=log_level,
        notify=notify,
        umik_thr_bands=umik_thr_bands,   # поле с дефолтом — в самом конце dataclass
    )
