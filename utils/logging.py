# sound_analyzer/utils/logging.py
from __future__ import annotations

import logging
import sys
from typing import Optional

# Глобальный реестр логгеров
_LOGGER_CACHE: dict[str, logging.Logger] = {}


def setup_logging(config) -> None:
    """
    Настройка логирования по конфигу.
    config.log_level должен быть строкой: DEBUG | INFO | WARNING | ERROR.
    """
    level = getattr(logging, config.log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(fmt)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)

    # Сброс кэша
    _LOGGER_CACHE.clear()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Вернуть логгер по имени. Логгеры кэшируются.
    """
    if not name:
        name = "sound_analyzer"
    if name in _LOGGER_CACHE:
        return _LOGGER_CACHE[name]

    logger = logging.getLogger(name)
    _LOGGER_CACHE[name] = logger
    return logger
