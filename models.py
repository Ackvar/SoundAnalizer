# sound_analyzer/models.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Literal, List


# -----------------------------
# Измерения (сырые данные из БД)
# -----------------------------

@dataclass
class MeasurementU:
    """Запись из таблицы measurements (UMIK-1)."""

    timestamp: float
    spl: Optional[float] = None
    leq_1s: Optional[float] = None
    leq_60s: Optional[float] = None
    lmax: Optional[float] = None
    bands: Dict[str, Optional[float]] = field(default_factory=dict)


@dataclass
class MeasurementA:
    """Запись из таблицы weighted_measurements (аналоговый микрофон)."""

    timestamp: float
    weight_type: Optional[str] = None
    spl: Optional[float] = None
    leq: Optional[float] = None
    lmax: Optional[float] = None


# -----------------------------
# Факт анализа окна
# -----------------------------

@dataclass
class Fact:
    """
    Агрегированное окно данных (после db_client + правил).
    Используется как вход в state_machine.
    """

    ts_from: float
    ts_to: float
    src: Literal["UMIK", "ANALOG"]

    # агрегированные значения
    spl_max: Optional[float] = None
    lmax_max: Optional[float] = None

    # только UMIK
    leq_1s_avg: Optional[float] = None
    leq_60s_last: Optional[float] = None
    bands_max: Dict[str, Optional[float]] = field(default_factory=dict)

    # только ANALOG
    leq_avg: Optional[float] = None

    # что превысило
    exceeded_levels: Dict[str, bool] = field(default_factory=dict)
    exceeded_bands: Dict[str, bool] = field(default_factory=dict)


# -----------------------------
# Событие (ALERT/RECOVERY)
# -----------------------------

@dataclass
class Event:
    """
    Событие, которое будет отправлено нотификатору.
    """

    type: Literal["ALERT", "RECOVERY"]
    src: Literal["UMIK", "ANALOG"]

    ts_first: float
    ts_last: float

    # значения
    levels: Dict[str, Optional[float]] = field(default_factory=dict)
    octaves: Dict[str, Optional[float]] = field(default_factory=dict)

    # детали
    exceeded: Dict[str, Dict[str, bool]] = field(default_factory=dict)
    thresholds: Dict[str, Dict[str, float]] = field(default_factory=dict)

    samples: int = 0
    window_sec: float = 0.0
