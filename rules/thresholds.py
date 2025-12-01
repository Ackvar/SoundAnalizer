# sound_analyzer/rules/thresholds.py
from __future__ import annotations

from typing import Dict, Tuple

from models import Fact
from env import Config
from utils.logging import get_logger


logger = get_logger("thresholds")


def check_levels_and_bands(fact: Fact, config: Config) -> Fact:
    """
    Проверяет значения Fact против порогов из Config.
    Возвращает обновлённый Fact с заполненными exceeded_levels и exceeded_bands.
    """

    exceeded_levels: Dict[str, bool] = {}
    exceeded_bands: Dict[str, bool] = {}

    if fact.src == "UMIK":
        if config.umik_thr_spl is not None:
            exceeded_levels["spl"] = (
                fact.spl_max is not None and fact.spl_max > config.umik_thr_spl
            )
        if config.umik_thr_leq_1s is not None:
            exceeded_levels["leq_1s"] = (
                fact.leq_1s_avg is not None and fact.leq_1s_avg > config.umik_thr_leq_1s
            )
        if config.umik_thr_leq_60s is not None:
            exceeded_levels["leq_60s"] = (
                fact.leq_60s_last is not None and fact.leq_60s_last > config.umik_thr_leq_60s
            )
        if config.umik_thr_lmax is not None:
            exceeded_levels["lmax"] = (
                fact.lmax_max is not None and fact.lmax_max > config.umik_thr_lmax
            )

        # Проверка полос (октав)
        for band, thr in config.umik_thr_bands.items():
            val = fact.bands_max.get(band)
            if val is not None:
                exceeded_bands[band] = val > thr

    elif fact.src == "ANALOG":
        if config.analog_thr_spl is not None:
            exceeded_levels["spl"] = (
                fact.spl_max is not None and fact.spl_max > config.analog_thr_spl
            )
        if config.analog_thr_leq is not None:
            exceeded_levels["leq"] = (
                fact.leq_avg is not None and fact.leq_avg > config.analog_thr_leq
            )
        if config.analog_thr_lmax is not None:
            exceeded_levels["lmax"] = (
                fact.lmax_max is not None and fact.lmax_max > config.analog_thr_lmax
            )

    else:
        logger.warning("Unknown fact.src: %s", fact.src)

    fact.exceeded_levels = exceeded_levels
    fact.exceeded_bands = exceeded_bands
    return fact


def is_exceeded(fact: Fact) -> Tuple[bool, Dict[str, bool], Dict[str, bool]]:
    """
    Удобная обёртка: возвращает (флаг, exceeded_levels, exceeded_bands).
    Флаг True, если превышено хотя бы одно условие.
    """
    flag = any(fact.exceeded_levels.values()) or any(fact.exceeded_bands.values())
    return flag, fact.exceeded_levels, fact.exceeded_bands
