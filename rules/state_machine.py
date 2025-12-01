# sound_analyzer/rules/state_machine.py
from __future__ import annotations

import time
from typing import Literal, Optional

from models import Fact, Event
from env import Config
from utils.logging import get_logger
from rules.thresholds import is_exceeded

logger = get_logger("state_machine")


class StateMachine:
    """
    FSM для одного источника (UMIK или ANALOG).
    Состояния: NORMAL -> ALERT -> COOLDOWN -> NORMAL
    """

    def __init__(self, src: Literal["UMIK", "ANALOG"], config: Config):
        self.src = src
        self.cfg = config

        self.state: str = "NORMAL"
        self.last_transition: float = time.time()
        self.active_since: Optional[float] = None      # с какого момента есть превышение
        self.recovery_since: Optional[float] = None    # с какого момента нет превышения

        # новые вспомогательные поля (не меняют внешний контракт)
        self._consec_hit: int = 0                      # подряд "есть превышение"
        self._consec_ok: int = 0                       # подряд "нет превышения"
        self._last_alert_at_ms: Optional[float] = None # когда последний ALERT отправлялся (для retrigger gap)

    def _now_ms(self) -> float:
        return time.time() * 1000.0

    def _win_seconds(self, fact: Fact) -> float:
        try:
            return max(0.0, float(fact.ts_to - fact.ts_from))
        except Exception:
            return float(self.cfg.window_seconds)

    def step(self, fact: Fact, thresholds: dict) -> Optional[Event]:
        """
        Обрабатывает новое окно Fact.
        Может вернуть Event (ALERT или RECOVERY), либо None.
        """
        now = time.time()
        now_ms = self._now_ms()
        flag, exceeded_levels, exceeded_bands = is_exceeded(fact)

        # обновим счётчики последовательностей
        if flag:
            self._consec_hit += 1
            self._consec_ok = 0
            if self.active_since is None:
                self.active_since = now
        else:
            self._consec_ok += 1
            self._consec_hit = 0
            if self.recovery_since is None:
                self.recovery_since = now
            # сбрасываем "активность", если снова нет превышения
            self.active_since = None

        # удобные локальные константы
        hold_trig = self.cfg.trigger_hold_ms
        hold_rec = self.cfg.recover_hold_ms
        cooldown_ms = self.cfg.cooldown_ms
        need_seq = self.cfg.consecutive_required
        retrigger_gap = self.cfg.retrigger_gap_ms

        if self.state == "NORMAL":
            # Требуем: есть превышение, держится >= hold_trig и >= need_seq подряд,
            # и соблюдён retrigger gap.
            hit_long_enough = self.active_since is not None and (now - self.active_since) * 1000.0 >= hold_trig
            enough_seq = self._consec_hit >= need_seq
            gap_ok = (self._last_alert_at_ms is None) or ((now_ms - self._last_alert_at_ms) >= retrigger_gap)

            if flag and hit_long_enough and enough_seq and gap_ok:
                self.state = "ALERT"
                self.last_transition = now
                self._last_alert_at_ms = now_ms
                self.recovery_since = None
                logger.info("[%s] ALERT triggered", self.src)

                ev = self._make_event("ALERT", fact, thresholds, exceeded_levels, exceeded_bands)
                # сразу уходим в COOLDOWN, чтобы не «дребезжало»
                self.state = "COOLDOWN"
                self.last_transition = now
                return ev

            return None

        elif self.state == "ALERT":
            # В этой реализации мы почти не находимся в ALERT (сразу отправили и ушли в COOLDOWN),
            # так что сюда обычно не попадаем. Оставим на всякий случай.
            if not flag and self.recovery_since is not None and (now - self.recovery_since) * 1000.0 >= hold_rec:
                self.state = "COOLDOWN"
                self.last_transition = now
                logger.info("[%s] RECOVERY -> cooldown", self.src)
                if self.cfg.notify.send_recovery:
                    return self._make_event("RECOVERY", fact, thresholds, exceeded_levels, exceeded_bands)
            return None

        elif self.state == "COOLDOWN":
            # Ждём истечения кулдауна и возвращаемся в NORMAL.
            if (now - self.last_transition) * 1000.0 >= cooldown_ms:
                self.state = "NORMAL"
                self.active_since = None
                self.recovery_since = None
                self._consec_hit = 0
                self._consec_ok = 0
                self.last_transition = now
                logger.info("[%s] Back to NORMAL", self.src)
            return None

        return None

    def _make_event(
        self,
        etype: Literal["ALERT", "RECOVERY"],
        fact: Fact,
        thresholds: dict,
        exceeded_levels: dict,
        exceeded_bands: dict,
    ) -> Event:
        return Event(
            type=etype,
            src=self.src,
            ts_first=fact.ts_from,
            ts_last=fact.ts_to,
            levels={
                "spl_max": fact.spl_max,
                "lmax_max": fact.lmax_max,
                "leq_1s_avg": getattr(fact, "leq_1s_avg", None),
                "leq_60s_last": getattr(fact, "leq_60s_last", None),
                "leq_avg": getattr(fact, "leq_avg", None),
            },
            octaves=fact.bands_max if self.src == "UMIK" else {},
            exceeded={
                "levels": exceeded_levels,
                "bands": exceeded_bands,
            },
            thresholds=thresholds,
            samples=0,  # воркер заполнит
            window_sec=self._win_seconds(fact),
        )
