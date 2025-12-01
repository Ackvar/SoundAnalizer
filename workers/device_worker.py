from __future__ import annotations

import math
from typing import Literal, Optional, Dict, Any

from db_client import DBClient
from env import Config
from models import Fact
from utils.logging import get_logger
from actions.notifier import Notifier
from rules.thresholds import check_levels_and_bands, is_exceeded
from rules.state_machine import StateMachine

logger = get_logger("device_worker")


def _safe_max(values):
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def _safe_avg(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _fmt(v):
    return None if v is None else (round(v, 2) if isinstance(v, (int, float)) and not math.isnan(v) else v)


def _short(values: dict | None, bands: dict | None) -> str:
    vals = values or {}
    b = bands or {}
    hot_bands = [k for k, v in b.items() if v]
    return (
        f"spl={_fmt(vals.get('spl_max'))}, lmax={_fmt(vals.get('lmax_max'))}, "
        f"leq1s={_fmt(vals.get('leq_1s_avg'))}, leq60={_fmt(vals.get('leq_60s_last'))}, bands={hot_bands}"
    )


class DeviceWorker:
    """
    Воркер одного источника (UMIK или ANALOG).
    Контракт, ожидаемый main/scheduler:
      - init(name, kind, config, db, notifier)
      - async poll(window_seconds)
      - async shutdown()
    """

    def __init__(
        self,
        name: str,
        kind: Literal["UMIK", "ANALOG"],
        config: Config,
        db: DBClient,
        notifier: Notifier,
    ):
        self.name = name
        self.kind = kind
        self.cfg = config
        self.db = db
        self.notifier = notifier
        self._stopped = False

        # FSM на источник
        self.fsm = StateMachine(kind, config)

        # чтобы не жевать одно и то же окно при частом poll
        self._last_anchor: Optional[float] = None

    async def shutdown(self) -> None:
        """Нужно main.py для graceful stop."""
        self._stopped = True

    # -------- основной цикл воркера (вызывается планировщиком) --------

    async def poll(self, window_seconds: float) -> None:
        if self._stopped:
            return

        # 1) Определим якорь времени по последней записи нужной таблицы
        try:
            anchor = self._latest_anchor()
        except Exception as e:
            logger.exception("[%s] anchor read failed: %s", self.name, e)
            return

        if anchor is None:
            logger.debug("[%s] no data yet", self.name)
            return

        # если якорь не изменился — ничего не делаем
        if self._last_anchor is not None and anchor <= self._last_anchor:
            logger.debug("[%s] anchor unchanged: %s", self.name, _fmt(anchor))
            return
        self._last_anchor = anchor

        ts_to = anchor
        ts_from = ts_to - float(window_seconds)
        if ts_from >= ts_to:
            # на всякий случай расширим окно на 1 секунду
            ts_from = ts_to - 1.0

        # 2) Выборка окна
        try:
            if self.kind == "UMIK":
                rows = self.db.fetch_umik_window(ts_from, ts_to, limit=self.cfg.limit_last_u)
            else:
                rows = self.db.fetch_analog_window(
                    ts_from,
                    ts_to,
                    limit=self.cfg.limit_last_a,
                    weight_type=(self.cfg.analog_weight_type or None),
                )
        except Exception as e:
            logger.exception("[%s] DB window fetch failed: %s", self.name, e)
            return

        if not rows:
            logger.debug("[%s] rows=0 (%.3f..%.3f)", self.name, ts_from, ts_to)
            return

        # 3) Агрегация окна -> Fact
        fact = self._make_fact(rows, ts_from, ts_to)

        # Диагностика окна (DEBUG)
        if self.kind == "UMIK":
            bands_dbg = {k: _fmt(v) for k, v in (fact.bands_max or {}).items()}
        else:
            bands_dbg = {}
        logger.debug(
            "[%s] rows=%d | spl_max=%s lmax_max=%s leq_1s_avg=%s leq_60s_last=%s leq_avg=%s | bands=%s",
            self.name,
            len(rows),
            _fmt(fact.spl_max),
            _fmt(fact.lmax_max),
            _fmt(getattr(fact, "leq_1s_avg", None)),
            _fmt(getattr(fact, "leq_60s_last", None)),
            _fmt(getattr(fact, "leq_avg", None)),
            bands_dbg,
        )

        # 4) Проверка порогов
        fact = check_levels_and_bands(fact, self.cfg)
        exceeded_flag, exceeded_levels, exceeded_bands = is_exceeded(fact)

        # Для наглядности логируем сам факт превышений
        if exceeded_flag:
            lvl_hot = {k: v for k, v in exceeded_levels.items() if v}
            bnd_hot = {k: v for k, v in exceeded_bands.items() if v}
            logger.debug("[%s] exceeded: levels=%s bands=%s", self.name, lvl_hot, bnd_hot)
        else:
            logger.debug("[%s] within thresholds", self.name)

        # 5) FSM -> Event?
        thresholds_view: Dict[str, Any] = {
            "levels": {
                "spl": self.cfg.umik_thr_spl if self.kind == "UMIK" else self.cfg.analog_thr_spl,
                "leq_1s": getattr(self.cfg, "umik_thr_leq_1s", None) if self.kind == "UMIK" else None,
                "leq_60s": getattr(self.cfg, "umik_thr_leq_60s", None) if self.kind == "UMIK" else None,
                "leq": self.cfg.analog_thr_leq if self.kind == "ANALOG" else None,
                "lmax": self.cfg.umik_thr_lmax if self.kind == "UMIK" else self.cfg.analog_thr_lmax,
            },
            "bands": self.cfg.umik_thr_bands if self.kind == "UMIK" else {},
        }

        event = self.fsm.step(fact, thresholds_view)
        if event:
            # 6) Отправка события
            try:
                await self.notifier.send(event)
                logger.info(
                    "[%s] %s sent: %s",
                    self.name,
                    event.type,
                    _short(values=event.levels, bands=event.exceeded.get("bands")),
                )
            except Exception as e:
                logger.exception("[%s] notifier failed for %s: %s", self.name, event.type, e)
        # иначе ничего не делаем

    # -------------------- helpers --------------------

    def _latest_anchor(self) -> Optional[float]:
        """Последний timestamp (в секундах) соответствующей таблицы."""
        table = "measurements" if self.kind == "UMIK" else "weighted_measurements"
        return self.db.latest_ts(table)

    def _make_fact(self, rows: list[dict], ts_from: float, ts_to: float) -> Fact:
        if self.kind == "UMIK":
            # уровни
            spl_max = _safe_max([r.get("spl") for r in rows])
            lmax_max = _safe_max([r.get("lmax") for r in rows])
            leq_1s_avg = _safe_avg([r.get("leq_1s") for r in rows])
            # Leq_60s — берём «последнее доступное» значение (rows отсортированы DESC по времени)
            leq_60s_last = None
            for r in rows:
                val = r.get("leq_60s")
                if val is not None:
                    leq_60s_last = val
                    break

            # октавы: берём максимум по каждой колонке
            bands = {}
            for col in (
                "31.5_Hz",
                "63.0_Hz",
                "125.0_Hz",
                "250.0_Hz",
                "500.0_Hz",
                "1000.0_Hz",
                "2000.0_Hz",
                "4000.0_Hz",
                "8000.0_Hz",
            ):
                bands[col] = _safe_max([r.get(col) for r in rows])

            return Fact(
                src="UMIK",
                ts_from=ts_from,
                ts_to=ts_to,
                spl_max=spl_max,
                lmax_max=lmax_max,
                leq_1s_avg=leq_1s_avg,
                leq_60s_last=leq_60s_last,
                bands_max=bands,
            )

        else:  # ANALOG
            spl_max = _safe_max([r.get("spl") for r in rows])
            lmax_max = _safe_max([r.get("lmax") for r in rows])
            leq_avg = _safe_avg([r.get("leq") for r in rows])
            return Fact(
                src="ANALOG",
                ts_from=ts_from,
                ts_to=ts_to,
                spl_max=spl_max,
                lmax_max=lmax_max,
                leq_avg=leq_avg,
                bands_max={},  # у аналогового нет октав
            )
