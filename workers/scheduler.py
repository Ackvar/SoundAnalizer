# sound_analyzer/workers/scheduler.py
from __future__ import annotations

import asyncio
from typing import List

from utils.logging import get_logger


class Scheduler:
    """
    Периодически опрашивает всех воркеров.
    Интервал задаётся в миллисекундах. Для каждого воркера берётся его
    собственное окно (worker.cfg.window_seconds).
    """

    def __init__(self, interval_ms: int, workers: List):
        self.interval_ms = max(1, int(interval_ms))
        self.workers = workers
        self._stop_event = asyncio.Event()
        self._log = get_logger("scheduler")

    async def run(self) -> None:
        self._log.info(
            "Scheduler started: interval=%d ms, workers=%d",
            self.interval_ms,
            len(self.workers),
        )
        try:
            # основной цикл
            while not self._stop_event.is_set():
                # один тик: запускаем poll у всех воркеров параллельно
                tasks = []
                for w in self.workers:
                    try:
                        window = int(getattr(w.cfg, "window_seconds", 5))
                    except Exception:
                        window = 5
                    tasks.append(asyncio.create_task(w.poll(window)))

                if tasks:
                    # ждём завершения тика, не падаем из-за одного воркера
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for w, res in zip(self.workers, results):
                        if isinstance(res, Exception):
                            self._log.error("Worker %s tick error: %s", w.name, res)

                # пауза до следующего тика (с возможностью прервать stop'ом)
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.interval_ms / 1000.0,
                    )
                except asyncio.TimeoutError:
                    # нормальный переход к следующему тиканью
                    pass
        finally:
            self._log.info("Scheduler stopped")

    async def stop(self) -> None:
        """Остановить цикл run()."""
        self._stop_event.set()
