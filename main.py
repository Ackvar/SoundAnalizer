# sound_analyzer/main.py
from __future__ import annotations

import asyncio
import os
import signal
import sys
import contextlib
from typing import List

# --- Внутренние импорты с ожидаемым контрактом ---
try:
    from env import load_config
    from utils.logging import setup_logging, get_logger
    from db_client import DBClient
    from actions.notifier import Notifier
    from workers.device_worker import DeviceWorker
    from workers.scheduler import Scheduler
except Exception as e:
    print(
        f"[sound_analyzer] Missing modules for main.py startup: {e}\n"
        "Please implement required modules (env, utils.logging, db_client, actions.notifier, workers.*) "
        "before running.",
        file=sys.stderr,
    )
    raise


# -------------------- helpers --------------------

def _env_path_from_cli() -> str | None:
    """
    Путь к .env через:
      - аргумент командной строки: python main.py /path/to/.env
      - переменную окружения: ENV_PATH=/path/to/.env
    """
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()
    return os.environ.get("ENV_PATH")


def _summarize_notify(cfg) -> str:
    parts = []
    if getattr(cfg.notify.http, "enabled", False):
        parts.append(f"http:{cfg.notify.http.method}@{cfg.notify.http.url}")
    if getattr(cfg.notify.udp, "enabled", False):
        parts.append(f"udp:{cfg.notify.udp.host}:{cfg.notify.udp.port}")
    if getattr(cfg.notify.tcp, "enabled", False):
        parts.append(f"tcp:{cfg.notify.tcp.host}:{cfg.notify.tcp.port}")
    if getattr(cfg.notify.file, "enabled", False):
        parts.append(f"file:{cfg.notify.file.path}")
    return ", ".join(parts) if parts else "none"

def _summarize_thresholds(cfg) -> str:
    def pick(v): return "—" if v is None else str(v)
    um = f"UMIK(spl={pick(cfg.umik_thr_spl)}, leq1s={pick(cfg.umik_thr_leq_1s)}, leq60s={pick(cfg.umik_thr_leq_60s)}, lmax={pick(cfg.umik_thr_lmax)}, bands={len(cfg.umik_thr_bands)})" if cfg.umik_enabled else "UMIK(disabled)"
    an = f"ANALOG(spl={pick(cfg.analog_thr_spl)}, leq={pick(cfg.analog_thr_leq)}, lmax={pick(cfg.analog_thr_lmax)}, weight={cfg.analog_weight_type or '—'})" if cfg.analog_enabled else "ANALOG(disabled)"
    return f"{um}; {an}"


# -------------------- bootstrap --------------------

async def _startup(config) -> tuple[Scheduler, List[DeviceWorker]]:
    """
    Инициализация всех подсистем:
      - БД клиент
      - нотификатор
      - воркеры по источникам
      - планировщик (scheduler)
    """
    logger = get_logger("main")
    logger.info("Starting sound_analyzer...")

    # Клиент БД (передаём воркерам)
    db = DBClient(config.db_path)

    # Нотификатор(ы)
    notifier = Notifier.from_config(config)

    workers: List[DeviceWorker] = []
    if config.umik_enabled:
        workers.append(DeviceWorker(name="UMIK",   kind="UMIK",   config=config, db=db, notifier=notifier))
    if config.analog_enabled:
        workers.append(DeviceWorker(name="ANALOG", kind="ANALOG", config=config, db=db, notifier=notifier))

    if not workers:
        logger.warning("No workers enabled. Check your .env configuration.")
    else:
        logger.info("Workers enabled: %s", ", ".join(w.name for w in workers))

    scheduler = Scheduler(interval_ms=config.poll_interval_ms, workers=workers)
    return scheduler, workers


async def _graceful_run(scheduler: Scheduler, workers: List[DeviceWorker]) -> None:
    """
    Запуск планировщика и мягкая остановка по сигналам.
    """
    logger = get_logger("main")
    stop_event = asyncio.Event()

    def _signal_handler(signame: str):
        logger.info("Received signal %s -> stopping...", signame)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig.name)
        except NotImplementedError:
            # Windows/некоторые окружения —signals не поддерживаются, игнорируем.
            pass

    # Старт планировщика
    run_task = asyncio.create_task(scheduler.run(), name="scheduler")
    logger.info("Scheduler started: interval=%d ms, workers=%d",
                getattr(scheduler, "interval_ms", -1), len(workers))

    # Ожидаем сигнал остановки
    await stop_event.wait()

    # Останавливаем планировщик
    await scheduler.stop()
    logger.info("Scheduler stop requested")

    # Даём воркерам завершить свои ресурсы
    for w in workers:
        try:
            if hasattr(w, "shutdown") and callable(getattr(w, "shutdown")):
                await w.shutdown()
        except Exception as e:
            logger.exception("Worker %s shutdown error: %s", w.name, e)

    # Дожидаемся завершения задачи планировщика
    try:
        await asyncio.wait_for(run_task, timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Scheduler didn't stop in time, canceling task...")
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task

    logger.info("sound_analyzer stopped.")


async def _async_main() -> int:
    # 1) Загружаем конфиг
    env_path = _env_path_from_cli()
    config = load_config(env_path)

    # 2) Настраиваем логирование (уровень/формат берём из конфига)
    setup_logging(config)
    logger = get_logger("main")

    logger.debug("ENV path: %s", env_path or "<default>")
    logger.info("Using DB: %s", config.db_path)
    logger.info("Poll interval: %d ms | Window: %ds", config.poll_interval_ms, config.window_seconds)
    logger.info("Notify channels: %s", _summarize_notify(config))
    logger.debug("Thresholds: %s", _summarize_thresholds(config))

    # 3) Старт подсистем
    scheduler, workers = await _startup(config)

    # 4) Основной цикл с graceful shutdown
    await _graceful_run(scheduler, workers)
    return 0


def main() -> int:
    try:
        return asyncio.run(_async_main())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        # На самый крайний случай — логируем в stderr
        print(f"[sound_analyzer] Fatal error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
