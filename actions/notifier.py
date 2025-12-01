from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional

from env import Config
from utils.logging import get_logger

try:
    import aiohttp  # для HTTP
except Exception:
    aiohttp = None  # будем логировать предупреждение, если попросят HTTP без aiohttp

logger = get_logger("notifier")


def _event_to_payload(event: Any, cfg: Config) -> Dict[str, Any]:
    """
    Преобразуем Event (dataclass) в словарь с учётом флагов include_*.
    """
    if is_dataclass(event):
        data = asdict(event)
    elif isinstance(event, dict):
        data = dict(event)
    else:
        # на крайний случай — сериализация через строку
        data = {"raw": str(event)}

    # Обрезаем payload по флагам
    if not cfg.notify.include_levels and "levels" in data:
        data.pop("levels", None)
    if not cfg.notify.include_spectrum and "octaves" in data:
        data.pop("octaves", None)

    # Немного порядка в ключах
    ordered = {
        "type": data.get("type"),
        "src": data.get("src"),
        "ts_first": data.get("ts_first"),
        "ts_last": data.get("ts_last"),
        "thresholds": data.get("thresholds"),
        "exceeded": data.get("exceeded"),
        "levels": data.get("levels"),
        "octaves": data.get("octaves"),
        "samples": data.get("samples"),
        "window_sec": data.get("window_sec"),
    }
    # Уберём None-ключи
    return {k: v for k, v in ordered.items() if v is not None}


class Notifier:
    """
    Канал оповещений. Конфигурируется из .env.
    Поддерживает file / http / udp / tcp (udp/tcp — заготовки).
    Использование: notifier = Notifier.from_config(cfg); await notifier.send(event)
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._session: Optional[aiohttp.ClientSession] = None

    @classmethod
    def from_config(cls, cfg: Config) -> "Notifier":
        return cls(cfg)

    # ------------- публичный API -------------

    async def send(self, event: Any) -> None:
        payload = _event_to_payload(event, self.cfg)

        # FILE
        if self.cfg.notify.file.enabled:
            try:
                await self._write_file_line(payload, self.cfg.notify.file.path)
            except Exception as e:
                logger.error("File notifier error: %s", e)

        # HTTP
        if self.cfg.notify.http.enabled:
            if aiohttp is None:
                logger.warning("HTTP notifier requested but aiohttp not installed.")
            else:
                try:
                    await self._send_http(payload)
                except Exception as e:
                    logger.error("HTTP notifier error: %s", e)

        # UDP/TCP — по необходимости можно расширить
        # if self.cfg.notify.udp.enabled: ...
        # if self.cfg.notify.tcp.enabled: ...

    async def close(self):
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    # ------------- реализации каналов -------------

    async def _write_file_line(self, payload: Dict[str, Any], path: str) -> None:
        """
        Запись в файл по одной строке JSON (JSONL). Потокобезопасно между корутинами.
        """
        # json dumps без ascii-эскейпа, чтобы кириллица была читаемой
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        # файловые операции блокирующие — унесём в threadpool
        def _write():
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

        await asyncio.to_thread(_write)
        logger.debug("file notifier -> %s | %s", path, payload.get("type"))

    async def _send_http(self, payload: Dict[str, Any]) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))

        url = self.cfg.notify.http.url
        method = self.cfg.notify.http.method or "POST"
        headers = {"Content-Type": "application/json"}
        token = self.cfg.notify.http.token
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with self._session.request(method, url, headers=headers, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            logger.debug("http notifier -> %s %s | %s", method, url, payload.get("type"))
