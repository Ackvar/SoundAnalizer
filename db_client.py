from __future__ import annotations

import sqlite3
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

ISO_FMT = "%Y-%m-%d %H:%M:%S"


def _to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime(ISO_FMT)


def _to_epoch(ts_val: Any) -> Optional[float]:
    """TEXT 'YYYY-MM-DD HH:MM:SS' -> epoch; REAL/INT -> float; иное -> None."""
    if ts_val is None:
        return None
    if isinstance(ts_val, (int, float)):
        return float(ts_val)
    try:
        return datetime.strptime(str(ts_val), ISO_FMT).timestamp()
    except Exception:
        return None


class DBClient:
    """
    Только-читатель SQLite. Публичный API сохранён.
    Поддерживает timestamp как TEXT (ISO) и как REAL/INTEGER (unix time).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def now(self) -> float:
        return time.time()

    # -------------------- низкоуровневые утилиты --------------------

    def _connect(self) -> sqlite3.Connection:
        """
        Соединение, оптимизированное под одновременную запись/чтение.
        WAL даёт видимость свежих коммитов без блокировок.
        """
        conn = sqlite3.connect(self.db_path, timeout=1.0)  # 1s busy timeout
        conn.row_factory = sqlite3.Row
        # Безопасные pragmas для читающего клиента
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            # На некоторых сборках изменение режима может быть запрещено — игнорируем
            pass
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=1000;")  # мс
        return conn

    def _query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Выполнить запрос и вернуть список словарей."""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    # ---------- полезные методы времени ----------

    def latest_ts(self, table: str) -> Optional[float]:
        """epoch последней записи (учитываем TEXT и REAL timestamp)."""
        rows = self._query(f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1")
        if not rows:
            return None
        return _to_epoch(rows[0]["timestamp"])

    # -------------------- выборки --------------------

    def fetch_umik_window(self, ts_from: float, ts_to: float, limit: int = 200) -> List[Dict[str, Any]]:
        f_iso, t_iso = _to_iso(ts_from), _to_iso(ts_to)
        f_num, t_num = float(ts_from), float(ts_to)
        sql = """
            SELECT *
            FROM measurements
            WHERE (
                (typeof(timestamp)='text' AND timestamp BETWEEN ? AND ?)
                OR
                (typeof(timestamp) IN ('real','integer') AND CAST(timestamp AS REAL) BETWEEN ? AND ?)
            )
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return self._query(sql, (f_iso, t_iso, f_num, t_num, limit))

    def fetch_analog_window(
        self,
        ts_from: float,
        ts_to: float,
        limit: int = 200,
        weight_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        f_iso, t_iso = _to_iso(ts_from), _to_iso(ts_to)
        f_num, t_num = float(ts_from), float(ts_to)

        if weight_type:
            sql = """
                SELECT *
                FROM weighted_measurements
                WHERE (
                    (typeof(timestamp)='text' AND timestamp BETWEEN ? AND ?)
                    OR
                    (typeof(timestamp) IN ('real','integer') AND CAST(timestamp AS REAL) BETWEEN ? AND ?)
                )
                  AND weight_type = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            return self._query(sql, (f_iso, t_iso, f_num, t_num, weight_type, limit))
        else:
            sql = """
                SELECT *
                FROM weighted_measurements
                WHERE (
                    (typeof(timestamp)='text' AND timestamp BETWEEN ? AND ?)
                    OR
                    (typeof(timestamp) IN ('real','integer') AND CAST(timestamp AS REAL) BETWEEN ? AND ?)
                )
                ORDER BY timestamp DESC
                LIMIT ?
            """
            return self._query(sql, (f_iso, t_iso, f_num, t_num, limit))

    # -------------------- служебные --------------------

    def latest_umik(self) -> Optional[Dict[str, Any]]:
        rows = self._query("SELECT * FROM measurements ORDER BY timestamp DESC LIMIT 1")
        return rows[0] if rows else None

    def latest_analog(self, weight_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if weight_type:
            rows = self._query(
                "SELECT * FROM weighted_measurements WHERE weight_type = ? ORDER BY timestamp DESC LIMIT 1",
                (weight_type,),
            )
        else:
            rows = self._query(
                "SELECT * FROM weighted_measurements ORDER BY timestamp DESC LIMIT 1"
            )
        return rows[0] if rows else None
