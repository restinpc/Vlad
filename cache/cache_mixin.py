
import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from typing import Callable, Awaitable, Optional

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text


def _canonical_json(data: dict) -> str:
    """отсортированные ключи, без пробелов."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _checksum(data: dict) -> str:
    """MD5 от JSON."""
    return hashlib.md5(_canonical_json(data).encode()).hexdigest()


class CacheMixin:
    CACHE_TABLE = "vlad_values_cache"

    def __init__(
        self,
        engine: AsyncEngine,
        service_port: int,
        *,
        realtime_window_hours: int = 6,
    ):
        self.engine = engine
        self.service_port = service_port
        self.realtime_window = timedelta(hours=realtime_window_hours)

    # ── Чтение из кеша ──────────────────────────────────────────────────────

    async def cache_read(
        self, pair: int, day: int, date_key: datetime,
        calc_type: int = 0, calc_var: int = 0
    ) -> Optional[dict]:
        """
        Читает закешированный ответ.
        Возвращает dict с ключами weight_code→float или None если промах.
        """
        async with self.engine.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT output_json
                FROM {self.CACHE_TABLE}
                WHERE service_port = :sp
                  AND pair         = :pair
                  AND day          = :day
                  AND date_key     = :dk
                  AND calc_type    = :ct
                  AND calc_var     = :cv
                LIMIT 1
            """), {
                "sp": self.service_port,
                "pair": pair,
                "day": day,
                "dk": date_key,
                "ct": calc_type,
                "cv": calc_var,
            })
            row = res.fetchone()
            if row is None:
                return None
            raw = row[0]
            if isinstance(raw, str):
                return json.loads(raw)
            return raw  # MySQL JSON → уже dict

    # ── Запись в кеш ────────────────────────────────────────────────────────

    async def cache_write(
        self, pair: int, day: int, date_key: datetime,
        calc_type: int, calc_var: int,
        output: dict
    ) -> None:
        """Upsert одной записи в кеш."""
        jdata = _canonical_json(output)
        chk = _checksum(output)
        fc = sum(1 for v in output.values() if v != 0)

        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO {self.CACHE_TABLE}
                    (service_port, pair, day, date_key, calc_type, calc_var,
                     output_json, field_count, checksum, computed_at)
                VALUES
                    (:sp, :pair, :day, :dk, :ct, :cv, :oj, :fc, :chk, NOW())
                ON DUPLICATE KEY UPDATE
                    output_json = VALUES(output_json),
                    field_count = VALUES(field_count),
                    checksum    = VALUES(checksum),
                    computed_at = NOW()
            """), {
                "sp": self.service_port,
                "pair": pair,
                "day": day,
                "dk": date_key,
                "ct": calc_type,
                "cv": calc_var,
                "oj": jdata,
                "fc": fc,
                "chk": chk,
            })

    # ── Пакетная запись ──────────────────────────────────────────────────────

    async def cache_write_batch(
        self, rows: list[dict]
    ) -> int:
        """
        Пакетный upsert. Каждый элемент rows — dict с ключами:
        pair, day, date_key, calc_type, calc_var, output.
        Возвращает количество записанных строк.
        """
        if not rows:
            return 0

        values_parts = []
        params = {}
        for i, row in enumerate(rows):
            output = row["output"]
            jdata = _canonical_json(output)
            chk = _checksum(output)
            fc = sum(1 for v in output.values() if v != 0)

            values_parts.append(
                f"(:sp_{i}, :pair_{i}, :day_{i}, :dk_{i}, :ct_{i}, :cv_{i}, "
                f":oj_{i}, :fc_{i}, :chk_{i}, NOW())"
            )
            params[f"sp_{i}"] = self.service_port
            params[f"pair_{i}"] = row["pair"]
            params[f"day_{i}"] = row["day"]
            params[f"dk_{i}"] = row["date_key"]
            params[f"ct_{i}"] = row["calc_type"]
            params[f"cv_{i}"] = row["calc_var"]
            params[f"oj_{i}"] = jdata
            params[f"fc_{i}"] = fc
            params[f"chk_{i}"] = chk

        sql = f"""
            INSERT INTO {self.CACHE_TABLE}
                (service_port, pair, day, date_key, calc_type, calc_var,
                 output_json, field_count, checksum, computed_at)
            VALUES {', '.join(values_parts)}
            ON DUPLICATE KEY UPDATE
                output_json = VALUES(output_json),
                field_count = VALUES(field_count),
                checksum    = VALUES(checksum),
                computed_at = NOW()
        """
        async with self.engine.begin() as conn:
            await conn.execute(text(sql), params)
        return len(rows)

    # ── Обёртка /values с авто-fallback ──────────────────────────────────────

    async def cached_values(
        self,
        pair: int,
        day: int,
        date_str: str,
        calc_type: int,
        calc_var: int,
        compute_fn: Callable[[], Awaitable[dict]],
        *,
        parse_date_fn: Optional[Callable] = None,
    ) -> dict:
        """
        Главная точка входа для /values с кешированием.

        Логика:
        1. Парсим дату.
        2. Если дата в real-time окне (свежее now - realtime_window):
           → вычисляем live, пишем в кеш, возвращаем.
        3. Иначе — пытаемся читать из кеша:
           → hit  → возвращаем из кеша.
           → miss → вычисляем live, пишем в кеш, возвращаем.
        """
        # Парсинг даты
        if parse_date_fn:
            date_key = parse_date_fn(date_str)
        else:
            date_key = self._default_parse_date(date_str)

        if date_key is None:
            # Не смогли распарсить — просто считаем live
            return await compute_fn()

        now = datetime.now()
        is_realtime = date_key > (now - self.realtime_window)

        # Real-time: всегда считаем live
        if is_realtime:
            result = await compute_fn()
            if isinstance(result, dict) and "error" not in result:
                # Пишем в кеш асинхронно (fire-and-forget)
                asyncio.create_task(
                    self._safe_write(pair, day, date_key, calc_type, calc_var, result)
                )
            return result

        # Историческая дата: пробуем кеш
        cached = await self.cache_read(pair, day, date_key, calc_type, calc_var)
        if cached is not None:
            return cached

        # Cache miss → compute + save
        result = await compute_fn()
        if isinstance(result, dict) and "error" not in result:
            asyncio.create_task(
                self._safe_write(pair, day, date_key, calc_type, calc_var, result)
            )
        return result

    async def _safe_write(self, pair, day, date_key, calc_type, calc_var, result):
        """Безопасная запись — глотает ошибки."""
        try:
            await self.cache_write(pair, day, date_key, calc_type, calc_var, result)
        except Exception as e:
            print(f"⚠️ Cache write error: {e}")

    @staticmethod
    def _default_parse_date(date_str: str) -> Optional[datetime]:
        for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    # ── Статистика покрытия ──────────────────────────────────────────────────

    async def get_coverage_stats(self) -> list[dict]:
        """Возвращает статистику покрытия кеша для данного сервиса."""
        async with self.engine.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT
                    pair, day, calc_type, calc_var,
                    COUNT(*) AS total_cached,
                    SUM(IF(field_count > 0, 1, 0)) AS nonempty,
                    ROUND(AVG(IF(field_count > 0, field_count, NULL)), 1) AS avg_fields,
                    ROUND(SUM(IF(field_count > 0, 1, 0)) / COUNT(*) * 100, 1) AS coverage_pct,
                    MIN(date_key) AS min_date,
                    MAX(date_key) AS max_date
                FROM {self.CACHE_TABLE}
                WHERE service_port = :sp
                GROUP BY pair, day, calc_type, calc_var
                ORDER BY pair, day, calc_type, calc_var
            """), {"sp": self.service_port})
            return [dict(r._mapping) for r in res.fetchall()]
