import uvicorn
import os
import asyncio
import traceback
import requests
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv
import bisect

# ── Трассировка ошибок ────────────────────────────────────────────────────────
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "brain-weights-microservice")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "server.py"):
    logs = (
        f"Node: {NODE_NAME}\nScript: {script_name}\n"
        f"Exception: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    )
    try:
        print(f"\n📤 [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
        r = requests.post(TRACE_URL, data={"url": "fastapi_microservice",
                                            "node": NODE_NAME, "email": EMAIL,
                                            "logs": logs}, timeout=10)
        print(f"✅ [POST] Статус: {r.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")


# ── Конфигурация ──────────────────────────────────────────────────────────────
load_dotenv()

DB_HOST         = os.getenv("DB_HOST",         "127.0.0.1")
DB_PORT         = os.getenv("DB_PORT",         "3306")
DB_USER         = os.getenv("DB_USER",         "vlad")
DB_PASSWORD     = os.getenv("DB_PASSWORD",     "")
DB_NAME         = os.getenv("DB_NAME",         "vlad")
MASTER_HOST     = os.getenv("MASTER_HOST",     "127.0.0.1")
MASTER_PORT     = os.getenv("MASTER_PORT",     "3306")
MASTER_USER     = os.getenv("MASTER_USER",     "vlad")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD", "")
MASTER_NAME     = os.getenv("MASTER_NAME",     "brain")

DATABASE_URL       = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BRAIN_DATABASE_URL = f"mysql+aiomysql://{MASTER_USER}:{MASTER_PASSWORD}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}"

print(f"📊 vlad:  {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"📊 brain: {MASTER_USER}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}")

engine_vlad  = create_async_engine(DATABASE_URL,       pool_size=10, echo=False)
engine_brain = create_async_engine(BRAIN_DATABASE_URL, pool_size=5,  echo=False)

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_EXTREMUMS     = {}   # table → {'min': set[date], 'max': set[date]}
GLOBAL_RATES         = {}   # table → {date: t1}
GLOBAL_CANDLE_RANGES = {}   # table → {date: float}
GLOBAL_AVG_RANGE     = {}   # table → float
GLOBAL_LAST_CANDLES  = {}   # table → [(date, is_bull), ...]

# Календарь: date → [{"EventId", "Importance", "event_date", "actual", "forecast", "previous"}, ...]
GLOBAL_CALENDAR      = {}
# История дат по event_id (отсортированная)
GLOBAL_HISTORY       = {}   # event_id → [date, ...]

# Контекстный индекс: (event_id, fdir, sdir, adir) → {"occurrence_count", "importance", ...}
GLOBAL_CTX_INDEX     = {}

# Список всех weight_code из таблицы весов
GLOBAL_WEIGHT_CODES  = []

LAST_RELOAD_TIME     = None

# Порог для "recurring" события
RECURRING_MIN_COUNT  = 2


def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    suffix = "_day" if day_flag == 1 else ""
    return {1: "brain_rates_eur_usd",
            3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + suffix


def get_modification_factor(pair_id: int) -> float:
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


# ── Определение контекста события ────────────────────────────────────────────

THRESHOLD = 0.0001


def _direction(a, b, beat_label="UP", miss_label="DOWN", inline_label="FLAT") -> str:
    """Сравнивает два float-значения и возвращает метку направления."""
    if a is None or b is None:
        return "UNKNOWN"
    if a > b + THRESHOLD:
        return beat_label
    if a < b - THRESHOLD:
        return miss_label
    return inline_label


def resolve_event_context(actual, forecast, previous) -> tuple[str, str, str]:
    """
    Возвращает (forecast_direction, surprise_direction, actual_direction)
    по той же логике, что и SQL в inv_cal_event_indx.py.
    """
    fdir = _direction(forecast, previous, "UP",   "DOWN",   "FLAT")
    sdir = _direction(actual,   forecast,  "BEAT", "MISS",   "INLINE")
    adir = _direction(actual,   previous,  "UP",   "DOWN",   "FLAT")
    return fdir, sdir, adir


def try_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


# ── Предзагрузка ──────────────────────────────────────────────────────────────

async def preload_all_data():
    global LAST_RELOAD_TIME
    print("🔄 FULL DATA RELOAD STARTED")

    async with engine_vlad.connect() as conn:

        # Все weight_code
        res = await conn.execute(
            text("SELECT weight_code FROM vlad_investing_weights"))
        GLOBAL_WEIGHT_CODES[:] = [r["weight_code"] for r in res.mappings().all()]
        print(f"  weight_codes загружено: {len(GLOBAL_WEIGHT_CODES)}")

        # Контекстный индекс
        res = await conn.execute(text("""
            SELECT event_id, forecast_direction, surprise_direction, actual_direction,
                   occurrence_count, importance, currency
            FROM vlad_investing_event_context_idx
        """))
        GLOBAL_CTX_INDEX.clear()
        for r in res.mappings().all():
            key = (r["event_id"],
                   r["forecast_direction"],
                   r["surprise_direction"],
                   r["actual_direction"])
            GLOBAL_CTX_INDEX[key] = {
                "occurrence_count": r["occurrence_count"] or 0,
                "importance":       r["importance"],
                "currency":         r["currency"],
            }
        print(f"  ctx_index загружено: {len(GLOBAL_CTX_INDEX)} контекстов")

        # Календарь (с actual/forecast/previous для определения контекста)
        res = await conn.execute(text("""
            SELECT event_id, occurrence_time_utc, importance,
                   actual, forecast, previous
            FROM vlad_investing_calendar
            WHERE event_id IS NOT NULL
        """))
        GLOBAL_CALENDAR.clear()
        GLOBAL_HISTORY.clear()
        for r in res.mappings().all():
            dt  = r["occurrence_time_utc"]
            eid = r["event_id"]
            GLOBAL_HISTORY.setdefault(eid, []).append(dt)
            GLOBAL_CALENDAR.setdefault(dt, []).append({
                "EventId":    eid,
                "Importance": r["importance"],
                "event_date": dt,
                "actual":     try_float(r["actual"]),
                "forecast":   try_float(r["forecast"]),
                "previous":   try_float(r["previous"]),
            })

        # Сортируем историю по дате
        for eid in GLOBAL_HISTORY:
            GLOBAL_HISTORY[eid].sort()

    # Котировки и экстремумы
    tables = [
        "brain_rates_eur_usd", "brain_rates_eur_usd_day",
        "brain_rates_btc_usd", "brain_rates_btc_usd_day",
        "brain_rates_eth_usd", "brain_rates_eth_usd_day",
    ]
    for table in tables:
        GLOBAL_RATES[table]         = {}
        GLOBAL_LAST_CANDLES[table]  = []
        GLOBAL_CANDLE_RANGES[table] = {}
        GLOBAL_AVG_RANGE[table]     = 0.0
        GLOBAL_EXTREMUMS[table]     = {"min": set(), "max": set()}

        async with engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `max`, `min`, t1 FROM {table}"))
            rows = sorted(res.mappings().all(), key=lambda x: x["date"])

            ranges = []
            for r in rows:
                dt = r["date"]
                if r["t1"] is not None:
                    GLOBAL_RATES[table][dt] = float(r["t1"])
                is_bull = r["close"] > r["open"]
                GLOBAL_LAST_CANDLES[table].append((dt, is_bull))
                rng = float(r["max"] or 0) - float(r["min"] or 0)
                GLOBAL_CANDLE_RANGES[table][dt] = rng
                ranges.append(rng)

            GLOBAL_AVG_RANGE[table] = sum(ranges) / len(ranges) if ranges else 0.0

            for typ in ("min", "max"):
                op  = ">" if typ == "max" else "<"
                col = "max" if typ == "max" else "min"
                q = f"""
                    SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                    JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                    WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                """
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {
                    r["date"] for r in res_ext.mappings().all()}

        print(f"  {table}: {len(GLOBAL_RATES[table])} свечей")

    LAST_RELOAD_TIME = datetime.now()
    print("✅ FULL DATA RELOAD COMPLETED")


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "server_background_reload")


# ── FastAPI lifespan ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_all_data()
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()


app = FastAPI(lifespan=lifespan)


# ── Вспомогательные функции ───────────────────────────────────────────────────

def find_prev_candle_trend(table: str, target_date: datetime):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None


def get_last_known_context(event_id: int, before_date: datetime) -> tuple[str, str, str]:
    """
    Находит последний релиз события строго до before_date
    и возвращает его (fdir, sdir, adir).
    Если данных нет или все None — возвращает ('UNKNOWN', 'UNKNOWN', 'UNKNOWN').
    """
    history = GLOBAL_HISTORY.get(event_id, [])
    # Ищем последнюю дату < before_date
    idx = bisect.bisect_left(history, before_date)
    if idx == 0:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"

    # Идём назад от idx-1 — ищем первую запись с хоть каким-то данными
    for i in range(idx - 1, -1, -1):
        dt = history[i]
        for ev in GLOBAL_CALENDAR.get(dt, []):
            if ev["EventId"] == event_id:
                fdir, sdir, adir = resolve_event_context(
                    ev["actual"], ev["forecast"], ev["previous"]
                )
                return fdir, sdir, adir

    return "UNKNOWN", "UNKNOWN", "UNKNOWN"


def build_weight_code(event_id: int, fdir: str, sdir: str, adir: str,
                       mode: int, hour: int | None = None) -> str:
    base = f"{event_id}__{fdir}__{sdir}__{adir}__{mode}"
    return base if hour is None else f"{base}__{hour}"


def compute_t1_value(t_dates: list, calc_var: int,
                     ram_rates: dict, candle_ranges: dict, avg_range: float) -> float:
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4

    total = 0.0
    for d in t_dates:
        rng = candle_ranges.get(d, 0.0)
        if need_filter and rng <= avg_range:
            continue
        if use_range:
            total += rng - avg_range
        else:
            t1 = ram_rates.get(d, 0.0)
            total += t1 * abs(t1) if use_square else t1
    return total


def compute_extremum_value(t_dates: list, calc_var: int, ext_set: set,
                            candle_ranges: dict, avg_range: float,
                            modification: float, total_hist: int) -> float | None:
    need_filter = calc_var in (1, 3, 4)
    use_range   = calc_var == 4

    pool = [d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range] \
        if need_filter else t_dates

    if not pool:
        return None

    if use_range:
        val = sum(candle_ranges.get(d, 0.0) - avg_range for d in pool if d in ext_set)
        return val if val != 0 else None
    else:
        if total_hist == 0:
            return None
        matches = sum(1 for d in pool if d in ext_set)
        val = ((matches / total_hist) * 2 - 1) * modification
        return val if val != 0 else None


# ── Основной расчёт ───────────────────────────────────────────────────────────

async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                 calc_type: int = 0, calc_var: int = 0) -> dict:
    target_date = parse_date_string(date_str)
    if not target_date:
        return {"error": "Invalid date format"}

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window       = 12

    check_dates = (
        [target_date + timedelta(hours=h) for h in range(-window, window + 1)]
        if day == 0 else
        [target_date + timedelta(days=d)  for d in range(-window, window + 1)]
    )

    # Собираем события в окне
    events_in_window = []
    for dt in check_dates:
        for e in GLOBAL_CALENDAR.get(dt, []):
            if e["Importance"] != 1 or dt == target_date:
                events_in_window.append(e)

    if not events_in_window:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}

    for e in events_in_window:
        diff  = target_date - e["event_date"]
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        eid   = e["EventId"]

        # Определяем контекст последнего известного релиза до target_date
        fdir, sdir, adir = get_last_known_context(eid, target_date)

        # Проверяем, что такой контекст вообще есть в индексе
        ctx_key = (eid, fdir, sdir, adir)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue

        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT

        # Для non-recurring события shift должен быть 0
        if not is_recurring and shift != 0:
            continue
        # Для recurring — shift в пределах окна
        if is_recurring and abs(shift) > window:
            continue

        valid_dates = [d for d in GLOBAL_HISTORY.get(eid, []) if d < target_date]
        if not valid_dates:
            continue

        t_dates = (
            [d + timedelta(hours=shift) for d in valid_dates]
            if day == 0 else
            [d + timedelta(days=shift)  for d in valid_dates]
        )

        # Суффикс часа только для recurring
        hour_arg = shift if is_recurring else None

        # ── mode=0: T1-компонента ────────────────────────────────────────
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = build_weight_code(eid, fdir, sdir, adir, 0, hour_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        # ── mode=1: extremum-компонента ──────────────────────────────────
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges, avg_range,
                modification, len(valid_dates))
            if ext_val is not None:
                wc = build_weight_code(eid, fdir, sdir, adir, 1, hour_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    required_tables = [
        "vlad_investing_weights",
        "vlad_investing_event_context_idx",
        "vlad_investing_calendar",
        "version_microservice",
    ]
    brain_tables = ["brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd"]

    async with engine_vlad.connect() as conn:
        for t in required_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"Table {t} in 'vlad' inaccessible: {e}"}

    async with engine_brain.connect() as conn:
        for t in brain_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"Table {t} in 'brain' inaccessible: {e}"}

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = 25"))
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception as e:
            return {"status": "error", "error": str(e)}

    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "name":    "brain-weights-microservice",
        "text":    "Calculates historical market weights keyed by event context (fdir/sdir/adir)",
        "weight_code_format": "{event_id}__{fdir}__{sdir}__{adir}__{mode}[__{hour}]",
        "params": {
            "type": "0=T1+extremum, 1=T1 only, 2=extremum only",
            "var":  "0=all/linear, 1=filtered/linear, 2=all/squared, 3=filtered/squared, 4=filtered/range_delta",
        },
        "metadata": {
            "author": "Vlad",
            "stack":  "Python 3 + MySQL",
            "ctx_index_rows": len(GLOBAL_CTX_INDEX),
            "weight_codes":   len(GLOBAL_WEIGHT_CODES),
            "last_reload":    LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
        },
    }


@app.get("/weights")
async def get_weights():
    return {"weights": GLOBAL_WEIGHT_CODES, "total": len(GLOBAL_WEIGHT_CODES)}


@app.get("/values")
async def get_values(
    pair: int = Query(1),
    day:  int = Query(0),
    date: str = Query(...),
    type: int = Query(0, ge=0, le=2, description="0=T1+ext, 1=T1 only, 2=ext only"),
    var:  int = Query(0, ge=0, le=4, description="Алгоритм суммирования 0..4"),
):
    return await calculate_pure_memory(pair, day, date, calc_type=type, calc_var=var)


@app.post("/patch")
async def patch_service():
    service_id = 28
    async with engine_vlad.begin() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": service_id})
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail=f"Service ID {service_id} not found")
        old_version = row[0]
        new_version = max(old_version, 3)
        if new_version != old_version:
            await conn.execute(
                text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                {"v": new_version, "id": service_id})
    return {"status": "ok", "from_version": old_version, "to_version": new_version}


# ── Точка входа ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=8892, reload=False, workers=1)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)