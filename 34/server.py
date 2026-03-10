import uvicorn
import os
import traceback
import requests
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

# ====================== КОНФИГУРАЦИЯ ======================
MODEL_A_ID = 23  # ← первая модель (A)
MODEL_B_ID = 31  # ← вторая модель (B)
SERVICE_ID = 34  # ← id этой комплексной модели в vlad.version_microservice

BEST_URL = "https://server.brain-project.online/best.php"
# =============================================================================

NODE_NAME = f"brain-complex-{MODEL_A_ID}-{MODEL_B_ID}-microservice"
SERVICE_NAME = NODE_NAME

# ── Трассировка ошибок ────────────────────────────────────────────────────────
TRACE_URL = "https://server.brain-project.online/trace.php"
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception):
    logs = f"Node: {NODE_NAME}\nException: {repr(exc)}\n\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL,
                      data={"url": "fastapi_microservice",
                            "node": NODE_NAME,
                            "email": EMAIL,
                            "logs": logs},
                      timeout=8)
    except Exception:
        pass

# ── БД ────────────────────────────────────────────────────────────────────────
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "vlad")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "vlad")
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = os.getenv("MASTER_PORT", "3306")
MASTER_USER = os.getenv("MASTER_USER", "vlad")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD", "")
MASTER_NAME = os.getenv("MASTER_NAME", "brain")
DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BRAIN_DATABASE_URL = f"mysql+aiomysql://{MASTER_USER}:{MASTER_PASSWORD}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}"
engine_vlad = create_async_engine(DATABASE_URL, pool_size=10, echo=False)
engine_brain = create_async_engine(BRAIN_DATABASE_URL, pool_size=6, echo=False)


# ── Вспомогательная функция: загрузка весов у дочернего сервиса ───────────────
def _fetch_weights_from_child(url: str, model_id: int) -> list[str]:
    """
    Синхронный запрос к /weights дочернего микросервиса.
    Возвращает список строк-весов или поднимает исключение.
    """
    r = requests.get(f"{url}/weights", timeout=10)
    r.raise_for_status()
    data = r.json()
    if "weights" not in data:
        raise ValueError(f"Ответ модели {model_id} не содержит ключа 'weights'")
    return data["weights"]


# ── FastAPI ───────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Загружаем URL-ы из brain_service
    async with engine_brain.connect() as conn:
        res = await conn.execute(text("""
            SELECT id, url 
            FROM brain_service 
            WHERE id IN (:a, :b) AND active = 1
        """), {"a": MODEL_A_ID, "b": MODEL_B_ID})

        urls = {row[0]: row[1].rstrip('/') for row in res.fetchall()}

    app.state.URL_A = urls.get(MODEL_A_ID)
    app.state.URL_B = urls.get(MODEL_B_ID)

    if not app.state.URL_A:
        raise RuntimeError(f"❌ URL для модели {MODEL_A_ID} не найден в brain_service или не active=1")
    if not app.state.URL_B:
        raise RuntimeError(f"❌ URL для модели {MODEL_B_ID} не найден в brain_service или не active=1")

    print(f"✅ Загружены URL из brain_service:")
    print(f"   {MODEL_A_ID} → {app.state.URL_A}")
    print(f"   {MODEL_B_ID} → {app.state.URL_B}")

    # 2. Определяем, какие поля параметров существуют в таблицах сигналов
    async with engine_brain.connect() as conn:
        result_a = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_A_ID}`"))
        cols_a = [row[0] for row in result_a.fetchall() if row[0] in ('type', 'var', 'param')]
        app.state.cols_A = cols_a

        result_b = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_B_ID}`"))
        cols_b = [row[0] for row in result_b.fetchall() if row[0] in ('type', 'var', 'param')]
        app.state.cols_B = cols_b

    print(f"✅ Поля сигналов модели {MODEL_A_ID}: {app.state.cols_A}")
    print(f"✅ Поля сигналов модели {MODEL_B_ID}: {app.state.cols_B}")

    # 3. Загружаем флаги static из brain_models
    async with engine_brain.connect() as conn:
        res = await conn.execute(text("""
            SELECT id, static
            FROM brain_models
            WHERE id IN (:a, :b)
        """), {"a": MODEL_A_ID, "b": MODEL_B_ID})
        static_flags = {row[0]: bool(row[1]) for row in res.fetchall()}

    app.state.static_A = static_flags.get(MODEL_A_ID, True)
    app.state.static_B = static_flags.get(MODEL_B_ID, True)

    print(f"✅ Флаги static:")
    print(f"   {MODEL_A_ID} → static={app.state.static_A}")
    print(f"   {MODEL_B_ID} → static={app.state.static_B}")

    # 4. Для нестатичных моделей загружаем веса при старте
    #    Статичные модели не кешируют веса (запрашиваются «на лету» в /weights)
    app.state.weights_A = None
    app.state.weights_B = None

    if not app.state.static_A:
        try:
            app.state.weights_A = _fetch_weights_from_child(app.state.URL_A, MODEL_A_ID)
            print(f"✅ Предзагружены веса модели {MODEL_A_ID} "
                  f"({len(app.state.weights_A)} шт.) — нестатичная модель")
        except Exception as e:
            print(f"⚠️ Не удалось предзагрузить веса модели {MODEL_A_ID}: {e}")
            send_error_trace(e)

    if not app.state.static_B:
        try:
            app.state.weights_B = _fetch_weights_from_child(app.state.URL_B, MODEL_B_ID)
            print(f"✅ Предзагружены веса модели {MODEL_B_ID} "
                  f"({len(app.state.weights_B)} шт.) — нестатичная модель")
        except Exception as e:
            print(f"⚠️ Не удалось предзагрузить веса модели {MODEL_B_ID}: {e}")
            send_error_trace(e)

    yield
    await engine_vlad.dispose()
    await engine_brain.dispose()

app = FastAPI(lifespan=lifespan)


# ── Метаданные ────────────────────────────────────────────────────────────────
@app.get("/")
async def get_metadata():
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            "SELECT version FROM version_microservice WHERE microservice_id = :sid"),
            {"sid": SERVICE_ID})
        row = res.fetchone()
        version = row[0] if row else 0

    return {
        "status": "ok",
        "version": f"1.{version}.0",
        "name": SERVICE_NAME,
        "text": f"Composite model {MODEL_A_ID} + {MODEL_B_ID}",
        "child_urls": {
            str(MODEL_A_ID): app.state.URL_A,
            str(MODEL_B_ID): app.state.URL_B,
        },
        "static": {
            str(MODEL_A_ID): app.state.static_A,
            str(MODEL_B_ID): app.state.static_B,
        },
        "weights_cached": {
            str(MODEL_A_ID): app.state.weights_A is not None,
            str(MODEL_B_ID): app.state.weights_B is not None,
        },
        "params": {
            "pair": "1=EURUSD, 3=BTC, 4=ETH",
            "day": "1=daily, 0=hourly",
            "params_format": f"{{ {MODEL_A_ID}: {{type, var, k}}, {MODEL_B_ID}: {{type, var, k}} }}"
        },
        "metadata": {
            "child_models": [MODEL_A_ID, MODEL_B_ID],
            "static_flags": {
                str(MODEL_A_ID): app.state.static_A,
                str(MODEL_B_ID): app.state.static_B,
            },
            "weights_cached": {
                str(MODEL_A_ID): app.state.weights_A is not None,
                str(MODEL_B_ID): app.state.weights_B is not None,
            }
        }
    }


# ── Weights ───────────────────────────────────────────────────────────────────
@app.get("/weights")
async def get_weights():
    try:
        # Модель A
        if not app.state.static_A and app.state.weights_A is not None:
            w_a = app.state.weights_A
        else:
            w_a = _fetch_weights_from_child(app.state.URL_A, MODEL_A_ID)

        # Модель B
        if not app.state.static_B and app.state.weights_B is not None:
            w_b = app.state.weights_B
        else:
            w_b = _fetch_weights_from_child(app.state.URL_B, MODEL_B_ID)

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Child service unreachable: {e}")

    combined = (
        [f"{MODEL_A_ID}_" + w for w in w_a]
        + [f"{MODEL_B_ID}_" + w for w in w_b]
    )
    # list(set(...)) убирает дубли, но теряет порядок — сохраняем порядок через dict
    seen = {}
    for w in combined:
        seen[w] = True
    deduped = list(seen.keys())

    return {"weights": deduped, "total": len(deduped)}

@app.get("/new_weights")
async def new_weights():
    all_weights: list[str] = []

    for model_id, url, is_static, attr in [
        (MODEL_A_ID, app.state.URL_A, app.state.static_A, "weights_A"),
        (MODEL_B_ID, app.state.URL_B, app.state.static_B, "weights_B"),
    ]:
        if is_static:
            try:
                weights = _fetch_weights_from_child(url, model_id)
            except Exception as e:
                send_error_trace(e)
                weights = []
        else:
            try:
                weights = _fetch_weights_from_child(url, model_id)
                setattr(app.state, attr, weights)
            except Exception as e:
                send_error_trace(e)
                weights = getattr(app.state, attr) or []

        all_weights += [f"{model_id}_" + w for w in weights]

    # Дедупликация с сохранением порядка
    seen: dict[str, bool] = {}
    for w in all_weights:
        seen[w] = True

    return {"weights": list(seen.keys())}


# ── Values ────────────────────────────────────────────────────────────────────
@app.get("/values")
async def get_values(
        pair: int = Query(1),
        day: int = Query(1),
        date: str = Query(...),
        params: str = Query(...),
):
    try:
        param_dict = json.loads(params)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON in params")

    combined = {}
    for model_str, subp in param_dict.items():
        model_id = int(model_str)
        k = subp.get("k")
        if k is None:
            raise HTTPException(status_code=400, detail="Missing k in params")

        if model_id == MODEL_A_ID:
            url = app.state.URL_A
        elif model_id == MODEL_B_ID:
            url = app.state.URL_B
        else:
            continue  # неизвестная модель – пропускаем

        query_params = {key: val for key, val in subp.items() if key != 'k'}
        query_params.update({'pair': pair, 'day': day, 'date': date})

        try:
            r = requests.get(f"{url}/values", params=query_params)
            res = r.json()
            if "error" in res:
                return res
            for key, val in res.items():
                combined[f"{model_id}_{key}"] = round(val * k, 6)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Child model {model_id} error: {e}")

    return combined


# ── Params ────────────────────────────────────────────────────────────────────
@app.get("/params")
async def get_params(
        pair: int = Query(1),
        day: int = Query(1),
        tier: int = Query(..., ge=0, le=1),
):
    max_per_model = 4 if tier == 0 else 3

    try:
        best_a = requests.get(f"{BEST_URL}?neuronet_id={MODEL_A_ID}&pair={pair}&day={day}").json()
        best_b = requests.get(f"{BEST_URL}?neuronet_id={MODEL_B_ID}&pair={pair}&day={day}").json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"best.php error: {e}")

    sorted_a = sorted(best_a, key=best_a.get, reverse=True)
    sorted_b = sorted(best_b, key=best_b.get, reverse=True)

    TABLE_A = f"brain_signal{MODEL_A_ID}"
    TABLE_B = f"brain_signal{MODEL_B_ID}"

    params_a = []
    params_b = []

    async with engine_brain.connect() as conn:
        for sid in sorted_a:
            if len(params_a) >= max_per_model:
                break
            if app.state.cols_A:
                cols_str = ", ".join(app.state.cols_A)
                query = text(f"SELECT {cols_str} FROM `{TABLE_A}` WHERE id = :sid AND tier = :tier AND is_day = :day")
                res = await conn.execute(query, {"sid": sid, "tier": tier, "day": day})
                row = res.fetchone()
                if row:
                    param_obj = {}
                    for idx, col in enumerate(app.state.cols_A):
                        value = row[idx]
                        if col == 'param' and value is not None:
                            param_obj[col] = [value]
                        else:
                            param_obj[col] = value
                    params_a.append(param_obj)

        for sid in sorted_b:
            if len(params_b) >= max_per_model:
                break
            if app.state.cols_B:
                cols_str = ", ".join(app.state.cols_B)
                query = text(f"SELECT {cols_str} FROM `{TABLE_B}` WHERE id = :sid AND tier = :tier AND is_day = :day")
                res = await conn.execute(query, {"sid": sid, "tier": tier, "day": day})
                row = res.fetchone()
                if row:
                    param_obj = {}
                    for idx, col in enumerate(app.state.cols_B):
                        value = row[idx]
                        if col == 'param' and value is not None:
                            param_obj[col] = [value]
                        else:
                            param_obj[col] = value
                    params_b.append(param_obj)

    combs = []
    if tier == 0:
        for pa in params_a:
            for pb in params_b:
                combs.append({
                    str(MODEL_A_ID): {**pa, "k": 0.5},
                    str(MODEL_B_ID): {**pb, "k": 0.5}
                })
    else:
        for pa in params_a:
            for pb in params_b:
                for ki in range(1, 10):
                    k = round(ki / 10, 1)
                    combs.append({
                        str(MODEL_A_ID): {**pa, "k": k},
                        str(MODEL_B_ID): {**pb, "k": round(1 - k, 1)}
                    })

    return combs[:150]


# ── Patch ─────────────────────────────────────────────────────────────────────
@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        res = await conn.execute(text(
            "SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        row = res.fetchone()
        old = row[0] if row else 0
        new = max(old, 1)
        if new != old:
            await conn.execute(text(
                "UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                {"v": new, "id": SERVICE_ID})
    return {"status": "ok", "version": new}


# ── Запуск ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _workers = int(os.getenv("WORKERS", "1"))
    uvicorn.run("server:app", host="0.0.0.0", port=8897, reload=False, workers=_workers)