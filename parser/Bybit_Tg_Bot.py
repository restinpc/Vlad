import asyncio
import argparse
import logging
import os
import signal
import sys
import traceback
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from telethon import TelegramClient, events

# ==================== НАСТРОЙКИ ====================
load_dotenv()

API_ID = int(os.getenv("API_ID", 32475085))
API_HASH = os.getenv("API_HASH", "2152018f5ca2fa91b67c5dbf589f89e7")
PHONE = os.getenv("PHONE", "+79330941672")

TARGET_BOT = "@Bybit_TradeGPT_bot"

RESPONSE_TIMEOUT = int(os.getenv("RESPONSE_TIMEOUT", 30))
MAX_WAIT = int(os.getenv("MAX_WAIT", 120))

SESSION_FILE = os.getenv("SESSION_FILE", "/brain/Brain-Services/parser/test_session")

TRACE_URL = os.getenv("TRACE_URL", "https://server.brain-project.online/trace.php")
NODE_NAME = os.getenv("NODE_NAME", "bybit_trend_bot")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "your@email.com")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
shutdown = asyncio.Event()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def send_error_trace(exc: Exception, script_name: str = "Bybit_Tg_Bot.py"):
    """Отправка информации об ошибке на сервер трекинга."""
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": ALERT_EMAIL,
        "logs": logs
    }
    try:
        requests.post(TRACE_URL, data=payload, timeout=10)
    except Exception:
        pass


# ==================== ПАРСЕРЫ ====================

def extract_asset(query: str) -> str:
    """Извлекает тикер из запроса (BTC, ETH или UNKNOWN)."""
    for token in query.upper().split():
        if token in ('BTC', 'ETH'):
            return token
    return 'UNKNOWN'


# ==================== РАБОТА С БОТОМ ====================

async def collect_response(bot_id: int, query: str) -> str:
    """
    Отправляет запрос боту и собирает все последующие текстовые сообщения от него.
    Возвращает объединённый текст.
    """
    queue: asyncio.Queue[str] = asyncio.Queue()

    async def handler(event):
        if event.message.text:
            queue.put_nowait(event.message.text)

    client.add_event_handler(handler, events.NewMessage(from_users=bot_id))

    try:
        log.info(f"→ {query}")
        await client.send_message(TARGET_BOT, query)

        parts, deadline = [], asyncio.get_event_loop().time() + MAX_WAIT

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.warning("⏰ Таймаут сбора ответа")
                break
            try:
                text = await asyncio.wait_for(
                    queue.get(),
                    timeout=min(RESPONSE_TIMEOUT, remaining)
                )
                parts.append(text)
            except asyncio.TimeoutError:
                break

        return '\n\n'.join(parts)
    finally:
        client.remove_event_handler(handler, events.NewMessage)


# ==================== РАБОТА С БАЗОЙ ДАННЫХ  ====================

def ensure_table_exists(engine, table_name, database_name):
    """
    Проверяет наличие таблицы, создаёт её при отсутствии.
    Дополнительно проверяет наличие первичного ключа (не обязательно).
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{database_name}' AND table_name = '{table_name}'
        """))
        table_exists = result.scalar() > 0

    if not table_exists:
        create_query = text(f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            asset VARCHAR(20) NOT NULL,
            raw_response LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        with engine.connect() as conn:
            conn.execute(create_query)
            conn.commit()
        log.info(f"✅ Таблица '{table_name}' успешно создана")
    else:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = '{database_name}'
                AND table_name = '{table_name}'
                AND index_name = 'PRIMARY'
            """))
            has_primary = result.scalar() > 0
        if not has_primary:
            log.warning(f"⚠️ В таблице '{table_name}' отсутствует первичный ключ. Рекомендуется добавить.")
        else:
            log.info(f"✓ Таблица '{table_name}' уже существует, первичный ключ найден.")


def save_record(engine, table_name, asset, raw_response):
    """
    Сохраняет одну запись в таблицу.
    Возвращает id вставленной записи или None при ошибке.
    """
    insert_sql = text(f"""
        INSERT INTO {table_name} (asset, raw_response)
        VALUES (:asset, :raw_response)
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(insert_sql, {
                'asset': asset,
                'raw_response': raw_response
            })
            conn.commit()
            inserted_id = result.lastrowid
            log.debug(f"Запись добавлена, ID: {inserted_id}")
            return inserted_id
    except SQLAlchemyError as e:
        log.error(f"❌ Ошибка вставки в БД: {e}")
        send_error_trace(e, "save_record")
        return None


# ==================== ОСНОВНАЯ ЛОГИКА ====================

async def process_query(query: str, bot_id: int, engine, table_name):
    """Получает ответ, извлекает asset и сохраняет в БД."""
    try:
        raw = await collect_response(bot_id, query)
        if not raw.strip():
            log.warning("⚠ Пустой ответ")
            return None

        asset = extract_asset(query)
        inserted_id = save_record(engine, table_name, asset, raw)

        if inserted_id:
            log.info(f"✓ [ID {inserted_id}] {asset}: сохранено {len(raw)} симв.")
        else:
            log.info(f"⚠ {asset}: не сохранено")

        return inserted_id
    except Exception as e:
        log.error(f"❌ Ошибка '{query}': {e}", exc_info=True)
        send_error_trace(e, "process_query")
        return None


# ==================== ТОЧКА ВХОДА ====================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Однократный сбор ответа от @Bybit_TradeGPT_bot с сохранением в MySQL"
    )
    parser.add_argument("table_name", help="Имя целевой таблицы в БД")
    parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
    parser.add_argument("user", nargs="?", default=os.getenv("DB_USER", "root"))
    parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME", "test"))
    # Все оставшиеся аргументы объединяются в одну строку запроса
    parser.add_argument("query", nargs=argparse.REMAINDER, help="Текст запроса (можно писать без кавычек, всё после database будет объединено)")
    return parser.parse_args()


async def main_async(args):
    # Объединяем части запроса в одну строку
    if not args.query:
        log.error("❌ Не указан текст запроса")
        sys.exit(1)
    query_str = ' '.join(args.query)

    db_url = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
    engine = create_engine(
        db_url,
        pool_recycle=3600,
        connect_args={"auth_plugin": "caching_sha2_password"}
    )

    ensure_table_exists(engine, args.table_name, args.database)

    await client.connect()
    if not await client.is_user_authorized():
        log.error("❌ Нет активной сессии. Сначала авторизуйтесь вручную или удалите сессионный файл и запустите с QR-кодом.")
        return
    else:
        me = await client.get_me()
        log.info(f"✅ Сессия загружена: {me.username or me.first_name}")

    bot_entity = await client.get_entity(TARGET_BOT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    log.info(f"📨 Запрос: {query_str}")
    await process_query(query_str, bot_entity.id, engine, args.table_name)

    await client.disconnect()
    log.info("👋 Соединение закрыто")


def main():
    args = parse_args()

    log.info("=" * 60)
    log.info("🚀 Однократный сборщик ответов Bybit")
    log.info(f"База: {args.host}:{args.port}/{args.database}")
    log.info(f"Таблица: {args.table_name}")
    log.info(f"Запрос: {' '.join(args.query) if args.query else 'Не указан'}")
    log.info("=" * 60)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("🛑 Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)


if __name__ == "__main__":
    main()