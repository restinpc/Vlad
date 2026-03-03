#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import traceback
import asyncio
import pandas as pd
from sqlalchemy import create_engine, text
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import os
import tempfile

# Создаём папку для временных файлов Playwright в домашней директории
TEMP_DIR = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(TEMP_DIR, exist_ok=True)
# Перенаправляем все переменные окружения, влияющие на временные файлы
os.environ["PLAYWRIGHT_TMPDIR"] = TEMP_DIR
os.environ["TMPDIR"] = TEMP_DIR
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR
# Также говорим модулю tempfile использовать эту папку
tempfile.tempdir = TEMP_DIR
print(f"Временная директория Playwright: {TEMP_DIR}")

load_dotenv()

# ----------------------------------------------------------------------
# Конфигурация отправки ошибок (аналог trace.php)
# ----------------------------------------------------------------------
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_coindesk_crypto_news")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ----------------------------------------------------------------------
# Параметры парсинга CoinDesk.com
# ----------------------------------------------------------------------
BASE_URL = "https://www.coindesk.com/latest-crypto-news"
MAX_CLICKS = 999  # Защита от бесконечного цикла (практически unlimited)

# ----------------------------------------------------------------------
# Функция отправки трейса об ошибке
# ----------------------------------------------------------------------
def send_error_trace(exc: Exception, script_name: str = "coindesk_crypto_news.py"):
    """
    Отправляет информацию об исключении на сервер трейсов.
    """
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
        # Если не удалось отправить трейс – игнорируем
        pass

# ----------------------------------------------------------------------
# Парсинг аргументов командной строки
# ----------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Загрузчик новостей CoinDesk.com (крипто) в MySQL"
)
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument("--max-clicks", type=int, default=None,
                    help="Максимум кликов на 'More Stories' (по умолчанию: все доступные)")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

# ----------------------------------------------------------------------
# Формируем строку подключения SQLAlchemy
# ----------------------------------------------------------------------
SQLALCHEMY_URL = (
    f"mysql+mysqlconnector://{args.user}:{args.password}@"
    f"{args.host}:{args.port}/{args.database}"
)
engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)

# ----------------------------------------------------------------------
# Асинхронный парсинг с бесконечным скроллом и загрузкой в БД
# ----------------------------------------------------------------------
async def parse_and_save_incrementally(table_name, max_clicks=None):
    """
    Парсит страницу, кликает на 'More Stories' и сохраняет в БД инкрементально.
    Останавливается, когда кнопка исчезает или достигнут лимит кликов.
    """
    print("[*] Запуск парсера CoinDesk.com (headless mode)...")
    # Создаём таблицу если её нет
    ensure_table_exists(table_name)
    # Получаем список существующих ссылок ОДИН РАЗ в начале
    print("[*] Загрузка существующих ссылок из БД...")
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(f"SELECT link FROM {table_name}", conn)
            existing_links = set(existing['link'].tolist()) if not existing.empty else set()
        print(f" ✓ В БД уже есть {len(existing_links)} новостей")
    except Exception as e:
        print(f" ⚠️ Ошибка при получении существующих ссылок: {e}")
        existing_links = set()

    async with async_playwright() as p:
        # Headless-режим с stealth настройками
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
        )
        page = await context.new_page()
        # Скрываем webdriver
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        total_parsed = 0
        total_added = 0
        click_num = 0
        max_limit = max_clicks if max_clicks else MAX_CLICKS
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 10

        try:
            print(f"\n📄 Загрузка главной страницы: {BASE_URL}")
            await page.goto(BASE_URL, timeout=30000)
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(2)

            # Закрываем cookie-баннер если есть
            try:
                accept_btn = page.locator('button:has-text("Accept"), button:has-text("I Accept"), button:has-text("Got It")')
                if await accept_btn.is_visible(timeout=3000):
                    await accept_btn.click()
                    print(" ✓ Cookie-баннер закрыт")
                    await asyncio.sleep(1)
            except:
                pass

            while click_num < max_limit and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
                # Парсим текущие новости
                news_items = await page.locator('div[class*="flex gap-4"]').all()  # Адаптированный селектор
                page_news = []
                for idx, item in enumerate(news_items, 1):
                    try:
                        # === ЗАГОЛОВОК И ССЫЛКА ===
                        title_elem = item.locator('a[class*="content-card-title"]')
                        title = await title_elem.inner_text(timeout=1000) if await title_elem.count() > 0 else None
                        href = await title_elem.get_attribute('href', timeout=1000) if title else None

                        if not title or not href:
                            continue
                        title = title.strip()
                        if len(title) < 10:
                            continue
                        if not href.startswith('http'):
                            href = f"https://www.coindesk.com{href}"

                        # === КАТЕГОРИЯ ===
                        cat_elem = item.locator('a[class*="font-title"]')
                        category = await cat_elem.inner_text(timeout=1000) if await cat_elem.count() > 0 else "News"

                        # === ОПИСАНИЕ ===
                        desc_elem = item.locator('p[class*="font-body"]')
                        description = await desc_elem.inner_text(timeout=1000) if await desc_elem.count() > 0 else ""

                        # === ДАТА ===
                        date_elem = item.locator('span[class*="font-metadata"]')
                        published_at = await date_elem.inner_text(timeout=1000) if await date_elem.count() > 0 else ""

                        # Конвертируем в datetime (если нужно)
                        dt = datetime.datetime.now()  # По умолчанию

                        page_news.append({
                            'datetime': dt,
                            'title': title,
                            'source': 'CoinDesk',
                            'category': category.strip(),
                            'description': description.strip(),
                            'link': href,
                            'published_at': published_at.strip()
                        })
                    except Exception as e:
                        print(f" ⚠️ Ошибка парсинга элемента {idx}: {e}")
                        continue

                # === СРАЗУ СОХРАНЯЕМ В БД ===
                if page_news:
                    df_page = pd.DataFrame(page_news)
                    # Фильтруем новые
                    df_new = df_page[~df_page['link'].isin(existing_links)]
                    if not df_new.empty:
                        try:
                            df_new.to_sql(
                                name=table_name,
                                con=engine,
                                if_exists='append',
                                index=False,
                                chunksize=100,
                                method='multi'
                            )
                            existing_links.update(df_new['link'].tolist())
                            total_added += len(df_new)
                            print(f" ✅ Добавлено в БД: {len(df_new)} из {len(page_news)} (новые) на клике {click_num}")
                        except Exception as e:
                            print(f" ❌ Ошибка записи в БД: {e}")
                    else:
                        print(f" ℹ️ Все {len(page_news)} новостей уже есть в БД на клике {click_num}")
                    total_parsed += len(page_news)
                    consecutive_failures = 0

                # Кликаем на "More Stories"
                try:
                    load_more_btn = page.locator('button:has-text("More Stories")')
                    if not await load_more_btn.is_visible(timeout=5000):
                        print(" ℹ️ Кнопка 'More Stories' больше не видна. Остановка.")
                        break
                    await load_more_btn.click()
                    await page.wait_for_load_state('networkidle')
                    await asyncio.sleep(3)  # Дольше пауза для загрузки
                    click_num += 1
                    print(f" ✓ Клик на 'More Stories' {click_num}/{max_limit}")
                except Exception as e:
                    consecutive_failures += 1
                    print(f" ⚠️ Ошибка клика (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}")
                    await asyncio.sleep(2)
                    continue

        except Exception as e:
            print(f" ❌ Критическая ошибка в цикле: {e}")

        await browser.close()

        print(f"\n{'=' * 60}")
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f"⚠️ Остановлено: {MAX_CONSECUTIVE_FAILURES} последовательных неудач")
        elif click_num >= max_limit:
            print(f"ℹ️ Достигнут лимит кликов: {max_limit}")
        else:
            print(f"✅ Все новости загружены")
        print(f"📄 Обработано кликов: {click_num}")
        print(f"📊 Спарсено новостей: {total_parsed}")
        print(f"💾 Добавлено в БД: {total_added}")
        # Показываем последний ID
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
                max_id = result.scalar()
                print(f"🔢 Последний ID в таблице: {max_id}")
        except:
            pass
        return total_added

# ----------------------------------------------------------------------
# Функция для создания таблицы с автоинкрементным ID
# ----------------------------------------------------------------------
def ensure_table_exists(table_name):
    """
    Проверяет существование таблицы и создаёт её при необходимости
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{args.database}' AND table_name = '{table_name}'
        """))
        table_exists = result.scalar() > 0

    if not table_exists:
        create_query = text(f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            datetime DATETIME,
            title TEXT,
            source VARCHAR(255),
            category VARCHAR(255),
            description TEXT,
            link VARCHAR(500),
            published_at VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_link (link)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        with engine.connect() as conn:
            conn.execute(create_query)
            conn.commit()
        print(f"✅ Таблица '{table_name}' создана с автоинкрементом")
    else:
        # Проверяем наличие уникального индекса
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = '{args.database}'
                AND table_name = '{table_name}'
                AND column_name = 'link'
                AND non_unique = 0
            """))
            has_unique = result.scalar() > 0
        if not has_unique:
            print(f"⚠️ Внимание: в таблице '{table_name}' нет уникального индекса на поле link")
            print("Рекомендуется добавить уникальный индекс командой:")
            print(f"ALTER TABLE {table_name} MODIFY link VARCHAR(500), ADD UNIQUE INDEX unique_link (link);")

# ----------------------------------------------------------------------
# Основная логика
# ----------------------------------------------------------------------
def main():
    print(f"🚀 Загрузчик новостей CoinDesk.com (крипто)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Целевая таблица: {args.table_name}")
    if args.max_clicks:
        print(f"📄 Лимит кликов: {args.max_clicks}")
    else:
        print(f"📄 Лимит кликов: все доступные (макс. {MAX_CLICKS})")
    print("=" * 60)
    # Запускаем парсинг с бесконечным скроллом
    total_added = asyncio.run(
        parse_and_save_incrementally(args.table_name, max_clicks=args.max_clicks)
    )
    print("=" * 60)
    print("🏁 Загрузка завершена")
    if total_added == 0:
        print("ℹ️ Все новости уже были в БД")
    else:
        print(f"✨ Успешно добавлено {total_added} новых записей")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)