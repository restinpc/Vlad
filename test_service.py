#!/usr/bin/env python3
"""
Универсальный тестер для микросервисов brain-weights.
Запуск: python test_service.py --port 8893
"""

import argparse
import random
import requests
from datetime import datetime, timedelta
import sys
from typing import Optional, Dict, Any

BASE_URL = "http://localhost"


def random_date(start_year=2020, end_year=2026) -> str:
    """Генерирует случайную дату в формате YYYY-MM-DD."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def test_endpoint(url: str, expected_status=200) -> Optional[Dict[str, Any]]:
    """Выполняет GET-запрос и возвращает JSON при успехе, иначе None."""
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != expected_status:
            print(f"❌ {url} -> статус {resp.status_code}, ожидался {expected_status}")
            return None
        return resp.json()
    except Exception as e:
        print(f"❌ {url} -> исключение: {e}")
        return None


def test_metadata(port: int) -> bool:
    """Тест корневого эндпоинта."""
    url = f"{BASE_URL}:{port}/"
    print(f"\n🔍 Тестируем метаданные: {url}")
    data = test_endpoint(url)
    if not data:
        return False

    # Проверяем обязательные поля
    required_fields = ["status", "name", "metadata"]
    for field in required_fields:
        if field not in data:
            print(f"❌ Отсутствует поле '{field}'")
            return False

    if data["status"] != "ok":
        print(f"❌ status = {data['status']}, ожидался 'ok'")
        return False

    meta = data.get("metadata", {})
    # Для ECB сервиса могут быть поля ecb_currencies, для investing — ctx_index_rows
    if "ecb_currencies" in meta:
        print(f"   ECB валют: {meta['ecb_currencies']}")
    if "ctx_index_rows" in meta:
        print(f"   Контекстов: {meta['ctx_index_rows']}")
    if "weight_codes" in meta:
        print(f"   Весов: {meta['weight_codes']}")

    print("✅ Метаданные OK")
    return True


def test_weights(port: int) -> bool:
    """Тест эндпоинта /weights."""
    url = f"{BASE_URL}:{port}/weights"
    print(f"\n🔍 Тестируем список весов: {url}")
    data = test_endpoint(url)
    if not data:
        return False

    if "weights" not in data:
        print("❌ Ответ не содержит 'weights'")
        return False

    print("✅ Список весов OK")
    return True


def test_values(port: int, pair: int, day: int, date_str: str,
                type_val: int = 0, var_val: int = 0) -> bool:
    """Тест эндпоинта /values с заданными параметрами."""
    url = (f"{BASE_URL}:{port}/values?"
           f"pair={pair}&day={day}&date={date_str}&type={type_val}&var={var_val}")
    print(f"🔍 Тест values: {url}")
    data = test_endpoint(url)
    if data is None:
        return False

    # Успешный ответ — либо пустой словарь, либо словарь с весами
    if isinstance(data, dict):
        if "error" in data:
            print(f"⚠️ Сервер вернул ошибку: {data['error']}")
            return False
        # Если не ошибка, считаем успехом (пустой ответ допустим)
        if data:
            print(f"   Получено {len(data)} записей, пример: {list(data.items())[:2]}")
        else:
            print("   Ответ пуст (нет данных для этой даты)")
        return True
    else:
        print(f"❌ Ответ не является словарём: {data}")
        return False


def run_tests(port: int, num_random_tests: int = 15):
    """Запускает все тесты для указанного порта."""
    print(f"\n{'='*50}")
    print(f" ТЕСТИРОВАНИЕ СЕРВИСА НА ПОРТУ {port}")
    print(f"{'='*50}")

    # 1. Метаданные
    if not test_metadata(port):
        print("❌ Критическая ошибка: метаданные не получены, дальнейшие тесты прерваны.")
        return False

    # 2. Список весов
    if not test_weights(port):
        print("⚠️ Продолжаем тестирование values, несмотря на ошибку weights.")

    # 3. Тесты values с фиксированными датами
    print("\n🔍 Тесты /values с фиксированными датами")
    fixed_tests = [
        {"pair": 1, "day": 1, "date_str": "2024-06-15", "type_val": 0, "var_val": 0},
        {"pair": 1, "day": 1, "date_str": "2025-01-15", "type_val": 1, "var_val": 0},
        {"pair": 3, "day": 1, "date_str": "2024-06-15", "type_val": 2, "var_val": 0},
        {"pair": 4, "day": 0, "date_str": "2025-01-15 00:00:00", "type_val": 0, "var_val": 4},
    ]
    for params in fixed_tests:
        ok = test_values(port, **params)
        if not ok:
            print(f"   ❌ Провален тест с параметрами {params}")

    # 4. Случайные тесты
    print(f"\n🔍 Случайные тесты /values (количество: {num_random_tests})")
    for i in range(num_random_tests):
        pair = random.choice([1, 3, 4])
        day = random.choice([0, 1])
        date = random_date()
        type_val = random.randint(0, 2)
        var_val = random.randint(0, 4)
        ok = test_values(port, pair, day, date, type_val, var_val)
        if not ok:
            print(f"   ❌ Провален случайный тест #{i+1}")

    print(f"\n{'='*50}")
    print(f" ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ДЛЯ ПОРТА {port}")
    print(f"{'='*50}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Тестирование brain-микросервисов")
    parser.add_argument("--port", type=int, required=True, help="Порт сервиса (например, 8892 или 8893)")
    parser.add_argument("--num-random", type=int, default=5, help="Количество случайных тестов")
    args = parser.parse_args()

    run_tests(args.port, args.num_random)


if __name__ == "__main__":
    main()