#!/usr/bin/env python3
"""
Универсальная проверка look‑ahead bias для всех микросервисов в папке Brain-Services.
Выполняет два теста:
1. Будущие события: событие через 1 час после target_date – не должно использоваться.
2. Превышение сдвига: историческое событие за 5 дней до target_date + наблюдение с shift=6 дней
   даёт дату в будущем – такое наблюдение не должно давать вклада.
"""

import importlib.util
import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import traceback

def import_module(service_path: Path):
    """Импортирует модуль server.py из указанной папки."""
    file_path = service_path / "server.py"
    if not file_path.exists():
        return None, f"Файл {file_path} не найден"

    module_name = f"service_{service_path.name}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        return None, f"Ошибка импорта: {e}\n{traceback.format_exc()}"

    return module, None

async def call_calculate(module, kwargs):
    """Безопасно вызывает асинхронную функцию calculate_pure_memory с разными сигнатурами."""
    if hasattr(module, 'calculate_pure_memory'):
        func = module.calculate_pure_memory
        # Пробуем разные варианты аргументов
        signatures = [
            {'pair': 1, 'day': 1, 'date_str': kwargs['date'], 'calc_type': 0, 'calc_var': 0},
            {'pair': 1, 'day': 1, 'date': kwargs['date'], 'type_': 0, 'var': 0},
            {'pair': 1, 'day': 1, 'date_str': kwargs['date']},
            {'pair': 1, 'day': 1, 'date': kwargs['date']},
        ]
        for sig in signatures:
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(**sig)
                else:
                    return func(**sig)
            except TypeError:
                continue
        raise TypeError("Не найдена подходящая сигнатура для calculate_pure_memory")
    else:
        raise AttributeError("Модуль не содержит calculate_pure_memory")

def get_structures(module):
    """Определяет, какие глобальные структуры есть в модуле, и возвращает словарь с их именами."""
    calendar_candidates = ['GLOBAL_CALENDAR', 'GLOBAL_CAL_BY_DT',
                           'GLOBAL_MKT_OBS_DTS', 'GLOBAL_ECB_OBS_DATES']
    history_candidates = ['GLOBAL_HISTORY', 'GLOBAL_CAL_CTX_HIST',
                          'GLOBAL_MKT_CTX_HIST', 'GLOBAL_ECB_CTX_HIST']
    calendar_attr = None
    history_attr = None
    for attr in calendar_candidates:
        if hasattr(module, attr):
            calendar_attr = attr
            break
    for attr in history_candidates:
        if hasattr(module, attr):
            history_attr = attr
            break
    return calendar_attr, history_attr

async def test_future_event(module):
    """Тест 1: будущее событие (дата > target_date) не должно использоваться."""
    calendar_attr, history_attr = get_structures(module)
    if not calendar_attr:
        return False, "Нет структуры календаря"

    orig_calendar = getattr(module, calendar_attr, {})
    orig_history = getattr(module, history_attr, {}) if history_attr else {}
    orig_other = {}

    target_date = datetime(2024, 6, 15, 12, 0)
    target_str = "2024-06-15 12:00:00"
    future_date = target_date + timedelta(hours=1)
    test_id = 99999

    mock_calendar = {}
    mock_history = {}

    if calendar_attr == 'GLOBAL_CALENDAR':
        mock_calendar[future_date] = [{"EventId": test_id, "Importance": "high", "event_date": future_date}]
        mock_history[test_id] = [future_date]
    elif calendar_attr == 'GLOBAL_CAL_BY_DT':
        mock_calendar[future_date] = [(test_id, "dummy", "ctx")]
        if history_attr:
            mock_history[(test_id, "dummy", "ctx")] = [future_date]
    elif calendar_attr == 'GLOBAL_MKT_OBS_DTS':
        mock_calendar[future_date] = {"TESTINSTR"}
        if hasattr(module, 'GLOBAL_MKT_CONTEXT'):
            orig_other['GLOBAL_MKT_CONTEXT'] = getattr(module, 'GLOBAL_MKT_CONTEXT', {})
            setattr(module, 'GLOBAL_MKT_CONTEXT', {("TESTINSTR", future_date): ("UP", "ABOVE", "UP")})
    elif calendar_attr == 'GLOBAL_ECB_OBS_DATES':
        mock_calendar[future_date] = {"TESTCCY"}
        if hasattr(module, 'GLOBAL_ECB_CONTEXT'):
            orig_other['GLOBAL_ECB_CONTEXT'] = getattr(module, 'GLOBAL_ECB_CONTEXT', {})
            setattr(module, 'GLOBAL_ECB_CONTEXT', {("TESTCCY", future_date): ("UP", "ABOVE", "UP")})
    else:
        return False, f"Неизвестный тип календаря {calendar_attr}"

    setattr(module, calendar_attr, mock_calendar)
    if history_attr:
        setattr(module, history_attr, mock_history)

    try:
        result = await call_calculate(module, {'date': target_str})
    except Exception as e:
        # Восстановить оригиналы и вернуть ошибку
        setattr(module, calendar_attr, orig_calendar)
        if history_attr:
            setattr(module, history_attr, orig_history)
        for k, v in orig_other.items():
            setattr(module, k, v)
        return False, f"Ошибка вызова: {e}"

    setattr(module, calendar_attr, orig_calendar)
    if history_attr:
        setattr(module, history_attr, orig_history)
    for k, v in orig_other.items():
        setattr(module, k, v)

    result_str = str(result)
    if str(test_id) in result_str:
        return False, f"Найден признак использования будущего события: {result}"

    if calendar_attr in ('GLOBAL_MKT_OBS_DTS', 'GLOBAL_ECB_OBS_DATES'):
        if result and any(abs(v) > 1e-9 for v in result.values()):
            return False, f"Результат не пуст (есть ненулевые веса): {result}"

    return True, "OK"

async def test_shift_exceeds_target(module):
    """
    Тест 2: событие произошло в прошлом, но из-за сдвига d+shift > target_date.
    Ожидается, что такое наблюдение не даст вклада.
    """
    calendar_attr, history_attr = get_structures(module)
    if not calendar_attr:
        return False, "Нет структуры календаря"

    orig_calendar = getattr(module, calendar_attr, {})
    orig_history = getattr(module, history_attr, {}) if history_attr else {}
    orig_other = {}

    target_date = datetime(2024, 6, 15, 12, 0)
    target_str = "2024-06-15 12:00:00"

    # Историческое событие (произошло за 5 дней до target)
    past_date = target_date - timedelta(days=5)
    # Наблюдаемое событие в окне (за 2 дня до target) -> shift = 2
    # Но мы хотим проверить случай, когда shift большой, например 6 дней.
    # Для этого создадим наблюдение с event_date = target_date - timedelta(days=6) ? Нет, тогда shift = 6.
    # Но тогда сдвиг будет 6 дней, и past_date + 6 = target_date + 1 день (будущее).
    # Создадим наблюдение с датой на 6 дней раньше target, чтобы shift = 6.
    observation_date = target_date - timedelta(days=6)
    shift = (target_date - observation_date).days  # = 6
    test_id = 99998  # другой id, чтобы не пересекаться с первым тестом

    # Проверим, что past_date + shift > target_date:
    # past_date (target-5) + 6 = target+1 > target, условие выполняется.

    # Заполняем мок-структуры
    mock_calendar = {}
    mock_history = {}

    if calendar_attr == 'GLOBAL_CALENDAR':
        mock_calendar[observation_date] = [{"EventId": test_id, "Importance": "high", "event_date": observation_date}]
        mock_history[test_id] = [past_date]   # историческое событие
    elif calendar_attr == 'GLOBAL_CAL_BY_DT':
        # Для GLOBAL_CAL_BY_DT ключ – кортеж (event_id, ...). Нам нужно, чтобы контекст совпадал.
        # Упростим: пусть контекст будет (test_id, "dummy", "ctx")
        ctx_key = (test_id, "dummy", "ctx")
        mock_calendar[observation_date] = [ctx_key]
        if history_attr:
            mock_history[ctx_key] = [past_date]
    elif calendar_attr == 'GLOBAL_MKT_OBS_DTS':
        # Для market: наблюдение по инструменту TESTINSTR
        mock_calendar[observation_date] = {"TESTINSTR"}
        # Нужен контекст для этого инструмента и даты
        if hasattr(module, 'GLOBAL_MKT_CONTEXT'):
            orig_other['GLOBAL_MKT_CONTEXT'] = getattr(module, 'GLOBAL_MKT_CONTEXT', {})
            setattr(module, 'GLOBAL_MKT_CONTEXT', {("TESTINSTR", observation_date): ("UP", "ABOVE", "UP")})
        # История: в GLOBAL_MKT_CTX_HIST ключ – (instr, rcd, td, md)
        if history_attr:
            ctx_key = ("TESTINSTR", "UP", "ABOVE", "UP")
            mock_history[ctx_key] = [past_date]  # историческая дата с таким же контекстом
    elif calendar_attr == 'GLOBAL_ECB_OBS_DATES':
        mock_calendar[observation_date] = {"TESTCCY"}
        if hasattr(module, 'GLOBAL_ECB_CONTEXT'):
            orig_other['GLOBAL_ECB_CONTEXT'] = getattr(module, 'GLOBAL_ECB_CONTEXT', {})
            setattr(module, 'GLOBAL_ECB_CONTEXT', {("TESTCCY", observation_date): ("UP", "ABOVE", "UP")})
        if history_attr:
            ctx_key = ("TESTCCY", "UP", "ABOVE", "UP")
            mock_history[ctx_key] = [past_date]
    else:
        return False, f"Неизвестный тип календаря {calendar_attr}"

    setattr(module, calendar_attr, mock_calendar)
    if history_attr:
        setattr(module, history_attr, mock_history)

    try:
        result = await call_calculate(module, {'date': target_str})
    except Exception as e:
        setattr(module, calendar_attr, orig_calendar)
        if history_attr:
            setattr(module, history_attr, orig_history)
        for k, v in orig_other.items():
            setattr(module, k, v)
        return False, f"Ошибка вызова: {e}"

    setattr(module, calendar_attr, orig_calendar)
    if history_attr:
        setattr(module, history_attr, orig_history)
    for k, v in orig_other.items():
        setattr(module, k, v)

    # Проверяем, что результат не содержит test_id и не пуст для market/ecb
    result_str = str(result)
    if str(test_id) in result_str:
        return False, f"Найден вклад от наблюдения с превышающим сдвигом: {result}"

    if calendar_attr in ('GLOBAL_MKT_OBS_DTS', 'GLOBAL_ECB_OBS_DATES'):
        # Если есть другие данные, может быть ненулевой результат. Но в нашем моке только одно наблюдение,
        # которое должно быть исключено, поэтому результат должен быть пустым.
        if result and any(abs(v) > 1e-9 for v in result.values()):
            return False, f"Результат не пуст (веса появились из-за сдвига в будущее): {result}"

    return True, "OK"

async def test_service(module):
    """Запускает оба теста и возвращает общий результат."""
    # Тест 1
    ok1, msg1 = await test_future_event(module)
    if not ok1:
        return False, f"Тест будущих событий провален: {msg1}"
    # Тест 2
    ok2, msg2 = await test_shift_exceeds_target(module)
    if not ok2:
        return False, f"Тест превышения сдвига провален: {msg2}"
    return True, "OK (оба теста пройдены)"

async def main_async():
    base_path = Path(__file__).parent
    service_dirs = [d for d in base_path.iterdir() if d.is_dir() and d.name.isdigit()]
    service_dirs.sort(key=lambda x: int(x.name))

    results = {}
    for service_dir in service_dirs:
        print(f"\n--- Проверка сервиса {service_dir.name} ---")
        module, err = import_module(service_dir)
        if err:
            print(f"  ❌ Ошибка импорта: {err}")
            results[service_dir.name] = ("ERROR", err)
            continue

        ok, msg = await test_service(module)
        if ok:
            print(f"  ✅ {msg}")
            results[service_dir.name] = ("PASS", msg)
        else:
            print(f"  ❌ {msg}")
            results[service_dir.name] = ("FAIL", msg)

    print("\n" + "="*60)
    print("ИТОГИ ПРОВЕРКИ")
    for name, (status, msg) in sorted(results.items()):
        mark = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        print(f"{mark} Сервис {name}: {status} — {msg}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()