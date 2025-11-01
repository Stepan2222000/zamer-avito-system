#!/usr/bin/env python3
"""
upload_tasks.py - Загрузка задач в БД из data/items.txt

Назначение:
    Читает список item_id из data/items.txt и загружает их в таблицу tasks.
    Интерактивно запрашивает режим: append (добавить) или overwrite (перезаписать).

Использование:
    python cli/upload_tasks.py

Режимы:
    1) append - Добавляет новые задачи, существующие item_id пропускаются (ON CONFLICT DO NOTHING)
    2) overwrite - Удаляет все задачи и загружает заново (DELETE + INSERT)

Формат data/items.txt:
    Один item_id на строку (целое число):
    3895922522
    3908287408
    ...

Дата: 01.11.2025
"""

import sys
import psycopg2
from pathlib import Path


# ============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ К БД
# ============================================================================
# ВАЖНО: Хардкод параметров в соответствии с принципом KISS
# ============================================================================

DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5413,
    'database': 'zamer_avito_system',
    'user': 'admin',
    'password': 'Password123'
}


def get_data_file_path() -> Path:
    """
    Находит путь к data/items.txt относительно текущего скрипта.

    Структура проекта:
        zamer_avito_system/
        ├── cli/
        │   └── upload_tasks.py  ← мы здесь
        └── data/
            └── items.txt        ← нужно найти

    Возвращает:
        Path: Абсолютный путь к data/items.txt
    """
    # cli/upload_tasks.py -> cli/ -> zamer_avito_system/
    project_root = Path(__file__).parent.parent
    data_file = project_root / 'data' / 'items.txt'

    if not data_file.exists():
        raise FileNotFoundError(f"Файл с задачами не найден: {data_file}")

    return data_file


def read_items(file_path: Path) -> list[int]:
    """
    Читает и валидирует item_id из файла.

    Что делает:
        1. Читает файл построчно
        2. Пропускает пустые строки
        3. Пытается преобразовать в int
        4. Если не число - выводит предупреждение и пропускает

    Аргументы:
        file_path: Путь к файлу с item_id

    Возвращает:
        list[int]: Список валидных item_id
    """
    items = []
    invalid_count = 0

    print(f"Чтение {file_path}...")

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()

            # Пропускаем пустые строки
            if not line:
                continue

            # Пытаемся преобразовать в int
            try:
                item_id = int(line)
                items.append(item_id)
            except ValueError:
                print(f"  ⚠ Строка {line_num}: '{line}' не является числом, пропускаем")
                invalid_count += 1

    print(f"  ✓ Прочитано валидных item_id: {len(items)}")
    if invalid_count > 0:
        print(f"  ⚠ Пропущено невалидных строк: {invalid_count}")
    print()

    return items


def ask_mode() -> str:
    """
    Интерактивно запрашивает режим загрузки у пользователя.

    Что делает:
        1. Выводит меню с вариантами
        2. Читает ввод пользователя
        3. Валидирует (должно быть 1 или 2)
        4. Если невалидный ввод - переспрашивает

    Возвращает:
        str: 'append' или 'overwrite'
    """
    print("=" * 80)
    print("РЕЖИМ ЗАГРУЗКИ ЗАДАЧ")
    print("=" * 80)
    print("Выберите режим загрузки:")
    print("  1) append - добавить новые задачи (существующие не трогать)")
    print("  2) overwrite - удалить все задачи и загрузить заново")
    print()

    while True:
        choice = input("Введите номер режима (1 или 2): ").strip()

        if choice == '1':
            return 'append'
        elif choice == '2':
            return 'overwrite'
        else:
            print("  ✗ Неверный ввод. Введите 1 или 2.")
            print()


def upload_append(conn, items: list[int]) -> tuple[int, int]:
    """
    Загружает задачи в режиме append (ON CONFLICT DO NOTHING).

    Что делает:
        1. INSERT INTO tasks (item_id) VALUES ... ON CONFLICT DO NOTHING
        2. Считает добавленные и пропущенные задачи

    Аргументы:
        conn: Подключение к PostgreSQL
        items: Список item_id для загрузки

    Возвращает:
        tuple[int, int]: (добавлено, пропущено)
    """
    cursor = conn.cursor()

    print("Загрузка задач в режиме append...")

    # Получаем количество задач до вставки
    cursor.execute("SELECT COUNT(*) FROM tasks")
    count_before = cursor.fetchone()[0]

    # Вставляем задачи (ON CONFLICT DO NOTHING для пропуска дубликатов)
    for item_id in items:
        cursor.execute(
            "INSERT INTO tasks (item_id) VALUES (%s) ON CONFLICT (item_id) DO NOTHING",
            (item_id,)
        )

    conn.commit()

    # Получаем количество задач после вставки
    cursor.execute("SELECT COUNT(*) FROM tasks")
    count_after = cursor.fetchone()[0]

    added = count_after - count_before
    skipped = len(items) - added

    cursor.close()

    return added, skipped


def upload_overwrite(conn, items: list[int]) -> int:
    """
    Загружает задачи в режиме overwrite (DELETE + INSERT).

    Что делает:
        1. DELETE FROM tasks - удаляет все задачи
        2. INSERT INTO tasks (item_id) VALUES ... - добавляет новые

    Аргументы:
        conn: Подключение к PostgreSQL
        items: Список item_id для загрузки

    Возвращает:
        int: Количество загруженных задач
    """
    cursor = conn.cursor()

    print("Загрузка задач в режиме overwrite...")

    # 1. Удаляем все задачи
    cursor.execute("DELETE FROM tasks")
    deleted_count = cursor.rowcount
    print(f"  ✓ Удалено старых задач: {deleted_count}")

    # 2. Вставляем новые задачи
    for item_id in items:
        cursor.execute(
            "INSERT INTO tasks (item_id) VALUES (%s)",
            (item_id,)
        )

    conn.commit()

    cursor.close()

    return len(items)


def main():
    """
    Основная функция загрузки задач.

    Что ��елает:
        1. Читает data/items.txt
        2. Спрашивает режим (append/overwrite)
        3. Подключается к PostgreSQL
        4. Загружает задачи
        5. Выводит статистику

    Возвращает:
        bool: True если успешно, False при ошибке
    """

    print("=" * 80)
    print("ЗАГРУЗКА ЗАДАЧ В БАЗУ ДАННЫХ zamer_avito_system")
    print("=" * 80)
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print("-" * 80)
    print()

    conn = None

    try:
        # 1. Читаем файл с задачами
        data_file = get_data_file_path()
        items = read_items(data_file)

        if len(items) == 0:
            print("✗ ОШИБКА: Нет валидных item_id для загрузки", file=sys.stderr)
            return False

        # 2. Спрашиваем режим
        mode = ask_mode()
        print()

        # 3. Подключаемся к PostgreSQL
        print("Подключение к PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("  ✓ Подключение установлено")
        print()

        # 4. Загружаем задачи
        if mode == 'append':
            added, skipped = upload_append(conn, items)

            # Получаем общее количество задач в БД
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tasks")
            total = cursor.fetchone()[0]
            cursor.close()

            print()
            print("=" * 80)
            print("✓ ЗАДАЧИ ЗАГРУЖЕНЫ УСПЕШНО (РЕЖИМ: APPEND)")
            print("=" * 80)
            print(f"  ✓ Загружено задач: {added}")
            print(f"  ⊘ Пропущено (дубликаты): {skipped}")
            print(f"  ✓ Всего задач в БД: {total}")
            print("=" * 80)

        else:  # overwrite
            loaded = upload_overwrite(conn, items)

            print()
            print("=" * 80)
            print("✓ ЗАДАЧИ ЗАГРУЖЕНЫ УСПЕШНО (РЕЖИМ: OVERWRITE)")
            print("=" * 80)
            print(f"  ✓ Загружено задач: {loaded}")
            print(f"  ✓ Всего задач в БД: {loaded}")
            print("=" * 80)

        print()
        return True

    except FileNotFoundError as e:
        print(f"✗ ОШИБКА: {e}", file=sys.stderr)
        print("\nУбедитесь, что файл data/items.txt существует.", file=sys.stderr)
        return False

    except psycopg2.OperationalError as e:
        print(f"✗ ОШИБКА ПОДКЛЮЧЕНИЯ К БД:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        print("\nПроверьте:", file=sys.stderr)
        print(f"  - Доступен ли хост {DB_CONFIG['host']}:{DB_CONFIG['port']}", file=sys.stderr)
        print(f"  - Правильные ли credentials (user={DB_CONFIG['user']})", file=sys.stderr)
        return False

    except psycopg2.Error as e:
        print(f"✗ ОШИБКА ВЫПОЛНЕНИЯ SQL:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return False

    except Exception as e:
        print(f"✗ НЕОЖИДАННАЯ ОШИБКА:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        return False

    finally:
        # 5. Закрываем подключение
        if conn:
            conn.close()


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
