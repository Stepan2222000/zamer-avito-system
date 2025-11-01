#!/usr/bin/env python3
"""
init_db.py - Инициализация базы данных zamer_avito_system

Назначение:
    Создает таблицы и индексы в PostgreSQL для распределенной системы парсинга Avito.
    Выполняет sql/init.sql с CREATE TABLE IF NOT EXISTS (можно запускать многократно).

Использование:
    python cli/init_db.py

Что делает:
    1. Подключается к PostgreSQL (81.30.105.134:5413)
    2. Читает sql/init.sql
    3. Выполняет CREATE TABLE IF NOT EXISTS для 4 таблиц
    4. Создает 6 индексов для оптимизации
    5. Выводит результат (таблицы созданы/уже существуют)

Результат:
    - База готова к работе
    - Можно запускать воркеры
    - Можно загружать задачи через upload_tasks.py

Дата: 01.11.2025
"""

import sys
import psycopg2
from pathlib import Path


# ============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ К БД
# ============================================================================
# ВАЖНО: Хардкод параметров в соответствии с принципом KISS
# Никаких .env файлов, никаких переменных окружения
# ============================================================================

DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5413,
    'database': 'zamer_avito_system',
    'user': 'admin',
    'password': 'Password123'
}


def get_sql_file_path() -> Path:
    """
    Находит путь к sql/init.sql относительно текущего скрипта.

    Структура проекта:
        zamer_avito_system/
        ├── cli/
        │   └── init_db.py  ← мы здесь
        └── sql/
            └── init.sql     ← нужно найти

    Возвращает:
        Path: Абсолютный путь к sql/init.sql
    """
    # cli/init_db.py -> cli/ -> zamer_avito_system/
    project_root = Path(__file__).parent.parent
    sql_file = project_root / 'sql' / 'init.sql'

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL файл не найден: {sql_file}")

    return sql_file


def init_database():
    """
    Основная функция инициализации базы данных.

    Что делает:
        1. Подключается к PostgreSQL
        2. Читает sql/init.sql
        3. Выполняет CREATE TABLE IF NOT EXISTS
        4. Создает индексы
        5. Логирует результат

    Возвращает:
        bool: True если успешно, False при ошибке
    """

    print("=" * 80)
    print("ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ zamer_avito_system")
    print("=" * 80)
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"User: {DB_CONFIG['user']}")
    print("-" * 80)

    conn = None

    try:
        # 1. Получаем путь к SQL файлу
        sql_file = get_sql_file_path()
        print(f"SQL файл: {sql_file}")
        print()

        # 2. Читаем SQL скрипт
        print("Чтение sql/init.sql...")
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        print(f"  ✓ Прочитано {len(sql_script)} символов")
        print()

        # 3. Подключаемся к PostgreSQL
        print("Подключение к PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Для DDL операций (CREATE TABLE)
        print("  ✓ Подключение установлено")
        print()

        # 4. Выполняем SQL скрипт
        print("Выполнение SQL скрипта...")
        with conn.cursor() as cursor:
            cursor.execute(sql_script)

            # Получаем все NOTICE сообщения (из DO $$ блока в конце init.sql)
            if cursor.connection.notices:
                for notice in cursor.connection.notices:
                    print(notice.strip())

        print()
        print("=" * 80)
        print("✓ БАЗА ДАННЫХ ИНИЦИАЛИЗИРОВАНА УСПЕШНО")
        print("=" * 80)
        print()
        print("Следующие шаги:")
        print("  1. Загрузить задачи:  python cli/upload_tasks.py")
        print("  2. Загрузить прокси:  python cli/upload_proxies.py")
        print("  3. Запустить воркеры: cd worker && docker compose up -d")
        print("  4. Мониторинг:        python cli/status.py")
        print()

        return True

    except FileNotFoundError as e:
        print(f"✗ ОШИБКА: {e}", file=sys.stderr)
        print("\nУбедитесь, что файл sql/init.sql существует.", file=sys.stderr)
        return False

    except psycopg2.OperationalError as e:
        print(f"✗ ОШИБКА ПОДКЛЮЧЕНИЯ К БД:", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        print("\nПроверьте:", file=sys.stderr)
        print(f"  - Доступен ли хост {DB_CONFIG['host']}:{DB_CONFIG['port']}", file=sys.stderr)
        print(f"  - Правильные ли credentials (user={DB_CONFIG['user']})", file=sys.stderr)
        print(f"  - Существует ли база данных '{DB_CONFIG['database']}'", file=sys.stderr)
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
            print("Подключение к БД закрыто.")


if __name__ == '__main__':
    success = init_database()
    sys.exit(0 if success else 1)
