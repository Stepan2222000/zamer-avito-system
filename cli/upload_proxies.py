#!/usr/bin/env python3
"""
upload_proxies.py - Загрузка прокси в БД из data/proxies.txt

Назначение:
    Читает список прокси из data/proxies.txt и загружает их в таблицу proxies.
    Интерактивно запрашивает режим: append (добавить) или overwrite (перезаписать).

Использование:
    python cli/upload_proxies.py

Режимы:
    1) append - Добавляет новые прокси, существующие пропускаются (ON CONFLICT DO NOTHING)
    2) overwrite - Удаляет все прокси и загружает заново (DELETE + INSERT)

Формат data/proxies.txt:
    Один прокси на строку (host:port:user:pass):
    178.250.190.177:3000:q5Wuid:1j8A4VJOZr
    194.145.147.144:3000:q5Wuid:1j8A4VJOZr
    ...

    Комментарии (строки начинающиеся с #) и пустые строки пропускаются.

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
    Находит путь к data/proxies.txt относительно текущего скрипта.

    Структура проекта:
        zamer_avito_system/
        ├── cli/
        │   └── upload_proxies.py  ← мы здесь
        └── data/
            └── proxies.txt        ← нужно найти

    Возвращает:
        Path: Абсолютный путь к data/proxies.txt
    """
    # cli/upload_proxies.py -> cli/ -> zamer_avito_system/
    project_root = Path(__file__).parent.parent
    data_file = project_root / 'data' / 'proxies.txt'

    if not data_file.exists():
        raise FileNotFoundError(f"Файл с прокси не найден: {data_file}")

    return data_file


def validate_proxy_format(proxy: str) -> bool:
    """
    Валидирует формат прокси (host:port:user:pass).

    Что проверяет:
        - Должно быть ровно 4 части при разделении по ':'
        - host должен быть непустым
        - port должен быть числом

    Аргументы:
        proxy: Строка прокси для валидации

    Возвращает:
        bool: True если формат корректный, False иначе
    """
    parts = proxy.split(':')

    # Должно быть ровно 4 части
    if len(parts) != 4:
        return False

    host, port, user, password = parts

    # host должен быть непустым
    if not host.strip():
        return False

    # port должен быть числом
    try:
        port_num = int(port)
        if port_num <= 0 or port_num > 65535:
            return False
    except ValueError:
        return False

    # user и password могут быть любыми (даже пустыми)

    return True


def read_proxies(file_path: Path) -> list[str]:
    """
    Читает и валидирует прокси из файла.

    Что делает:
        1. Читает файл построчно
        2. Пропускает комментарии (# в начале) и пустые строки
        3. Валидирует формат host:port:user:pass
        4. Если невалидный формат - выводит предупреждение и пропускает

    Аргументы:
        file_path: Путь к файлу с прокси

    Возвращает:
        list[str]: Список валидных прокси
    """
    proxies = []
    invalid_count = 0
    comment_count = 0

    print(f"Чтение {file_path}...")

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            original_line = line
            line = line.strip()

            # Пропускаем пустые строки
            if not line:
                continue

            # Пропускаем комментарии
            if line.startswith('#'):
                comment_count += 1
                continue

            # Валидируем формат
            if validate_proxy_format(line):
                proxies.append(line)
            else:
                print(f"  ⚠ Строка {line_num}: '{line}' имеет неверный формат, пропускаем")
                print(f"     Ожидается формат: host:port:user:pass")
                invalid_count += 1

    print(f"  ✓ Прочитано валидных прокси: {len(proxies)}")
    if comment_count > 0:
        print(f"  ⊘ Пропущено комментариев: {comment_count}")
    if invalid_count > 0:
        print(f"  ⚠ Пропущено невалидных строк: {invalid_count}")
    print()

    return proxies


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
    print("РЕЖИМ ЗАГРУЗКИ ПРОКСИ")
    print("=" * 80)
    print("Выберите режим загрузки:")
    print("  1) append - добавить новые прокси (существующие не трогать)")
    print("  2) overwrite - удалить все прокси и загрузить заново")
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


def upload_append(conn, proxies: list[str]) -> tuple[int, int]:
    """
    Загружает прокси в режиме append (ON CONFLICT DO NOTHING).

    Что делает:
        1. INSERT INTO proxies (proxy) VALUES ... ON CONFLICT DO NOTHING
        2. Считает добавленные и пропущенные прокси

    Аргументы:
        conn: Подключение к PostgreSQL
        proxies: Список прокси для загрузки

    Возвращает:
        tuple[int, int]: (добавлено, пропущено)
    """
    cursor = conn.cursor()

    print("Загрузка прокси в режиме append...")

    # Получаем количество прокси до вставки
    cursor.execute("SELECT COUNT(*) FROM proxies")
    count_before = cursor.fetchone()[0]

    # Вставляем прокси (ON CONFLICT DO NOTHING для пропуска дубликатов)
    for proxy in proxies:
        cursor.execute(
            "INSERT INTO proxies (proxy) VALUES (%s) ON CONFLICT (proxy) DO NOTHING",
            (proxy,)
        )

    conn.commit()

    # Получаем количество прокси после вставки
    cursor.execute("SELECT COUNT(*) FROM proxies")
    count_after = cursor.fetchone()[0]

    added = count_after - count_before
    skipped = len(proxies) - added

    cursor.close()

    return added, skipped


def upload_overwrite(conn, proxies: list[str]) -> int:
    """
    Загружает прокси в режиме overwrite (DELETE + INSERT).

    Что делает:
        1. DELETE FROM proxies - удаляет все прокси
        2. INSERT INTO proxies (proxy) VALUES ... - добавляет новые

    Аргументы:
        conn: Подключение к PostgreSQL
        proxies: Список прокси для загрузки

    Возвращает:
        int: Количество загруженных прокси
    """
    cursor = conn.cursor()

    print("Загрузка прокси в режиме overwrite...")

    # 1. Удаляем все прокси
    cursor.execute("DELETE FROM proxies")
    deleted_count = cursor.rowcount
    print(f"  ✓ Удалено старых прокси: {deleted_count}")

    # 2. Вставляем новые прокси
    for proxy in proxies:
        cursor.execute(
            "INSERT INTO proxies (proxy) VALUES (%s)",
            (proxy,)
        )

    conn.commit()

    cursor.close()

    return len(proxies)


def main():
    """
    Основная функция загрузки прокси.

    Что делает:
        1. Читает data/proxies.txt
        2. Спрашивает режим (append/overwrite)
        3. Подключается к PostgreSQL
        4. Загружает прокси
        5. Выводит статистику

    Возвращает:
        bool: True если успешно, False при ошибке
    """

    print("=" * 80)
    print("ЗАГРУЗКА ПРОКСИ В БАЗУ ДАННЫХ zamer_avito_system")
    print("=" * 80)
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print("-" * 80)
    print()

    conn = None

    try:
        # 1. Читаем файл с прокси
        data_file = get_data_file_path()
        proxies = read_proxies(data_file)

        if len(proxies) == 0:
            print("✗ ОШИБКА: Нет валидных прокси для загрузки", file=sys.stderr)
            return False

        # 2. Спрашиваем режим
        mode = ask_mode()
        print()

        # 3. Подключаемся к PostgreSQL
        print("Подключение к PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("  ✓ Подключение установлено")
        print()

        # 4. Загружаем прокси
        if mode == 'append':
            added, skipped = upload_append(conn, proxies)

            # Получаем общее количество прокси в БД
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM proxies")
            total = cursor.fetchone()[0]
            cursor.close()

            print()
            print("=" * 80)
            print("✓ ПРОКСИ ЗАГРУЖЕНЫ УСПЕШНО (РЕЖИМ: APPEND)")
            print("=" * 80)
            print(f"  ✓ Загружено прокси: {added}")
            print(f"  ⊘ Пропущено (дубликаты): {skipped}")
            print(f"  ✓ Всего прокси в БД: {total}")
            print("=" * 80)

        else:  # overwrite
            loaded = upload_overwrite(conn, proxies)

            print()
            print("=" * 80)
            print("✓ ПРОКСИ ЗАГРУЖЕНЫ УСПЕШНО (РЕЖИМ: OVERWRITE)")
            print("=" * 80)
            print(f"  ✓ Загружено прокси: {loaded}")
            print(f"  ✓ Всего прокси в БД: {loaded}")
            print("=" * 80)

        print()
        return True

    except FileNotFoundError as e:
        print(f"✗ ОШИБКА: {e}", file=sys.stderr)
        print("\nУбедитесь, что файл data/proxies.txt существует.", file=sys.stderr)
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
