#!/usr/bin/env python3
"""
status.py - Мониторинг статуса распределенной системы zamer_avito_system

Назначение:
    Подключается к PostgreSQL и выводит статистику по 4 секциям:
    - TASKS (pending/processing/completed/failed)
    - PROXIES (available/locked/blocked)
    - WORKERS (active/stopped)
    - SYSTEM HEALTH (зависшие ресурсы, требующие внимания)

Использование:
    python cli/status.py

Дата: 01.11.2025
"""

import sys
import psycopg2
from datetime import datetime


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


def get_tasks_stats(cursor) -> dict[str, int]:
    """
    Получает статистику задач по статусам.

    SQL запрос:
        SELECT status, COUNT(*) FROM tasks GROUP BY status

    Аргументы:
        cursor: Курсор PostgreSQL

    Возвращает:
        dict: {status: count}, например {'pending': 100, 'processing': 15, ...}
    """
    cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
    stats = {row[0]: row[1] for row in cursor.fetchall()}

    # Добавляем статусы со значением 0, если они отсутствуют
    for status in ['pending', 'processing', 'completed', 'failed']:
        if status not in stats:
            stats[status] = 0

    return stats


def get_proxies_stats(cursor) -> dict[str, int]:
    """
    Получает статистику прокси по статусам.

    SQL запрос:
        SELECT status, COUNT(*) FROM proxies GROUP BY status

    Аргументы:
        cursor: Курсор PostgreSQL

    Возвращает:
        dict: {status: count}, например {'available': 35, 'locked': 4, 'blocked': 0}
    """
    cursor.execute("SELECT status, COUNT(*) FROM proxies GROUP BY status")
    stats = {row[0]: row[1] for row in cursor.fetchall()}

    # Добавляем статусы со значением 0, если они отсутствуют
    for status in ['available', 'locked', 'blocked']:
        if status not in stats:
            stats[status] = 0

    return stats


def get_workers_stats(cursor) -> dict[str, int]:
    """
    Получает статистику воркеров по статусам.

    SQL запрос:
        SELECT status, COUNT(*) FROM workers GROUP BY status

    Аргументы:
        cursor: Курсор PostgreSQL

    Возвращает:
        dict: {status: count}, например {'active': 15, 'stopped': 0}
    """
    cursor.execute("SELECT status, COUNT(*) FROM workers GROUP BY status")
    stats = {row[0]: row[1] for row in cursor.fetchall()}

    # Добавляем статусы со значением 0, если они отсутствуют
    for status in ['active', 'stopped']:
        if status not in stats:
            stats[status] = 0

    return stats


def get_system_health(cursor) -> dict[str, int]:
    """
    Проверяет здоровье системы: зависшие ресурсы.

    Что проверяет:
        1. Зависшие задачи: status='processing' AND last_attempt_at > 10 минут
        2. Зависшие прокси: status='locked' AND locked_at > 5 минут
        3. Мертвые воркеры: status='active' AND last_heartbeat > 4 минут

    Аргументы:
        cursor: Курсор PostgreSQL

    Возвращает:
        dict: {
            'stuck_tasks': count,
            'stuck_proxies': count,
            'dead_workers': count
        }
    """
    health = {}

    # 1. Зависшие задачи (processing > 10 минут)
    cursor.execute("""
        SELECT COUNT(*) FROM tasks
        WHERE status='processing'
        AND last_attempt_at < NOW() - INTERVAL '10 minutes'
    """)
    health['stuck_tasks'] = cursor.fetchone()[0]

    # 2. Зависшие прокси (locked > 5 минут)
    cursor.execute("""
        SELECT COUNT(*) FROM proxies
        WHERE status='locked'
        AND locked_at < NOW() - INTERVAL '5 minutes'
    """)
    health['stuck_proxies'] = cursor.fetchone()[0]

    # 3. Мертвые воркеры (no heartbeat > 4 минут)
    cursor.execute("""
        SELECT COUNT(*) FROM workers
        WHERE status='active'
        AND last_heartbeat < NOW() - INTERVAL '4 minutes'
    """)
    health['dead_workers'] = cursor.fetchone()[0]

    return health


def print_status(tasks_stats: dict, proxies_stats: dict, workers_stats: dict, health: dict):
    """
    Выводит статистику системы в читаемом формате.

    Формат вывода:
        ================================================================================
        СТАТУС СИСТЕМЫ zamer_avito_system
        ================================================================================
        Дата: 2025-11-01 15:30:45
        --------------------------------------------------------------------------------

        ЗАДАЧИ (TASKS):
          pending:    100
          processing: 15
          completed:  500
          failed:     5
          ─────────────────
          Total:      620

        ... и т.д.

    Аргументы:
        tasks_stats: Статистика задач
        proxies_stats: Статистика прокси
        workers_stats: Статистика воркеров
        health: Показатели здоровья системы
    """

    print("=" * 80)
    print("СТАТУС СИСТЕМЫ zamer_avito_system")
    print("=" * 80)
    print(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    print()

    # ========================================
    # СЕКЦИЯ 1: ЗАДАЧИ
    # ========================================
    print("ЗАДАЧИ (TASKS):")
    print(f"  pending:    {tasks_stats['pending']}")
    print(f"  processing: {tasks_stats['processing']}")
    print(f"  completed:  {tasks_stats['completed']}")
    print(f"  failed:     {tasks_stats['failed']}")
    print("  " + "─" * 17)
    total_tasks = sum(tasks_stats.values())
    print(f"  Total:      {total_tasks}")
    print()

    # ========================================
    # СЕКЦИЯ 2: ПРОКСИ
    # ========================================
    print("ПРОКСИ (PROXIES):")
    print(f"  available:  {proxies_stats['available']}")
    print(f"  locked:     {proxies_stats['locked']}")
    print(f"  blocked:    {proxies_stats['blocked']}")
    print("  " + "─" * 17)
    total_proxies = sum(proxies_stats.values())
    print(f"  Total:      {total_proxies}")
    print()

    # ========================================
    # СЕКЦИЯ 3: ВОРКЕРЫ
    # ========================================
    print("ВОРКЕРЫ (WORKERS):")
    print(f"  active:     {workers_stats['active']}")
    print(f"  stopped:    {workers_stats['stopped']}")
    print("  " + "─" * 17)
    total_workers = sum(workers_stats.values())
    print(f"  Total:      {total_workers}")
    print()

    # ========================================
    # СЕКЦИЯ 4: SYSTEM HEALTH
    # ========================================
    print("СОСТОЯНИЕ СИСТЕМЫ (SYSTEM HEALTH):")

    # Проверяем, есть ли проблемы
    has_issues = (
        health['stuck_tasks'] > 0 or
        health['stuck_proxies'] > 0 or
        health['dead_workers'] > 0
    )

    if has_issues:
        print(f"  ⚠ Зависшие задачи (processing > 10 минут):    {health['stuck_tasks']}")
        print(f"  ⚠ Зависшие прокси (locked > 5 минут):         {health['stuck_proxies']}")
        print(f"  ⚠ Мертвые воркеры (no heartbeat > 4 минут):   {health['dead_workers']}")
        print()
        print("  Требуется внимание! Рекомендации:")

        if health['stuck_tasks'] > 0 or health['stuck_proxies'] > 0 or health['dead_workers'] > 0:
            print("    - Запустите cleanup сервис: cd cleanup && docker compose up -d")

        if health['dead_workers'] > 0:
            print("    - Проверьте логи воркеров: docker compose logs worker")

        if proxies_stats['available'] == 0 and proxies_stats['locked'] > 0:
            print("    - Все прокси заняты, рассмотрите добавление новых прокси")
    else:
        print("  ✓ Все системы работают нормально")
        print()
        print("  Проверки:")
        print("    ✓ Нет зависших задач (processing > 10 минут)")
        print("    ✓ Нет зависших прокси (locked > 5 минут)")
        print("    ✓ Нет мертвых воркеров (no heartbeat > 4 минут)")

    print()
    print("=" * 80)


def main():
    """
    Основная функция мониторинга.

    Что делает:
        1. Подключается к PostgreSQL
        2. Собирает статистику (tasks, proxies, workers, health)
        3. Выводит отформатированный отчет

    Возвращает:
        bool: True если успешно, False при ошибке
    """

    conn = None

    try:
        # 1. Подключаемся к PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 2. Собираем статистику
        tasks_stats = get_tasks_stats(cursor)
        proxies_stats = get_proxies_stats(cursor)
        workers_stats = get_workers_stats(cursor)
        health = get_system_health(cursor)

        cursor.close()

        # 3. Выводим статистику
        print_status(tasks_stats, proxies_stats, workers_stats, health)

        return True

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
        # 4. Закрываем подключение
        if conn:
            conn.close()


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
