#!/usr/bin/env python3
"""
cleanup.py - Cleanup сервис для zamer_avito_system

Освобождает зависшие ресурсы каждые 60 секунд:
1. Tasks в processing > 10 минут → pending
2. Proxies locked > 5 минут → available
3. Workers без heartbeat > 4 минут → stopped
4. Tasks с attempts >= max_attempts → failed

Дата: 01.11.2025
"""

import asyncio
import asyncpg
import logging
import os
import signal
from typing import Optional


# Конфигурация
class Config:
    # Database
    DB_HOST: str = os.getenv('DB_HOST', '81.30.105.134')
    DB_PORT: int = int(os.getenv('DB_PORT', '5413'))
    DB_NAME: str = os.getenv('DB_NAME', 'zamer_avito_system')
    DB_USER: str = os.getenv('DB_USER', 'admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', 'Password123')

    # Timeouts (в секундах)
    TASK_TIMEOUT: int = int(os.getenv('TASK_TIMEOUT', '600'))  # 10 минут
    PROXY_TIMEOUT: int = int(os.getenv('PROXY_TIMEOUT', '300'))  # 5 минут
    WORKER_TIMEOUT: int = int(os.getenv('WORKER_TIMEOUT', '240'))  # 4 минуты

    # Cleanup cycle
    CLEANUP_INTERVAL: int = int(os.getenv('CLEANUP_INTERVAL', '60'))  # 60 секунд

    # Retry
    DB_CONNECT_TIMEOUT: int = int(os.getenv('DB_CONNECT_TIMEOUT', '10'))
    DB_RETRY_ATTEMPTS: int = int(os.getenv('DB_RETRY_ATTEMPTS', '5'))
    RETRY_DELAY: int = int(os.getenv('RETRY_DELAY', '10'))


config = Config()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Connection pool
pool: Optional[asyncpg.Pool] = None
shutdown_event = asyncio.Event()


async def init_pool():
    """Инициализирует connection pool"""
    global pool
    logger.info(f"Connecting to PostgreSQL at {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        min_size=2,
        max_size=5,
        command_timeout=config.DB_CONNECT_TIMEOUT
    )
    logger.info("Connection pool initialized")


async def close_pool():
    """Закрывает connection pool"""
    global pool
    if pool:
        await pool.close()
        logger.info("Connection pool closed")


async def get_connection():
    """Получает соединение из pool с retry логикой"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            if pool is None:
                await init_pool()
            return await pool.acquire()
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                logger.warning(f"DB connection failed (attempt {attempt + 1}/{config.DB_RETRY_ATTEMPTS}): {e}")
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                logger.error(f"DB connection failed after {config.DB_RETRY_ATTEMPTS} attempts: {e}")
                raise e


async def release_stuck_tasks() -> int:
    """
    Освобождает зависшие tasks (processing > TASK_TIMEOUT)
    Возвращает: количество освобожденных задач
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                result = await conn.execute(f"""
                    UPDATE tasks
                    SET status='pending',
                        worker_id=NULL,
                        last_attempt_at=NULL
                    WHERE status='processing'
                    AND last_attempt_at < NOW() - INTERVAL '{config.TASK_TIMEOUT} seconds'
                """)

                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    logger.info(f"Released {count} stuck tasks")
                return count

            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                logger.error(f"Failed to release stuck tasks: {e}")
                return 0


async def release_stuck_proxies() -> int:
    """
    Освобождает зависшие proxies (locked > PROXY_TIMEOUT)
    Возвращает: количество освобожденных прокси
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                result = await conn.execute(f"""
                    UPDATE proxies
                    SET status='available',
                        locked_by=NULL,
                        locked_at=NULL,
                        last_used_at=NOW()
                    WHERE status='locked'
                    AND locked_at < NOW() - INTERVAL '{config.PROXY_TIMEOUT} seconds'
                """)

                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    logger.info(f"Released {count} stuck proxies")
                return count

            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                logger.error(f"Failed to release stuck proxies: {e}")
                return 0


async def mark_dead_workers() -> int:
    """
    Помечает мертвые workers (heartbeat > WORKER_TIMEOUT) как stopped
    Возвращает: количество помеченных воркеров
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                result = await conn.execute(f"""
                    UPDATE workers
                    SET status='stopped'
                    WHERE status='active'
                    AND last_heartbeat < NOW() - INTERVAL '{config.WORKER_TIMEOUT} seconds'
                """)

                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    logger.info(f"Marked {count} dead workers as stopped")
                return count

            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                logger.error(f"Failed to mark dead workers: {e}")
                return 0


async def fail_impossible_tasks() -> int:
    """
    Переводит безнадежные tasks (attempts >= max_attempts) в failed
    Возвращает: количество переведенных задач
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                result = await conn.execute("""
                    UPDATE tasks
                    SET status='failed'
                    WHERE status='pending'
                    AND attempts >= max_attempts
                """)

                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    logger.info(f"Marked {count} impossible tasks as failed")
                return count

            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                logger.error(f"Failed to mark impossible tasks: {e}")
                return 0


async def cleanup_cycle():
    """Выполняет один цикл cleanup (все 4 операции)"""
    tasks_released = await release_stuck_tasks()
    proxies_released = await release_stuck_proxies()
    workers_marked = await mark_dead_workers()
    tasks_failed = await fail_impossible_tasks()

    logger.info(
        f"Cleanup cycle completed: "
        f"tasks={tasks_released}, proxies={proxies_released}, "
        f"workers={workers_marked}, failed={tasks_failed}"
    )


async def main():
    """Основной цикл cleanup сервиса"""
    logger.info("Starting cleanup service")
    logger.info(f"Timeouts: task={config.TASK_TIMEOUT}s, proxy={config.PROXY_TIMEOUT}s, worker={config.WORKER_TIMEOUT}s")
    logger.info(f"Cleanup interval: {config.CLEANUP_INTERVAL}s")

    # Инициализация connection pool
    await init_pool()

    try:
        while not shutdown_event.is_set():
            try:
                await cleanup_cycle()
            except Exception as e:
                logger.error(f"Error in cleanup cycle: {e}")

            # Ждем CLEANUP_INTERVAL секунд или сигнала shutdown
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=config.CLEANUP_INTERVAL
                )
                break  # shutdown_event был установлен
            except asyncio.TimeoutError:
                pass  # Таймаут истек, продолжаем цикл

    finally:
        logger.info("Shutting down cleanup service")
        await close_pool()


def signal_handler(signum, frame):
    """Обработчик сигналов SIGTERM/SIGINT"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown")
    shutdown_event.set()


if __name__ == "__main__":
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Запускаем основной цикл
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    logger.info("Cleanup service stopped")
