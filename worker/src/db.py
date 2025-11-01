#!/usr/bin/env python3
"""
db.py - База данных для воркера zamer_avito_system

Дата: 01.11.2025
"""

import asyncpg
import asyncio
import json
import time
from typing import Optional
from .config import config


# Глобальный connection pool
pool: Optional[asyncpg.Pool] = None


async def init_pool():
    """Инициализирует connection pool"""
    global pool
    pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        min_size=5,
        max_size=20,
        command_timeout=config.DB_CONNECT_TIMEOUT
    )


async def close_pool():
    """Закрывает connection pool"""
    global pool
    if pool:
        await pool.close()


async def get_connection():
    """Получает соединение из pool с retry логикой"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            if pool is None:
                await init_pool()
            return await pool.acquire()
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                raise e


async def acquire_task(worker_id: str) -> Optional[dict]:
    """
    Атомарно захватывает pending задачу.
    Возвращает: {'task_id': int, 'item_id': int, 'attempts': int} или None
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                row = await conn.fetchrow("""
                    UPDATE tasks
                    SET status='processing',
                        worker_id=$1,
                        last_attempt_at=NOW(),
                        attempts=attempts+1
                    WHERE id = (
                        SELECT id FROM tasks
                        WHERE status='pending'
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, item_id, attempts
                """, worker_id)

                if row:
                    return {
                        'task_id': row['id'],
                        'item_id': row['item_id'],
                        'attempts': row['attempts']
                    }
                return None
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                raise e


async def acquire_proxy(worker_id: str) -> Optional[dict]:
    """
    Атомарно захватывает available прокси.
    Возвращает: {'proxy_id': int, 'proxy': str} или None
    """
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                row = await conn.fetchrow("""
                    UPDATE proxies
                    SET status='locked',
                        locked_by=$1,
                        locked_at=NOW(),
                        uses_count=uses_count+1
                    WHERE proxy = (
                        SELECT proxy FROM proxies
                        WHERE status='available'
                        ORDER BY uses_count ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, proxy
                """, worker_id)

                if row:
                    return {
                        'proxy_id': row['id'],
                        'proxy': row['proxy']
                    }
                return None
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                raise e


async def release_proxy(proxy_string: str) -> bool:
    """Освобождает прокси"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    UPDATE proxies
                    SET status='available',
                        locked_by=NULL,
                        locked_at=NULL,
                        last_used_at=NOW()
                    WHERE proxy=$1
                """, proxy_string)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def mark_proxy_blocked(proxy_string: str) -> bool:
    """Блокирует прокси"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    UPDATE proxies
                    SET status='blocked',
                        blocks_count=blocks_count+1
                    WHERE proxy=$1
                """, proxy_string)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def save_result_to_db(result: dict) -> bool:
    """Сохраняет результат парсинга"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    INSERT INTO results (
                        item_id, title, description, characteristics, price,
                        published_at, seller_name, seller_profile_url,
                        location_address, location_metro, location_region,
                        views_total, status, failure_reason, worker_id, attempts
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT (item_id) DO UPDATE SET
                        title=EXCLUDED.title,
                        description=EXCLUDED.description,
                        characteristics=EXCLUDED.characteristics,
                        price=EXCLUDED.price,
                        published_at=EXCLUDED.published_at,
                        seller_name=EXCLUDED.seller_name,
                        seller_profile_url=EXCLUDED.seller_profile_url,
                        location_address=EXCLUDED.location_address,
                        location_metro=EXCLUDED.location_metro,
                        location_region=EXCLUDED.location_region,
                        views_total=EXCLUDED.views_total,
                        status=EXCLUDED.status,
                        failure_reason=EXCLUDED.failure_reason,
                        worker_id=EXCLUDED.worker_id,
                        attempts=EXCLUDED.attempts,
                        updated_at=NOW()
                """,
                    result['item_id'],
                    result.get('title'),
                    result.get('description'),
                    json.dumps(result.get('characteristics', {})),
                    result.get('price'),
                    result.get('published_at'),
                    result.get('seller_name'),
                    result.get('seller_profile_url'),
                    result.get('location_address'),
                    result.get('location_metro'),
                    result.get('location_region'),
                    result.get('views_total'),
                    result['status'],
                    result.get('failure_reason'),
                    result['worker_id'],
                    result['attempts']
                )
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def mark_task_completed(task_id: int) -> bool:
    """Отмечает задачу как выполненную"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    UPDATE tasks
                    SET status='completed',
                        completed_at=NOW()
                    WHERE id=$1
                """, task_id)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def increment_task_attempts(task_id: int) -> bool:
    """Возвращает задачу в pending или failed (attempts уже увеличен в acquire_task)"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    UPDATE tasks
                    SET status = CASE
                        WHEN attempts >= max_attempts THEN 'failed'
                        ELSE 'pending'
                    END,
                    worker_id = NULL,
                    last_attempt_at = NULL
                    WHERE id=$1
                """, task_id)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def increment_worker_stats(worker_id: str, success: bool) -> bool:
    """Обновляет статистику воркера"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                if success:
                    await conn.execute("""
                        UPDATE workers
                        SET tasks_processed=tasks_processed+1
                        WHERE worker_id=$1
                    """, worker_id)
                else:
                    await conn.execute("""
                        UPDATE workers
                        SET tasks_failed=tasks_failed+1
                        WHERE worker_id=$1
                    """, worker_id)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False


async def update_heartbeat(worker_id: str) -> bool:
    """Обновляет heartbeat воркера (регистрирует при первом вызове)"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            conn = await get_connection()
            try:
                await conn.execute("""
                    INSERT INTO workers (worker_id)
                    VALUES ($1)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        last_heartbeat=NOW(),
                        status='active'
                """, worker_id)
                return True
            finally:
                await pool.release(conn)
        except Exception as e:
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)
            else:
                return False
