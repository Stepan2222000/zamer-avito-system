"""Основной модуль воркера - оркестрация 15 asyncio корутин.

Запускает 15 независимых воркеров, каждый с уникальным worker_id и DISPLAY.
"""
from __future__ import annotations

import asyncio
import os
import signal
import time

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from .config import config, get_settings
from .db import (
    acquire_proxy,
    acquire_task,
    close_pool,
    increment_task_attempts,
    increment_worker_stats,
    init_pool,
    mark_proxy_blocked,
    mark_task_completed,
    release_proxy,
    save_result_to_db,
    update_heartbeat,
)
from .logging_utils import configure_logging, log_event
from .processor import process_item
from .utils import parse_proxy

# Глобальный флаг для graceful shutdown
shutdown_event = asyncio.Event()


def signal_handler(signum: int, frame) -> None:
    """Обработчик сигналов SIGTERM/SIGINT для graceful shutdown"""
    log_event("shutdown_signal_received", extra={"signal": signum})
    shutdown_event.set()


async def register_worker(worker_id: str) -> bool:
    """Регистрирует воркер в БД с retry логикой (5 попыток по 10 сек)"""
    for attempt in range(config.DB_RETRY_ATTEMPTS):
        try:
            await update_heartbeat(worker_id)
            log_event("worker_registered", extra={"worker_id": worker_id})
            return True
        except Exception as exc:
            log_event(
                "worker_registration_retry",
                extra={
                    "worker_id": worker_id,
                    "attempt": attempt + 1,
                    "error": str(exc),
                },
            )
            if attempt < config.DB_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(config.RETRY_DELAY)

    log_event("worker_registration_failed", extra={"worker_id": worker_id})
    return False


async def init_playwright(
    worker_id: str,
    worker_index: int,
) -> tuple[Playwright, Browser, BrowserContext, Page, str]:
    """Инициализирует Playwright браузер с прокси и виртуальным дисплеем."""
    proxy_data = await acquire_proxy(worker_id)
    if not proxy_data:
        raise RuntimeError("No proxy available during init")

    proxy_string = proxy_data["proxy"]
    proxy_config = parse_proxy(proxy_string)

    display_value = f":{worker_index}"
    launch_env = os.environ.copy()
    launch_env["DISPLAY"] = display_value

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        proxy=proxy_config,
        headless=False,
        env=launch_env,
    )
    context = await browser.new_context()
    page = await context.new_page()

    log_event(
        "worker_page_ready",
        extra={
            "worker_id": worker_id,
            "proxy": proxy_string,
            "display": display_value,
        },
    )

    return playwright, browser, context, page, proxy_string


async def cleanup_playwright(
    playwright: Playwright,
    browser: Browser,
    context: BrowserContext,
    page: Page,
) -> None:
    """Корректно закрывает браузер и Playwright."""
    try:
        await page.close()
    except Exception:
        pass
    try:
        await context.close()
    except Exception:
        pass
    try:
        await browser.close()
    except Exception:
        pass
    try:
        await playwright.stop()
    except Exception:
        pass


async def worker_loop(worker_id: str, worker_index: int) -> None:
    """Основной цикл обработки задач одним воркером."""
    playwright = None
    browser = None
    context = None
    page = None
    current_proxy = None

    try:
        log_event("worker_start", extra={"worker_id": worker_id})

        # Регистрация воркера с retry логикой
        if not await register_worker(worker_id):
            log_event("worker_start_aborted", extra={"worker_id": worker_id})
            return

        last_heartbeat = time.time()

        playwright, browser, context, page, current_proxy = await init_playwright(
            worker_id, worker_index
        )

        while not shutdown_event.is_set():
            if time.time() - last_heartbeat > config.HEARTBEAT_INTERVAL:
                await update_heartbeat(worker_id)
                last_heartbeat = time.time()

            task = await acquire_task(worker_id)
            if not task:
                log_event("worker_no_tasks", extra={"worker_id": worker_id})
                break

            try:
                result = await process_item(page, task, current_proxy, worker_id)

                if result.get("rotate_proxy"):
                    await mark_proxy_blocked(current_proxy)
                    await cleanup_playwright(playwright, browser, context, page)
                    await release_proxy(current_proxy)

                    current_proxy = None
                    playwright = None
                    browser = None
                    context = None
                    page = None

                    playwright, browser, context, page, current_proxy = await init_playwright(
                        worker_id, worker_index
                    )

                if result["status"] in ["success", "unavailable"]:
                    await save_result_to_db(result)
                    await mark_task_completed(task["task_id"])
                    await increment_worker_stats(worker_id, success=True)
                    log_event(
                        "task_success",
                        item_id=task["item_id"],
                        proxy=current_proxy,
                        extra={"worker_id": worker_id},
                    )
                else:
                    await increment_task_attempts(task["task_id"])
                    await increment_worker_stats(worker_id, success=False)

            except Exception as exc:
                log_event(
                    "worker_error",
                    item_id=task["item_id"],
                    proxy=current_proxy,
                    extra={"worker_id": worker_id, "error": str(exc)},
                )
                await increment_task_attempts(task["task_id"])
                await increment_worker_stats(worker_id, success=False)

            await asyncio.sleep(0)

    except Exception as exc:
        log_event(
            "worker_fatal_error",
            extra={"worker_id": worker_id, "error": str(exc)},
        )

    finally:
        if playwright and browser and context and page:
            await cleanup_playwright(playwright, browser, context, page)
        if current_proxy:
            await release_proxy(current_proxy)
        log_event("worker_shutdown", extra={"worker_id": worker_id})


async def main() -> None:
    """Точка входа: инициализирует БД и запускает 15 воркеров."""
    # Регистрация signal handlers для graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    settings = get_settings()
    configure_logging(settings)

    log_event("app_start", extra={"workers_count": config.WORKERS_COUNT})

    await init_pool()

    base_worker_id = config.get_worker_id(config.PROGRAM_ID)

    tasks = []
    for i in range(config.WORKERS_COUNT):
        worker_id = f"{base_worker_id}:{i}"
        tasks.append(worker_loop(worker_id, i))

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await close_pool()
        log_event("app_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
