"""Процессор задач - логика обработки детекторов страницы.

Копирует логику из старого Worker с адаптацией под новую архитектуру.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

from avito_library import (
    CAPTCHA_DETECTOR_ID,
    CARD_FOUND_DETECTOR_ID,
    CATALOG_DETECTOR_ID,
    CONTINUE_BUTTON_DETECTOR_ID,
    PROXY_AUTH_DETECTOR_ID,
    PROXY_BLOCK_403_DETECTOR_ID,
    PROXY_BLOCK_429_DETECTOR_ID,
    REMOVED_DETECTOR_ID,
    SELLER_PROFILE_DETECTOR_ID,
    CardData,
    CardParsingError,
    DetectionError,
    detect_page_state,
    parse_card,
    resolve_captcha_flow,
)
from playwright.async_api import Page

from .logging_utils import log_event

PRIORITY_ORDER = (
    PROXY_BLOCK_403_DETECTOR_ID,
    PROXY_AUTH_DETECTOR_ID,
    PROXY_BLOCK_429_DETECTOR_ID,
    CAPTCHA_DETECTOR_ID,
    REMOVED_DETECTOR_ID,
    SELLER_PROFILE_DETECTOR_ID,
    CATALOG_DETECTOR_ID,
    CARD_FOUND_DETECTOR_ID,
    CONTINUE_BUTTON_DETECTOR_ID,
)


async def process_item(
    page: Page,
    task: dict,
    proxy_string: str,
    worker_id: str,
) -> dict:
    """Обрабатывает одну задачу: navigate → detect → обработка по state."""
    url = f"https://www.avito.ru/{task['item_id']}"

    log_event(
        "task_start",
        item_id=task["item_id"],
        proxy=proxy_string,
        extra={"worker_id": worker_id, "attempt": task["attempts"]},
    )

    await page.goto(url, wait_until="domcontentloaded", timeout=30000)

    try:
        state = await detect_page_state(page, priority=PRIORITY_ORDER)
    except DetectionError as exc:
        log_event(
            "detection_error",
            item_id=task["item_id"],
            proxy=proxy_string,
            extra={"worker_id": worker_id, "error": str(exc)},
        )
        return {
            "item_id": task["item_id"],
            "status": "error",
            "failure_reason": "detection_error",
            "rotate_proxy": True,
        }

    log_event(
        "worker_detect_state",
        item_id=task["item_id"],
        proxy=proxy_string,
        extra={"worker_id": worker_id, "state": state},
    )

    if state in [CAPTCHA_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID]:
        return await handle_captcha(page, task, proxy_string, worker_id)

    if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
        return handle_proxy_block(task, state, worker_id)

    if state == CARD_FOUND_DETECTOR_ID:
        return await handle_card_found(page, task, worker_id)

    if state == REMOVED_DETECTOR_ID:
        return handle_removed(task, worker_id)

    if state in [SELLER_PROFILE_DETECTOR_ID, CATALOG_DETECTOR_ID]:
        return handle_unexpected(task, state, worker_id)

    return {
        "item_id": task["item_id"],
        "status": "error",
        "failure_reason": f"unknown_state_{state}",
        "rotate_proxy": True,
    }


async def handle_captcha(
    page: Page,
    task: dict,
    proxy_string: str,
    worker_id: str,
) -> dict:
    """Решает капчу через resolve_captcha_flow, затем повторный детект."""
    _, solved = await resolve_captcha_flow(page, max_attempts=3)

    if not solved:
        log_event(
            "captcha_failed",
            item_id=task["item_id"],
            proxy=proxy_string,
            extra={"worker_id": worker_id},
        )
        return {
            "item_id": task["item_id"],
            "status": "error",
            "failure_reason": "captcha_failed",
            "rotate_proxy": True,
        }

    state = await detect_page_state(page, priority=PRIORITY_ORDER)
    log_event(
        "captcha_resolved",
        item_id=task["item_id"],
        proxy=proxy_string,
        extra={"worker_id": worker_id, "new_state": state},
    )

    if state == CARD_FOUND_DETECTOR_ID:
        return await handle_card_found(page, task, worker_id)
    elif state == REMOVED_DETECTOR_ID:
        return handle_removed(task, worker_id)
    else:
        return {
            "item_id": task["item_id"],
            "status": "error",
            "failure_reason": f"unexpected_after_captcha_{state}",
            "rotate_proxy": False,
        }


async def handle_card_found(
    page: Page,
    task: dict,
    worker_id: str,
) -> dict:
    """Парсит карточку через parse_card и возвращает результат."""
    html = await page.content()

    try:
        card = parse_card(
            html,
            fields=(
                "title",
                "description",
                "characteristics",
                "price",
                "seller",
                "item_id",
                "published_at",
                "location",
                "views_total",
            ),
        )
    except CardParsingError as exc:
        log_event(
            "task_parse_error",
            item_id=task["item_id"],
            extra={"worker_id": worker_id, "error": str(exc)},
        )
        return {
            "item_id": task["item_id"],
            "status": "error",
            "failure_reason": "parse_card_error",
            "rotate_proxy": False,
        }

    if card.item_id and card.item_id != task["item_id"]:
        log_event(
            "task_item_mismatch",
            item_id=task["item_id"],
            extra={"worker_id": worker_id, "card_item_id": card.item_id},
        )

    return build_result(task, card, status="success", worker_id=worker_id)


def handle_removed(task: dict, worker_id: str) -> dict:
    """Обрабатывает удаленное объявление → status='unavailable'."""
    log_event(
        "task_missing",
        item_id=task["item_id"],
        extra={"worker_id": worker_id},
    )

    return {
        "item_id": task["item_id"],
        "status": "unavailable",
        "worker_id": worker_id,
        "attempts": task["attempts"],
        "failure_reason": None,
        "rotate_proxy": False,
    }


def handle_proxy_block(task: dict, state: str, worker_id: str) -> dict:
    """Обрабатывает блокировку прокси (403/407) → rotate_proxy=True."""
    reason = "http_403" if state == PROXY_BLOCK_403_DETECTOR_ID else "http_407"

    log_event(
        "proxy_blocked",
        item_id=task["item_id"],
        extra={"worker_id": worker_id, "reason": reason},
    )

    return {
        "item_id": task["item_id"],
        "status": "error",
        "failure_reason": f"proxy_blocked_{reason}",
        "rotate_proxy": True,
    }


def handle_unexpected(task: dict, state: str, worker_id: str) -> dict:
    """Обрабатывает неожиданные состояния (SELLER_PROFILE, CATALOG)."""
    log_event(
        "unexpected_state",
        item_id=task["item_id"],
        extra={"worker_id": worker_id, "state": state},
    )

    return {
        "item_id": task["item_id"],
        "status": "error",
        "failure_reason": f"unexpected_state_{state}",
        "rotate_proxy": False,
    }


def build_result(
    task: dict,
    card: CardData,
    status: str,
    worker_id: str,
) -> dict:
    """Строит result dict из CardData для сохранения в БД."""
    location = card.location or {}
    seller = card.seller or {}

    return {
        "item_id": task["item_id"],
        "status": status,
        "title": card.title,
        "description": card.description,
        "characteristics": card.characteristics,
        "price": _normalize_price(card.price),
        "published_at": card.published_at,
        "seller_name": seller.get("name"),
        "seller_profile_url": seller.get("profile_url"),
        "location_address": location.get("address"),
        "location_metro": location.get("metro"),
        "location_region": location.get("region"),
        "views_total": _to_int(card.views_total),
        "worker_id": worker_id,
        "attempts": task["attempts"],
        "failure_reason": None,
        "rotate_proxy": False,
    }


def _normalize_price(raw_price: Optional[Any]) -> Optional[Decimal]:
    """Преобразовать цену в Decimal(12,2)."""
    if raw_price is None:
        return None
    try:
        price = Decimal(str(raw_price))
        return price.quantize(Decimal("1.00"))
    except Exception:
        return None


def _to_int(value: Any) -> Optional[int]:
    """Безопасно привести значение к int."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "process_item",
    "handle_captcha",
    "handle_card_found",
    "handle_removed",
    "handle_proxy_block",
    "handle_unexpected",
    "build_result",
]
