#!/usr/bin/env python3
"""
utils.py - Вспомогательные функции для воркера

Дата: 01.11.2025
"""


def parse_proxy(proxy_string: str) -> dict[str, str]:
    """
    Парсит строку прокси в формат для Playwright.

    Вход: "host:port:user:pass"
    Выход: {"server": "http://host:port", "username": "user", "password": "pass"}
    """
    parts = proxy_string.split(':')

    if len(parts) != 4:
        raise ValueError(f"Неверный формат прокси: '{proxy_string}'. Ожидается host:port:user:pass")

    host, port, username, password = parts

    return {
        "server": f"http://{host}:{port}",
        "username": username,
        "password": password
    }
