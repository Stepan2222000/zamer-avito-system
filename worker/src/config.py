#!/usr/bin/env python3
"""
config.py - Конфигурация воркера zamer_avito_system

Дата: 01.11.2025
"""

import os
import socket
from dataclasses import dataclass


@dataclass
class Config:
    """Конфигурация воркера"""

    # Database
    DB_HOST: str = os.getenv('DB_HOST', '81.30.105.134')
    DB_PORT: int = int(os.getenv('DB_PORT', '5413'))
    DB_NAME: str = os.getenv('DB_NAME', 'zamer_avito_system')
    DB_USER: str = os.getenv('DB_USER', 'admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', 'Password123')

    # Program Identity
    PROGRAM_ID: str = os.getenv('PROGRAM_ID', 'zamer_avito_worker')

    # Workers
    WORKERS_COUNT: int = int(os.getenv('WORKERS_COUNT', '15'))

    # Timeouts (секунды)
    TASK_TIMEOUT: int = int(os.getenv('TASK_TIMEOUT', '600'))
    DB_CONNECT_TIMEOUT: int = int(os.getenv('DB_CONNECT_TIMEOUT', '10'))
    PLAYWRIGHT_TIMEOUT: int = int(os.getenv('PLAYWRIGHT_TIMEOUT', '30000'))

    # Retry Logic
    MAX_TASK_ATTEMPTS: int = int(os.getenv('MAX_TASK_ATTEMPTS', '5'))
    RETRY_DELAY: int = int(os.getenv('RETRY_DELAY', '10'))
    DB_RETRY_ATTEMPTS: int = int(os.getenv('DB_RETRY_ATTEMPTS', '5'))

    # Heartbeat
    HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', '30'))

    # Proxy
    PROXY_ROTATION_ENABLED: bool = os.getenv('PROXY_ROTATION_ENABLED', 'true').lower() == 'true'

    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')

    @staticmethod
    def get_hostname() -> str:
        """Получает hostname текущей машины"""
        return socket.gethostname()

    @staticmethod
    def get_worker_id(program_id: str) -> str:
        """Формирует уникальный worker_id: {PROGRAM_ID}:{hostname}:{pid}"""
        hostname = Config.get_hostname()
        pid = os.getpid()
        return f"{program_id}:{hostname}:{pid}"

    def get_db_dsn(self) -> str:
        """Формирует DSN строку для PostgreSQL"""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


# Глобальный экземпляр конфигурации
config = Config()

# Алиас для совместимости с logging_utils
Settings = Config


def get_settings() -> Config:
    """Возвращает глобальный экземпляр конфигурации"""
    return config
