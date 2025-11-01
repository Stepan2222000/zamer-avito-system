# syntax=docker/dockerfile:1
FROM python:3.13-slim

# Отключаем буферизацию stdout/stderr + кэш pip
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ===== Системные зависимости =====
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # Git для установки avito-library
        git \
        wget \
        gnupg \
        ca-certificates \
        # Xvfb + X11 зависимости
        xvfb \
        xauth \
        # Chromium зависимости (Playwright)
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libatspi2.0-0 \
        libdrm2 \
        libxkbcommon0 \
        libgtk-3-0 \
        libnss3 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libpango-1.0-0 \
        libpangocairo-1.0-0 \
        libxshmfence1 \
        libasound2 \
        fonts-liberation \
        # OpenCV зависимости (для numpy/opencv-python-headless)
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender-dev \
        libgomp1 \
        # Утилиты
        procps \
    && rm -rf /var/lib/apt/lists/*

# ===== Python зависимости =====
COPY requirements.txt ./requirements.txt

# Устанавливаем зависимости с retry для git clone
RUN pip install --upgrade pip \
    && for i in 1 2 3; do \
        pip install -r requirements.txt && break || \
        (echo "Retry $i/3..." && sleep 5); \
    done \
    && playwright install-deps chromium \
    && playwright install chromium

# ===== Копирование кода =====
COPY src/ ./src/
COPY entrypoint.sh ./entrypoint.sh

RUN chmod +x ./entrypoint.sh

# ===== Конфигурация окружения =====
# Playwright не в headless режиме (критически важно!)
ENV PLAYWRIGHT_HEADLESS=false

# Точка входа
ENTRYPOINT ["./entrypoint.sh"]
