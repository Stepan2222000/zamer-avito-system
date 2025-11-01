FROM python:3.12-slim

# Рабочая директория
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .

# Устанавливаем build tools для компиляции asyncpg, затем удаляем их
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

# Копируем cleanup.py
COPY cleanup.py .

# Делаем скрипт исполняемым
RUN chmod +x cleanup.py

# Запуск cleanup сервиса
CMD ["python", "cleanup.py"]
