#!/usr/bin/env bash
set -euo pipefail

# Количество воркеров из окружения (по умолчанию 15)
WORKERS=${WORKERS_COUNT:-15}
DISPLAY_BASE=0
SCREEN_SPEC=${XVFB_SCREEN:-1920x1080x24}

# Массив для хранения PID процессов Xvfb
declare -a XVFB_PIDS=()

# Cleanup функция для graceful shutdown
cleanup() {
  echo "Shutting down Xvfb servers..."
  for pid in "${XVFB_PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  wait || true
}

# Регистрируем cleanup на EXIT/INT/TERM
trap cleanup EXIT INT TERM

# Запуск WORKERS_COUNT Xvfb серверов (один на каждый воркер)
echo "Starting ${WORKERS} Xvfb servers..."
for ((i = 0; i < WORKERS; i++)); do
  DISPLAY_NUM=$((DISPLAY_BASE + i))
  echo "  - Xvfb :${DISPLAY_NUM} (screen ${SCREEN_SPEC})"
  Xvfb ":${DISPLAY_NUM}" -screen 0 "${SCREEN_SPEC}" -nolisten tcp >/tmp/xvfb-${DISPLAY_NUM}.log 2>&1 &
  XVFB_PIDS+=("$!")
done

# Даем Xvfb время на инициализацию
sleep 2

# Запуск Python приложения (main.py)
echo "Starting Python worker application..."
exec python -m src.main
