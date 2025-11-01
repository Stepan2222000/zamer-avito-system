# План реализации распределенной системы zamer_avito

**Принцип:** KISS | 

РАБОТАЕМ ИСКЛЮЧИТЕЛЬНО В ПАПКЕ /Users/stepanorlov/Desktop/STARTED/zamer_avito/zamer_avito_system 

---

## Этап 1: Структура проекта 
- Создать папки: `sql/`, `cli/`, `worker/src/`, `cleanup/`, `data/`

## Этап 2: SQL схема 
- Создать `sql/init.sql` с 4 таблицами (tasks, proxies, workers, results)
- Добавить 6 индексов
- Создать `cli/init_db.py` - выполнение init.sql

## Этап 3: CLI скрипты 
- `cli/upload_tasks.py` - data/items.txt → tasks
- `cli/upload_proxies.py` - data/proxies.txt → proxies
- `cli/status.py` - статистика задач/прокси/воркеров
- `cli/download_results.py` - экспорт JSON/CSV

## Этап 4: Worker - конфигурация
- `worker/src/config.py` - копия src/config.py + поля для координации
- `worker/src/utils.py` - parse_proxy()

## Этап 5: Worker - база данных 
- `worker/src/db.py` с 9 функциями:
  - acquire_task() - FOR UPDATE SKIP LOCKED
  - acquire_proxy() - FOR UPDATE SKIP LOCKED
  - release_proxy()
  - mark_proxy_blocked()
  - save_result_to_db() - ON CONFLICT DO UPDATE
  - mark_task_completed()
  - increment_task_attempts()
  - increment_worker_stats()
  - update_heartbeat()

## Этап 6: Worker - процессор 
- `worker/src/processor.py` - КОПИЯ методов из src/worker.py БЕЗ ИЗМЕНЕНИЙ
- Копировать: navigate(), _handle_*(), _process_task(), _build_listing_record()
- Главная функция: process_item()

## Этап 7: Worker - основной цикл 
- `worker/src/main.py`:
  - register_worker() - 5 retry попыток
  - heartbeat_loop() - каждые 30 сек
  - worker_loop() - acquire → process → save → release
  - graceful_shutdown() - SIGTERM/SIGINT
  - main() - запуск всего

## Этап 8: Worker - Docker 
- `worker/Dockerfile` - Python 3.13 + Playwright + Chromium
- `worker/docker-compose.yml` - xvfb + worker + environment
- `worker/requirements.txt` - зависимости

## Этап 9: Cleanup сервис (20 мин)
- `cleanup/cleanup.py` - 4 функции освобождения ресурсов (каждые 60 сек)
- `cleanup/Dockerfile` - легковесный образ
- `cleanup/docker-compose.yml` - контейнер

## Этап 10: Тестирование (30 мин)
- Инициализация БД
- Загрузка тестовых данных
- Запуск cleanup
- Запуск 1 воркера
- Тест координации (2 воркера)
- Тест cleanup при падении
- Выгрузка результатов

---

## Критические правила

1. **Этап 6** - НЕ МЕНЯТЬ логику парсинга из src/worker.py
2. **Этап 5** - обязательно FOR UPDATE SKIP LOCKED
3. **Этап 7** - обязательно try/finally для release_proxy
4. **Этап 2** - индексы критически важны
5. **Этап 9** - cleanup должен работать 24/7

---

**Следующий шаг:** Этап 1 - создание структуры