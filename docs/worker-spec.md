# Спецификация Worker - Полная документация

**Дата:** 01.11.2025
**Этап:** 6 - Worker процессор
**Принцип:** KISS + максимальное переиспользование логики из старого проекта

---

## 📋 Содержание

1. [Архитектура](#архитектура)
2. [Логика обработки детекторов](#логика-обработки-детекторов)
3. [Форматы данных](#форматы-данных)
4. [Обработка ошибок](#обработка-ошибок)
5. [Управление браузером](#управление-браузером)
6. [Heartbeat механизм](#heartbeat-механизм)
7. [Алгоритмы функций](#алгоритмы-функций)

---

## 🏗️ Архитектура

### Структура контейнера

```
Docker Container (один процесс Python):
├─ main.py
│  │
│  ├─ asyncio.gather():
│  │  ├─ worker_loop(worker_id_0, index=0)   # DISPLAY=:0, browser #1
│  │  ├─ worker_loop(worker_id_1, index=1)   # DISPLAY=:1, browser #2
│  │  ├─ ...
│  │  └─ worker_loop(worker_id_14, index=14) # DISPLAY=:14, browser #15
│
├─ Xvfb :0  (виртуальный дисплей для воркера 0)
├─ Xvfb :1  (виртуальный дисплей для воркера 1)
├─ ...
└─ Xvfb :14 (виртуальный дисплей для воркера 14)
```

### Ключевые параметры

- **WORKERS_COUNT:** 15 (из config.py)
- **Архитектура:** 15 asyncio корутин в одном процессе
- **worker_id:** **ОДИН НА КАЖДОГО ВОРКЕРА** (15 разных worker_id)
  ```python
  worker_id_0 = f"{PROGRAM_ID}:{hostname}:{pid}:0"  # zamer_avito_worker:host:12345:0
  worker_id_1 = f"{PROGRAM_ID}:{hostname}:{pid}:1"  # zamer_avito_worker:host:12345:1
  ...
  worker_id_14 = f"{PROGRAM_ID}:{hostname}:{pid}:14"
  ```
- **БД регистрация:** 15 записей в таблице `workers` (по одной на воркер)
- **Heartbeat:** Каждый воркер обновляет свой heartbeat независимо

### Playwright конфигурация

```python
# Для каждого воркера:
DISPLAY = f":{worker_index}"  # :0, :1, :2, ..., :14
headless = False              # ОБЯЗАТЕЛЬНО False!
```

**Важно:**
- В Docker используем Xvfb (виртуальный X сервер)
- Headless=False критически важен для обхода детекции
- Каждый воркер получает свой DISPLAY

---

## 🔄 Логика обработки детекторов

### Приоритет детекторов (из avito-library)

```python
from avito_library import (
    PROXY_BLOCK_403_DETECTOR_ID,      # "proxy_block_403_detector"
    PROXY_AUTH_DETECTOR_ID,            # "proxy_auth_detector" (407)
    PROXY_BLOCK_429_DETECTOR_ID,       # "proxy_block_429_detector"
    CAPTCHA_DETECTOR_ID,               # "captcha_geetest_detector"
    REMOVED_DETECTOR_ID,               # "removed_detector"
    SELLER_PROFILE_DETECTOR_ID,        # "seller_profile_detector"
    CATALOG_DETECTOR_ID,               # "catalog_page_detector"
    CARD_FOUND_DETECTOR_ID,            # "card_found_detector"
    CONTINUE_BUTTON_DETECTOR_ID,       # "continue_button_detector"
    detect_page_state,
    resolve_captcha_flow,
    parse_card,
    CardParsingError,
)

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
```

### Алгоритм после page.goto()

```
1. await page.goto(url, wait_until="domcontentloaded")
2. state = await detect_page_state(page, priority=PRIORITY_ORDER)
3. Обработка по state:
   ├─ CAPTCHA / CONTINUE_BUTTON / 429
   │  └─ resolve_captcha_flow(page)
   │     ├─ Решилась → detect_page_state() → продолжить обработку
   │     └─ НЕ решилась → rotate_proxy=True
   │
   ├─ 403 / 407
   │  └─ rotate_proxy=True (блокировка прокси)
   │
   ├─ CARD_FOUND
   │  └─ parse_card() → status='success'
   │
   ├─ REMOVED
   │  └─ status='unavailable'
   │
   └─ SELLER_PROFILE / CATALOG / DETECTION_ERROR
      └─ Проверить повтор → возможно rotate_proxy
```

### Детальная обработка каждого состояния

#### 1. CAPTCHA / CONTINUE_BUTTON / 429

```python
if state in [CAPTCHA_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID]:
    html, solved = await resolve_captcha_flow(page, max_attempts=3)

    if solved:
        # Капча решилась → повторный детект
        state = await detect_page_state(page, priority=PRIORITY_ORDER)

        # Продолжаем обработку с новым state
        if state == CARD_FOUND_DETECTOR_ID:
            return await handle_card_found(page, task, worker_id)
        elif state == REMOVED_DETECTOR_ID:
            return await handle_removed(task, worker_id)
        # и т.д.
    else:
        # Капча НЕ решилась → блокировка прокси
        return {
            'item_id': task['item_id'],
            'status': 'error',
            'failure_reason': 'captcha_failed',
            'rotate_proxy': True  # Пересоздать браузер с новым прокси
        }
```

#### 2. PROXY_BLOCK_403 / PROXY_AUTH (407)

```python
if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
    reason = 'http_403' if state == PROXY_BLOCK_403_DETECTOR_ID else 'http_407'

    return {
        'item_id': task['item_id'],
        'status': 'error',
        'failure_reason': f'proxy_blocked_{reason}',
        'rotate_proxy': True  # Пересоздать браузер
    }
```

#### 3. CARD_FOUND

```python
if state == CARD_FOUND_DETECTOR_ID:
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
            )
        )
    except CardParsingError as exc:
        return {
            'item_id': task['item_id'],
            'status': 'error',
            'failure_reason': 'parse_card_error',
            'rotate_proxy': False  # Продолжаем на той же странице
        }

    # Построение результата
    return build_result(task, card, status='success', worker_id=worker_id)
```

#### 4. REMOVED

```python
if state == REMOVED_DETECTOR_ID:
    return {
        'item_id': task['item_id'],
        'status': 'unavailable',
        'worker_id': worker_id,
        'attempts': task['attempts'],
        'failure_reason': None,
        'rotate_proxy': False
    }
```

#### 5. SELLER_PROFILE / CATALOG / DETECTION_ERROR (неожиданные состояния)

```python
if state in [SELLER_PROFILE_DETECTOR_ID, CATALOG_DETECTOR_ID, 'detection_error']:
    # Логика из старого кода: проверяем повторную ошибку
    # (в новой системе не храним last_result в памяти, поэтому упрощаем)

    return {
        'item_id': task['item_id'],
        'status': 'error',
        'failure_reason': f'unexpected_state_{state}',
        'rotate_proxy': False  # Первая попытка - без rotate
    }
```

---

## 📊 Форматы данных

### 1. Task dict (из acquire_task)

```python
task = {
    'task_id': 1,              # BIGINT
    'item_id': 3895922522,     # BIGINT
    'attempts': 1              # INTEGER (текущее количество попыток)
}
```

### 2. Proxy dict (из acquire_proxy)

```python
proxy = {
    'proxy_id': 5,             # BIGINT
    'proxy': '178.250.190.177:3000:q5Wuid:1j8A4VJOZr'  # TEXT (host:port:user:pass)
}
```

### 3. Result dict (возвращаемый из process_item)

#### Success:

```python
{
    'item_id': 3895922522,
    'status': 'success',
    'title': 'BMW X5 2020',
    'description': 'Описание объявления...',
    'characteristics': {          # dict, не JSON string!
        'Год выпуска': '2020',
        'Пробег': '50000 км',
        # ...
    },
    'price': 3500000.00,          # Decimal
    'published_at': '15 октября',
    'seller_name': 'Иван Иванов',
    'seller_profile_url': 'https://www.avito.ru/user/...',
    'location_address': 'Москва',
    'location_metro': 'Арбатская',
    'location_region': 'Москва',
    'views_total': 123,
    'worker_id': 'zamer_avito_worker:hostname:12345:5',  # С индексом!
    'attempts': 1,
    'failure_reason': None,
    'rotate_proxy': False
}
```

#### Unavailable:

```python
{
    'item_id': 3895922522,
    'status': 'unavailable',
    'worker_id': 'zamer_avito_worker:hostname:12345:5',
    'attempts': 1,
    'failure_reason': None,
    'rotate_proxy': False,
    # Остальные поля None или отсутствуют
}
```

#### Error (НЕ сохраняется в БД, только для retry):

```python
{
    'item_id': 3895922522,
    'status': 'error',
    'failure_reason': 'proxy_blocked_http_403',  # или 'captcha_failed', 'parse_card_error'
    'rotate_proxy': True  # или False
}
```

---

## ⚠️ Обработка ошибок

### 1. Сохранение результатов

**Правило:** При ошибках результаты **НЕ СОХРАНЯЕМ** в таблицу `results`.

```python
# main.py
result = await process_item(page, task, proxy['proxy'], worker_id)

if result['status'] in ['success', 'unavailable']:
    # Только success/unavailable сохраняем в results
    await save_result_to_db(result)
    await mark_task_completed(task['task_id'])

elif result['status'] == 'error':
    # Ошибки НЕ сохраняем в results
    # Только увеличиваем счетчик попыток
    await increment_task_attempts(task['task_id'])
    # increment_task_attempts автоматически переведет в failed при attempts >= max_attempts
```

### 2. Счетчик попыток

Используем существующее поле `attempts` в таблице `tasks`:

```sql
-- increment_task_attempts() автоматически:
UPDATE tasks
SET status = CASE
    WHEN attempts >= max_attempts THEN 'failed'  -- Достигли лимита → failed навсегда
    ELSE 'pending'                                -- Иначе → retry
END,
worker_id = NULL
WHERE id=$1
```

### 3. Обработка "нет прокси"

**Стратегия:** Пауза очереди

```python
proxy = await acquire_proxy(worker_id)

if not proxy:
    # Проверяем: все прокси заблокированы?
    # (можно сделать запрос к БД или просто ждать)

    log_event("worker_no_proxy", extra={"worker_id": worker_id})

    # Ждем появления прокси
    await asyncio.sleep(30)  # Пауза 30 сек
    continue  # Повторяем цикл
```

### 4. Обработка "нет задач"

**Стратегия:** Выход из цикла

```python
task = await acquire_task(worker_id)

if not task:
    # Задач больше нет → завершаем воркер
    log_event("worker_no_tasks", extra={"worker_id": worker_id})
    break  # Выход из worker_loop
```

---

## 🖥️ Управление браузером

### Принцип: Долгоживущая страница

**Критически важно:** Браузер живет весь цикл воркера, пересоздается ТОЛЬКО при фатальных ошибках.

```python
async def worker_loop(worker_id, worker_index):
    # Инициализируем браузер ОДИН РАЗ
    playwright, browser, context, page = await init_playwright(worker_id, worker_index)

    while True:
        task = await acquire_task(worker_id)
        if not task:
            break

        proxy = await acquire_proxy(worker_id)
        if not proxy:
            await asyncio.sleep(30)
            continue

        try:
            # Обрабатываем задачу на ТОЙ ЖЕ странице
            result = await process_item(page, task, proxy['proxy'], worker_id)

            if result.get('rotate_proxy'):
                # ФАТАЛЬНАЯ ОШИБКА → пересоздаем браузер
                await mark_proxy_blocked(proxy['proxy'])
                await cleanup_playwright(playwright, browser, context, page)

                # Новый браузер с новым прокси
                playwright, browser, context, page = await init_playwright(worker_id, worker_index)

            # Обработка результата
            if result['status'] in ['success', 'unavailable']:
                await save_result_to_db(result)
                await mark_task_completed(task['task_id'])
            else:
                await increment_task_attempts(task['task_id'])

        finally:
            await release_proxy(proxy['proxy'])
            await asyncio.sleep(0)  # Уступить управление event loop
```

### Фатальные ошибки (rotate_proxy = True)

- `proxy_blocked_http_403` - HTTP 403
- `proxy_blocked_http_407` - HTTP 407 (PROXY_AUTH)
- `captcha_failed` - Капча не решилась после 3 попыток

### НЕ фатальные ошибки (продолжаем на той же странице)

- `parse_card_error` - Ошибка парсинга карточки
- `unexpected_state_*` - Неожиданное состояние детектора

---

## 💓 Heartbeat механизм

### В каждом worker_loop

```python
async def worker_loop(worker_id: str, worker_index: int):
    """Основной цикл обработки задач"""

    # Регистрируем воркер при старте
    await update_heartbeat(worker_id)

    last_heartbeat = time.time()

    # Инициализация браузера
    playwright, browser, context, page = await init_playwright(worker_id, worker_index)

    while True:
        # Обновляем heartbeat каждые 30 секунд
        if time.time() - last_heartbeat > config.HEARTBEAT_INTERVAL:
            await update_heartbeat(worker_id)
            last_heartbeat = time.time()

        # ... обработка задач
```

---

## 🔧 Алгоритмы функций

### processor.py

#### 1. process_item (главная функция)

```python
async def process_item(
    page: Page,
    task: dict,
    proxy_string: str,
    worker_id: str
) -> dict:
    """
    Обрабатывает одну задачу.

    Args:
        page: Playwright Page (долгоживущая страница)
        task: {'task_id': int, 'item_id': int, 'attempts': int}
        proxy_string: 'host:port:user:pass'
        worker_id: 'program_id:hostname:pid:index'

    Returns:
        dict с полями: item_id, status, ..., rotate_proxy
    """

    # 1. Формирование URL
    url = f"https://www.avito.ru/items/{task['item_id']}"

    # 2. Navigate + Detect
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    state = await detect_page_state(page, priority=PRIORITY_ORDER)

    log_event("worker_detect_state", item_id=task['item_id'], proxy=proxy_string, extra={"state": state})

    # 3. Обработка по state
    if state in [CAPTCHA_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID]:
        return await handle_captcha(page, task, proxy_string, worker_id)

    if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
        return handle_proxy_block(task, state)

    if state == CARD_FOUND_DETECTOR_ID:
        return await handle_card_found(page, task, worker_id)

    if state == REMOVED_DETECTOR_ID:
        return handle_removed(task, worker_id)

    if state in [SELLER_PROFILE_DETECTOR_ID, CATALOG_DETECTOR_ID]:
        return handle_unexpected(task, state)

    # Неизвестное состояние
    return {
        'item_id': task['item_id'],
        'status': 'error',
        'failure_reason': f'unknown_state_{state}',
        'rotate_proxy': False
    }
```

#### 2. handle_captcha

```python
async def handle_captcha(page: Page, task: dict, proxy_string: str, worker_id: str) -> dict:
    """Решает капчу и продолжает обработку"""

    html, solved = await resolve_captcha_flow(page, max_attempts=3)

    if not solved:
        log_event("captcha_failed", item_id=task['item_id'], proxy=proxy_string)
        return {
            'item_id': task['item_id'],
            'status': 'error',
            'failure_reason': 'captcha_failed',
            'rotate_proxy': True
        }

    # Капча решилась → повторный детект
    state = await detect_page_state(page, priority=PRIORITY_ORDER)
    log_event("captcha_resolved", item_id=task['item_id'], proxy=proxy_string, extra={"new_state": state})

    # Продолжаем обработку с новым state
    if state == CARD_FOUND_DETECTOR_ID:
        return await handle_card_found(page, task, worker_id)
    elif state == REMOVED_DETECTOR_ID:
        return handle_removed(task, worker_id)
    else:
        return {
            'item_id': task['item_id'],
            'status': 'error',
            'failure_reason': f'unexpected_after_captcha_{state}',
            'rotate_proxy': False
        }
```

#### 3. handle_card_found

```python
async def handle_card_found(page: Page, task: dict, worker_id: str) -> dict:
    """Парсит карточку объявления"""

    html = await page.content()

    try:
        card = parse_card(
            html,
            fields=(
                "title", "description", "characteristics", "price",
                "seller", "item_id", "published_at", "location", "views_total",
            )
        )
    except CardParsingError as exc:
        log_event("task_parse_error", item_id=task['item_id'], extra={"error": str(exc)})
        return {
            'item_id': task['item_id'],
            'status': 'error',
            'failure_reason': 'parse_card_error',
            'rotate_proxy': False
        }

    # Построение результата
    return build_result(task, card, status='success', worker_id=worker_id)
```

#### 4. build_result

```python
def build_result(task: dict, card: CardData, status: str, worker_id: str) -> dict:
    """Строит result dict для save_result_to_db"""

    location = card.location or {}
    seller = card.seller or {}

    return {
        'item_id': task['item_id'],
        'status': status,
        'title': card.title,
        'description': card.description,
        'characteristics': card.characteristics,  # dict, не JSON!
        'price': _normalize_price(card.price),
        'published_at': card.published_at,
        'seller_name': seller.get('name'),
        'seller_profile_url': seller.get('profile_url'),
        'location_address': location.get('address'),
        'location_metro': location.get('metro'),
        'location_region': location.get('region'),
        'views_total': _to_int(card.views_total),
        'worker_id': worker_id,
        'attempts': task['attempts'],
        'failure_reason': None,
        'rotate_proxy': False
    }
```

### main.py

#### 1. init_playwright

```python
async def init_playwright(worker_id: str, worker_index: int) -> tuple[Playwright, Browser, Context, Page]:
    """
    Инициализирует Playwright браузер с прокси.

    Args:
        worker_id: ID воркера (zamer_avito_worker:host:pid:index)
        worker_index: Индекс корутины (0-14)

    Returns:
        (playwright, browser, context, page)
    """

    # Получаем прокси
    proxy_data = await acquire_proxy(worker_id)
    if not proxy_data:
        raise RuntimeError("No proxy available")

    # Парсим прокси
    from .utils import parse_proxy
    proxy_config = parse_proxy(proxy_data['proxy'])

    # Настройка окружения
    display_value = f":{worker_index}"
    launch_env = os.environ.copy()
    launch_env["DISPLAY"] = display_value

    # Запуск Playwright
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        proxy=proxy_config,
        headless=False,  # КРИТИЧЕСКИ ВАЖНО!
        env=launch_env,
    )
    context = await browser.new_context()
    page = await context.new_page()

    log_event("worker_page_ready", extra={"worker_id": worker_id, "proxy": proxy_data['proxy'], "display": display_value})

    return playwright, browser, context, page
```

#### 2. worker_loop

```python
async def worker_loop(worker_id: str, worker_index: int):
    """Основной цикл обработки задач"""

    log_event("worker_start", extra={"worker_id": worker_id})

    # Регистрация воркера
    await update_heartbeat(worker_id)
    last_heartbeat = time.time()

    # Инициализация браузера
    playwright, browser, context, page = await init_playwright(worker_id, worker_index)

    while True:
        # Heartbeat каждые 30 сек
        if time.time() - last_heartbeat > config.HEARTBEAT_INTERVAL:
            await update_heartbeat(worker_id)
            last_heartbeat = time.time()

        # Получить задачу
        task = await acquire_task(worker_id)
        if not task:
            log_event("worker_no_tasks", extra={"worker_id": worker_id})
            break

        # Получить прокси
        proxy = await acquire_proxy(worker_id)
        if not proxy:
            log_event("worker_no_proxy", extra={"worker_id": worker_id})
            await asyncio.sleep(30)
            continue

        try:
            # Обработать задачу
            result = await process_item(page, task, proxy['proxy'], worker_id)

            # Rotate proxy если нужно
            if result.get('rotate_proxy'):
                await mark_proxy_blocked(proxy['proxy'])
                await cleanup_playwright(playwright, browser, context, page)
                playwright, browser, context, page = await init_playwright(worker_id, worker_index)

            # Сохранить результат
            if result['status'] in ['success', 'unavailable']:
                await save_result_to_db(result)
                await mark_task_completed(task['task_id'])
                await increment_worker_stats(worker_id, success=True)
            else:
                await increment_task_attempts(task['task_id'])
                await increment_worker_stats(worker_id, success=False)

        except Exception as exc:
            log_event("worker_error", item_id=task['item_id'], proxy=proxy['proxy'], extra={"error": str(exc)})
            await increment_task_attempts(task['task_id'])
        finally:
            await release_proxy(proxy['proxy'])
            await asyncio.sleep(0)

    # Cleanup при завершении
    await cleanup_playwright(playwright, browser, context, page)
    log_event("worker_shutdown", extra={"worker_id": worker_id})
```

#### 3. main()

```python
async def main():
    """Точка входа"""
    # Инициализация DB pool
    await init_pool()

    # Генерация worker_id для каждого воркера
    base_worker_id = config.get_worker_id(config.PROGRAM_ID)  # zamer_avito_worker:host:pid

    # Запуск 15 воркеров
    tasks = []
    for i in range(config.WORKERS_COUNT):
        worker_id = f"{base_worker_id}:{i}"  # Добавляем индекс
        tasks.append(worker_loop(worker_id, i))

    try:
        await asyncio.gather(*tasks)
    finally:
        await close_pool()
```

---

## 📝 Итоговая структура файлов

```
zamer_avito_system/worker/src/
├── config.py          # Уже создан
├── utils.py           # Уже создан
├── db.py              # Уже создан
├── logging_utils.py   # КОПИЯ из старого проекта
├── processor.py       # НОВЫЙ (логика обработки)
└── main.py            # НОВЫЙ (оркестрация)
```

---

## ✅ Чеклист реализации

- [ ] Скопировать `logging_utils.py`
- [ ] Создать `processor.py` с функциями:
  - [ ] `process_item()`
  - [ ] `handle_captcha()`
  - [ ] `handle_card_found()`
  - [ ] `handle_removed()`
  - [ ] `handle_proxy_block()`
  - [ ] `handle_unexpected()`
  - [ ] `build_result()`
  - [ ] `_normalize_price()`, `_to_int()`
- [ ] Создать `main.py` с функциями:
  - [ ] `init_playwright()`
  - [ ] `cleanup_playwright()`
  - [ ] `worker_loop()`
  - [ ] `main()`
- [ ] Валидировать Python синтаксис

---

**Конец спецификации**
