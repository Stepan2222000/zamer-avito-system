-- ============================================================================
-- init.sql - Схема базы данных для распределенной системы zamer_avito
-- ============================================================================
-- Дата: 01.11.2025
-- Описание: 4 таблицы + 6 индексов для координации распределенных воркеров
-- PostgreSQL: 81.30.105.134:5413, Database: zamer_avito_system
-- ============================================================================

-- ============================================================================
-- 1. TASKS - Очередь задач для парсинга
-- ============================================================================
-- Назначение: Хранит item_id объявлений Avito, которые нужно спарсить
-- Механизм: FOR UPDATE SKIP LOCKED для атомарного захвата задач воркерами
-- Статусы:
--   - pending: готова к обработке
--   - processing: захвачена воркером
--   - completed: успешно обработана
--   - failed: исчерпаны все попытки (attempts >= max_attempts)
-- ============================================================================

CREATE TABLE IF NOT EXISTS tasks (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL UNIQUE,                    -- ID объявления на Avito
    status TEXT NOT NULL DEFAULT 'pending',            -- pending/processing/completed/failed
    worker_id TEXT,                                     -- ID воркера, обрабатывающего задачу
    attempts INTEGER NOT NULL DEFAULT 0,               -- Количество попыток обработки
    max_attempts INTEGER NOT NULL DEFAULT 5,           -- Максимальное количество попыток

    -- Временные метки
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- Когда задача добавлена
    last_attempt_at TIMESTAMPTZ,                       -- Последняя попытка обработки
    completed_at TIMESTAMPTZ,                          -- Когда успешно завершена

    -- Ограничения
    CONSTRAINT tasks_status_check CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Комментарии
COMMENT ON TABLE tasks IS 'Очередь задач для парсинга объявлений Avito';
COMMENT ON COLUMN tasks.item_id IS 'Уникальный ID объявления на Avito';
COMMENT ON COLUMN tasks.status IS 'Статус задачи: pending (готова), processing (в работе), completed (выполнена), failed (провалена)';
COMMENT ON COLUMN tasks.worker_id IS 'ID воркера в формате program_id:hostname:pid';
COMMENT ON COLUMN tasks.attempts IS 'Текущее количество попыток обработки';


-- ============================================================================
-- 2. PROXIES - Пул прокси-серверов
-- ============================================================================
-- Назначение: Хранит прокси для ротации при парсинге
-- Механизм: FOR UPDATE SKIP LOCKED для атомарного захвата прокси воркерами
-- Статусы:
--   - available: свободен для использования
--   - locked: захвачен воркером
--   - blocked: заблокирован (blocks_count >= 3)
-- Формат proxy: host:port:user:pass
-- ============================================================================

CREATE TABLE IF NOT EXISTS proxies (
    id BIGSERIAL PRIMARY KEY,
    proxy TEXT NOT NULL UNIQUE,                        -- host:port:user:pass
    status TEXT NOT NULL DEFAULT 'available',          -- available/locked/blocked

    -- Информация о блокировке
    locked_by TEXT,                                     -- ID воркера, захватившего прокси
    locked_at TIMESTAMPTZ,                             -- Когда захвачен

    -- Статистика использования
    uses_count INTEGER NOT NULL DEFAULT 0,             -- Количество использований
    blocks_count INTEGER NOT NULL DEFAULT 0,           -- Количество блокировок (403/407/429)
    last_used_at TIMESTAMPTZ,                          -- Последнее использование

    -- Временные метки
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ограничения
    CONSTRAINT proxies_status_check CHECK (status IN ('available', 'locked', 'blocked'))
);

-- Комментарии
COMMENT ON TABLE proxies IS 'Пул прокси-серверов для ротации при парсинге';
COMMENT ON COLUMN proxies.proxy IS 'Прокси в формате host:port:user:pass';
COMMENT ON COLUMN proxies.status IS 'Статус: available (доступен), locked (захвачен), blocked (заблокирован)';
COMMENT ON COLUMN proxies.uses_count IS 'Счетчик использований для равномерной ротации';
COMMENT ON COLUMN proxies.blocks_count IS 'Счетчик блокировок (автоблокировка при >= 3)';


-- ============================================================================
-- 3. WORKERS - Регистрация воркеров
-- ============================================================================
-- Назначение: Отслеживание активных воркеров через heartbeat механизм
-- Heartbeat: Каждый воркер обновляет last_heartbeat каждые 30 секунд
-- Мертвые воркеры: last_heartbeat > 4 минут → cleanup помечает как stopped
-- worker_id формат: program_id:hostname:pid (например: zamer_avito:server1:12345)
-- ============================================================================

CREATE TABLE IF NOT EXISTS workers (
    id BIGSERIAL PRIMARY KEY,
    worker_id TEXT NOT NULL UNIQUE,                    -- program_id:hostname:pid
    status TEXT NOT NULL DEFAULT 'active',             -- active/stopped

    -- Статистика
    tasks_processed INTEGER NOT NULL DEFAULT 0,        -- Успешно обработано задач
    tasks_failed INTEGER NOT NULL DEFAULT 0,           -- Провалено задач

    -- Временные метки
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- Когда воркер запустился
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Последний heartbeat

    -- Ограничения
    CONSTRAINT workers_status_check CHECK (status IN ('active', 'stopped'))
);

-- Комментарии
COMMENT ON TABLE workers IS 'Регистрация и мониторинг активных воркеров';
COMMENT ON COLUMN workers.worker_id IS 'Уникальный ID воркера: program_id:hostname:pid';
COMMENT ON COLUMN workers.last_heartbeat IS 'Последний heartbeat (обновляется каждые 30 сек)';
COMMENT ON COLUMN workers.tasks_processed IS 'Количество успешно обработанных задач';


-- ============================================================================
-- 4. RESULTS - Результаты парсинга
-- ============================================================================
-- Назначение: Хранит спарсенные данные объявлений Avito
-- Источник структуры: src/db.py, класс ListingRecord (копия 1:1)
-- Идемпотентность: ON CONFLICT (item_id) DO UPDATE для защиты от дубликатов
-- Статусы:
--   - success: объявление успешно спарсено
--   - unavailable: объявление удалено/недоступно
--   - error: ошибка при парсинге
-- ============================================================================

CREATE TABLE IF NOT EXISTS results (
    -- Основная информация объявления
    item_id BIGINT PRIMARY KEY,                        -- ID объявления (PK)
    title TEXT,                                         -- Название объявления
    description TEXT,                                   -- Полное описание
    characteristics JSONB,                              -- Характеристики товара (JSON)
    price NUMERIC(12, 2),                              -- Цена (поддержка копеек)
    published_at TEXT,                                  -- Дата публикации (как на Avito)

    -- Информация о продавце
    seller_name TEXT,                                   -- Имя/никнейм продавца
    seller_profile_url TEXT,                            -- URL профиля продавца

    -- Географическая информация
    location_address TEXT,                              -- Адрес (город, улица)
    location_metro TEXT,                                -- Ближайшая станция метро
    location_region TEXT,                               -- Регион

    -- Статистика
    views_total INTEGER,                                -- Общее количество просмотров

    -- Статус обработки
    status TEXT NOT NULL,                               -- success/unavailable/error
    failure_reason TEXT,                                -- Причина ошибки (если status='error')

    -- Мета-информация для распределенной системы
    worker_id TEXT,                                     -- Какой воркер обработал
    attempts INTEGER NOT NULL DEFAULT 1,               -- Сколько попыток потребовалось

    -- Временные метки
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),   -- Когда обработана
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- Время создания записи
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- Время последнего обновления

    -- Ограничения
    CONSTRAINT results_status_check CHECK (status IN ('success', 'unavailable', 'error'))
);

-- Комментарии
COMMENT ON TABLE results IS 'Результаты парсинга объявлений Avito';
COMMENT ON COLUMN results.item_id IS 'ID объявления (PRIMARY KEY для идемпотентности)';
COMMENT ON COLUMN results.status IS 'Статус: success (успех), unavailable (недоступно), error (ошибка)';
COMMENT ON COLUMN results.characteristics IS 'Характеристики товара в формате JSON';
COMMENT ON COLUMN results.worker_id IS 'ID воркера, который обработал задачу';


-- ============================================================================
-- ИНДЕКСЫ - Оптимизация для FOR UPDATE SKIP LOCKED
-- ============================================================================
-- Критически важны для производительности атомарных операций захвата задач/прокси
-- Без индексов FOR UPDATE SKIP LOCKED работает медленно при большом количестве задач
-- ============================================================================

-- Индекс 1: Захват pending задач (сортировка по created_at для FIFO)
CREATE INDEX IF NOT EXISTS idx_tasks_pending
ON tasks(status, created_at)
WHERE status = 'pending';

COMMENT ON INDEX idx_tasks_pending IS 'Оптимизация захвата pending задач (FIFO очередь)';


-- Индекс 2: Освобождение зависших processing задач (cleanup сервис)
CREATE INDEX IF NOT EXISTS idx_tasks_processing
ON tasks(status, last_attempt_at)
WHERE status = 'processing';

COMMENT ON INDEX idx_tasks_processing IS 'Оптимизация поиска зависших задач (cleanup каждые 60 сек)';


-- Индекс 3: Захват available прокси (сортировка по uses_count для равномерной ротации)
CREATE INDEX IF NOT EXISTS idx_proxies_available
ON proxies(status, uses_count)
WHERE status = 'available';

COMMENT ON INDEX idx_proxies_available IS 'Оптимизация захвата available прокси (равномерная ротация)';


-- Индекс 4: Освобождение зависших locked прокси (cleanup сервис)
CREATE INDEX IF NOT EXISTS idx_proxies_locked
ON proxies(status, locked_at)
WHERE status = 'locked';

COMMENT ON INDEX idx_proxies_locked IS 'Оптимизация поиска зависших прокси (cleanup каждые 60 сек)';


-- Индекс 5: Поиск мертвых воркеров (cleanup сервис)
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat
ON workers(last_heartbeat);

COMMENT ON INDEX idx_workers_heartbeat IS 'Оптимизация поиска мертвых воркеров (heartbeat > 4 минут)';


-- Индекс 6: Выборка результатов по статусу и времени обработки
CREATE INDEX IF NOT EXISTS idx_results_status_processed
ON results(status, processed_at);

COMMENT ON INDEX idx_results_status_processed IS 'Оптимизация выборки результатов для анализа';


-- ============================================================================
-- ЗАВЕРШЕНИЕ
-- ============================================================================

-- Выводим статистику созданных объектов
DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'База данных инициализирована успешно!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Таблицы созданы: tasks, proxies, workers, results';
    RAISE NOTICE 'Индексы созданы: 6 индексов для оптимизации FOR UPDATE SKIP LOCKED';
    RAISE NOTICE '============================================================================';
END $$;
