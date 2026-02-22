-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 5. Специализированное моделирование данных
-- Скрипт 1: Создание схемы и таблиц для временных рядов
-- Предприятие: "Руда+" — добыча железной руды
--
-- Два варианта:
--   A) С расширением TimescaleDB (гипертаблицы)
--   B) Без TimescaleDB (стандартное секционирование PostgreSQL)
--
-- ВАЖНО: Убедитесь, что скрипты модулей 1–4 уже выполнены.
-- ============================================================

-- ============================================================
-- Шаг 1: Создание схемы timeseries
-- ============================================================

CREATE SCHEMA IF NOT EXISTS timeseries;
SET search_path TO timeseries, public;

SELECT '--- Схема timeseries создана ---' AS info;

-- ============================================================
-- Шаг 2: Определяем, доступен ли TimescaleDB
-- ============================================================

-- Попытка подключить расширение.
-- Если TimescaleDB недоступен, команда завершится с ошибкой —
-- в этом случае перейдите к разделу B (секционирование).

-- Раскомментируйте следующую строку, если TimescaleDB установлен:
-- CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Проверка наличия расширения:
SELECT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
) AS timescaledb_available;

-- ============================================================
-- ============================================================
-- ВАРИАНТ A: С TimescaleDB (гипертаблицы)
-- ============================================================
-- ============================================================

-- Если TimescaleDB доступен, выполните этот раздел.
-- Если нет — перейдите к ВАРИАНТУ B ниже.

-- ============================================================
-- Шаг A.1: Основная таблица показаний датчиков (гипертаблица)
-- ============================================================

-- Удаляем предыдущие объекты (если перезапуск)
DROP TABLE IF EXISTS timeseries.sensor_readings CASCADE;

CREATE TABLE timeseries.sensor_readings (
    -- Время показания (основной столбец секционирования)
    reading_time    TIMESTAMPTZ     NOT NULL,
    -- Идентификатор оборудования (EQ-001..EQ-012)
    equipment_id    VARCHAR(20)     NOT NULL,
    -- Тип датчика (temperature, vibration, pressure, speed, fuel_level)
    sensor_type     VARCHAR(30)     NOT NULL,
    -- Значение показания
    value           DOUBLE PRECISION NOT NULL,
    -- Единица измерения (celsius, mm_s, bar, km_h, percent)
    unit            VARCHAR(20)     NOT NULL,
    -- Качество показания (good, suspect, bad)
    quality         VARCHAR(10)     DEFAULT 'good'
);

-- Комментарии к таблице
COMMENT ON TABLE timeseries.sensor_readings
    IS 'Показания датчиков оборудования «Руда+» (гипертаблица TimescaleDB)';
COMMENT ON COLUMN timeseries.sensor_readings.reading_time
    IS 'Временная метка показания (с часовым поясом)';
COMMENT ON COLUMN timeseries.sensor_readings.equipment_id
    IS 'Идентификатор оборудования (FK к ruda_plus.equipment)';
COMMENT ON COLUMN timeseries.sensor_readings.sensor_type
    IS 'Тип датчика: temperature, vibration, pressure, speed, fuel_level';
COMMENT ON COLUMN timeseries.sensor_readings.value
    IS 'Числовое значение показания';
COMMENT ON COLUMN timeseries.sensor_readings.unit
    IS 'Единица измерения: celsius, mm_s, bar, km_h, percent';
COMMENT ON COLUMN timeseries.sensor_readings.quality
    IS 'Качество данных: good (норма), suspect (подозрительное), bad (некорректное)';

-- Преобразуем в гипертаблицу с чанками по 7 дней
-- TimescaleDB автоматически создаёт секции (chunks) и индексы
SELECT create_hypertable(
    'timeseries.sensor_readings',
    'reading_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Дополнительный индекс для быстрого поиска по оборудованию и типу датчика
CREATE INDEX IF NOT EXISTS idx_sensor_readings_equip_type
    ON timeseries.sensor_readings (equipment_id, sensor_type, reading_time DESC);

SELECT '--- Гипертаблица sensor_readings создана ---' AS info;

-- ============================================================
-- Шаг A.2: Таблица часовых метрик оборудования
-- ============================================================

DROP TABLE IF EXISTS timeseries.equipment_metrics_hourly CASCADE;

CREATE TABLE timeseries.equipment_metrics_hourly (
    -- Начало часового интервала
    hour_start      TIMESTAMPTZ     NOT NULL,
    -- Идентификатор оборудования
    equipment_id    VARCHAR(20)     NOT NULL,
    -- Тип датчика
    sensor_type     VARCHAR(30)     NOT NULL,
    -- Агрегированные метрики
    avg_value       DOUBLE PRECISION,
    min_value       DOUBLE PRECISION,
    max_value       DOUBLE PRECISION,
    stddev_value    DOUBLE PRECISION,
    reading_count   INTEGER         DEFAULT 0,
    -- Количество аномальных показаний за час
    anomaly_count   INTEGER         DEFAULT 0,
    -- Первичный ключ
    PRIMARY KEY (hour_start, equipment_id, sensor_type)
);

COMMENT ON TABLE timeseries.equipment_metrics_hourly
    IS 'Предрассчитанные часовые агрегаты показаний датчиков';

-- Преобразуем в гипертаблицу
SELECT create_hypertable(
    'timeseries.equipment_metrics_hourly',
    'hour_start',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

SELECT '--- Гипертаблица equipment_metrics_hourly создана ---' AS info;

-- ============================================================
-- Шаг A.3: Таблица добычи во времени (production timeseries)
-- ============================================================

DROP TABLE IF EXISTS timeseries.production_timeseries CASCADE;

CREATE TABLE timeseries.production_timeseries (
    -- Временная метка (начало смены / часа)
    production_time TIMESTAMPTZ     NOT NULL,
    -- Идентификатор шахты
    mine_id         VARCHAR(20)     NOT NULL,
    -- Идентификатор оборудования (ПДМ или самосвал)
    equipment_id    VARCHAR(20)     NOT NULL,
    -- Идентификатор оператора
    operator_id     VARCHAR(20),
    -- Тонны добытой руды за интервал
    tonnage         DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- Содержание железа (%)
    fe_content      DOUBLE PRECISION,
    -- Влажность (%)
    moisture        DOUBLE PRECISION,
    -- Тип руды
    ore_type        VARCHAR(50),
    -- Горизонт добычи
    horizon         VARCHAR(50)
);

COMMENT ON TABLE timeseries.production_timeseries
    IS 'Временной ряд добычи руды по сменам/часам';

-- Преобразуем в гипертаблицу
SELECT create_hypertable(
    'timeseries.production_timeseries',
    'production_time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_prod_ts_mine
    ON timeseries.production_timeseries (mine_id, production_time DESC);

SELECT '--- Гипертаблица production_timeseries создана ---' AS info;

-- ============================================================
-- Шаг A.4: Таблица алертов (сработавшие пороги)
-- ============================================================

DROP TABLE IF EXISTS timeseries.alerts CASCADE;

CREATE TABLE timeseries.alerts (
    alert_id        SERIAL          PRIMARY KEY,
    -- Когда сработал алерт
    alert_time      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Какое оборудование
    equipment_id    VARCHAR(20)     NOT NULL,
    -- Какой датчик
    sensor_type     VARCHAR(30)     NOT NULL,
    -- Уровень: warning, critical, emergency
    severity        VARCHAR(20)     NOT NULL,
    -- Значение, вызвавшее алерт
    trigger_value   DOUBLE PRECISION NOT NULL,
    -- Порог, который был превышен
    threshold       DOUBLE PRECISION NOT NULL,
    -- Описание
    message         TEXT,
    -- Подтверждён ли оператором
    acknowledged    BOOLEAN         DEFAULT FALSE,
    acknowledged_by VARCHAR(20),
    acknowledged_at TIMESTAMPTZ
);

COMMENT ON TABLE timeseries.alerts
    IS 'Журнал сработавших алертов датчиков';

CREATE INDEX IF NOT EXISTS idx_alerts_time
    ON timeseries.alerts (alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_equip
    ON timeseries.alerts (equipment_id, alert_time DESC);

SELECT '--- Таблица alerts создана ---' AS info;

-- ============================================================
-- Шаг A.5: Настройка сжатия (compression)
-- ============================================================

-- Включаем сжатие для sensor_readings
-- Сегментирование по equipment_id и sensor_type означает,
-- что при запросе по конкретному оборудованию будут
-- распаковываться только нужные сегменты.

ALTER TABLE timeseries.sensor_readings
SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'equipment_id, sensor_type',
    timescaledb.compress_orderby = 'reading_time'
);

-- Автоматическое сжатие чанков старше 7 дней
-- (в учебных целях — в продакшн обычно 30+ дней)
SELECT add_compression_policy(
    'timeseries.sensor_readings',
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT '--- Политика сжатия настроена ---' AS info;

-- ============================================================
-- Шаг A.6: Проверка созданных объектов
-- ============================================================

-- Все таблицы в схеме timeseries
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'timeseries' AND c.table_name = t.table_name) AS columns
FROM information_schema.tables t
WHERE table_schema = 'timeseries'
ORDER BY table_name;

-- Информация о гипертаблицах
SELECT hypertable_schema, hypertable_name,
       num_dimensions, num_chunks,
       compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'timeseries';

SELECT '=== ВАРИАНТ A (TimescaleDB) завершён ===' AS info;

-- ============================================================
-- ============================================================
-- ВАРИАНТ B: Без TimescaleDB (стандартное секционирование)
-- ============================================================
-- ============================================================

-- ВНИМАНИЕ: Выполняйте этот раздел ТОЛЬКО если TimescaleDB
-- недоступен. Если вы уже выполнили вариант A — пропустите.

-- Раскомментируйте и выполните блоки ниже:

/*

-- ============================================================
-- Шаг B.1: Секционированная таблица показаний датчиков
-- ============================================================

DROP TABLE IF EXISTS timeseries.sensor_readings CASCADE;

-- Создаём секционированную таблицу (PARTITION BY RANGE)
CREATE TABLE timeseries.sensor_readings (
    reading_time    TIMESTAMPTZ     NOT NULL,
    equipment_id    VARCHAR(20)     NOT NULL,
    sensor_type     VARCHAR(30)     NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    unit            VARCHAR(20)     NOT NULL,
    quality         VARCHAR(10)     DEFAULT 'good'
) PARTITION BY RANGE (reading_time);

COMMENT ON TABLE timeseries.sensor_readings
    IS 'Показания датчиков «Руда+» (секционированная таблица PostgreSQL)';

-- Создаём секции помесячно (март–июнь 2025 для учебных целей)
CREATE TABLE timeseries.sensor_readings_2025_03
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE timeseries.sensor_readings_2025_04
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE timeseries.sensor_readings_2025_05
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE timeseries.sensor_readings_2025_06
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

-- Индексы на каждой секции создаются автоматически при наследовании,
-- но добавим составной индекс на родительской таблице:
CREATE INDEX IF NOT EXISTS idx_sensor_readings_equip_type_fb
    ON timeseries.sensor_readings (equipment_id, sensor_type, reading_time DESC);

SELECT '--- Секционированная таблица sensor_readings создана ---' AS info;

-- ============================================================
-- Шаг B.2: Таблица часовых метрик (обычная)
-- ============================================================

DROP TABLE IF EXISTS timeseries.equipment_metrics_hourly CASCADE;

CREATE TABLE timeseries.equipment_metrics_hourly (
    hour_start      TIMESTAMPTZ     NOT NULL,
    equipment_id    VARCHAR(20)     NOT NULL,
    sensor_type     VARCHAR(30)     NOT NULL,
    avg_value       DOUBLE PRECISION,
    min_value       DOUBLE PRECISION,
    max_value       DOUBLE PRECISION,
    stddev_value    DOUBLE PRECISION,
    reading_count   INTEGER         DEFAULT 0,
    anomaly_count   INTEGER         DEFAULT 0,
    PRIMARY KEY (hour_start, equipment_id, sensor_type)
);

COMMENT ON TABLE timeseries.equipment_metrics_hourly
    IS 'Часовые агрегаты показаний датчиков (без TimescaleDB)';

SELECT '--- Таблица equipment_metrics_hourly создана ---' AS info;

-- ============================================================
-- Шаг B.3: Таблица добычи (секционированная)
-- ============================================================

DROP TABLE IF EXISTS timeseries.production_timeseries CASCADE;

CREATE TABLE timeseries.production_timeseries (
    production_time TIMESTAMPTZ     NOT NULL,
    mine_id         VARCHAR(20)     NOT NULL,
    equipment_id    VARCHAR(20)     NOT NULL,
    operator_id     VARCHAR(20),
    tonnage         DOUBLE PRECISION NOT NULL DEFAULT 0,
    fe_content      DOUBLE PRECISION,
    moisture        DOUBLE PRECISION,
    ore_type        VARCHAR(50),
    horizon         VARCHAR(50)
) PARTITION BY RANGE (production_time);

CREATE TABLE timeseries.production_timeseries_2025_03
    PARTITION OF timeseries.production_timeseries
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE timeseries.production_timeseries_2025_04
    PARTITION OF timeseries.production_timeseries
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

COMMENT ON TABLE timeseries.production_timeseries
    IS 'Временной ряд добычи руды (секционированная таблица)';

CREATE INDEX IF NOT EXISTS idx_prod_ts_mine_fb
    ON timeseries.production_timeseries (mine_id, production_time DESC);

SELECT '--- Секционированная таблица production_timeseries создана ---' AS info;

-- ============================================================
-- Шаг B.4: Таблица алертов (такая же как в варианте A)
-- ============================================================

DROP TABLE IF EXISTS timeseries.alerts CASCADE;

CREATE TABLE timeseries.alerts (
    alert_id        SERIAL          PRIMARY KEY,
    alert_time      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    equipment_id    VARCHAR(20)     NOT NULL,
    sensor_type     VARCHAR(30)     NOT NULL,
    severity        VARCHAR(20)     NOT NULL,
    trigger_value   DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    message         TEXT,
    acknowledged    BOOLEAN         DEFAULT FALSE,
    acknowledged_by VARCHAR(20),
    acknowledged_at TIMESTAMPTZ
);

COMMENT ON TABLE timeseries.alerts
    IS 'Журнал сработавших алертов датчиков';

CREATE INDEX IF NOT EXISTS idx_alerts_time_fb
    ON timeseries.alerts (alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_equip_fb
    ON timeseries.alerts (equipment_id, alert_time DESC);

SELECT '--- Таблица alerts создана ---' AS info;

-- ============================================================
-- Шаг B.5: Проверка созданных объектов
-- ============================================================

-- Все таблицы в схеме timeseries (включая секции)
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'timeseries' AND c.table_name = t.table_name) AS columns
FROM information_schema.tables t
WHERE table_schema = 'timeseries'
ORDER BY table_name;

-- Проверка секций
SELECT parent.relname AS parent_table,
       child.relname AS partition,
       pg_get_expr(child.relpartbound, child.oid) AS bounds
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'timeseries')
ORDER BY parent.relname, child.relname;

SELECT '=== ВАРИАНТ B (без TimescaleDB) завершён ===' AS info;

*/
