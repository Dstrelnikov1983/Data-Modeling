-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 4. Моделирование потоковых и пакетных данных
-- Скрипт 4: Моделирование потоковой обработки данных
-- Предприятие: "Руда+" — добыча железной руды
--
-- PostgreSQL НЕ является потоковой системой. Мы МОДЕЛИРУЕМ
-- потоковую обработку, чтобы понять ключевые концепции:
-- - Event-driven архитектура
-- - Landing Zone (посадочная зона)
-- - Оконные агрегации (Tumbling, Sliding, Session Windows)
-- - Обнаружение аномалий
-- - Пороговые алерты
--
-- В реальной системе "Руда+" для потоковой обработки
-- используются: Apache Kafka, Flink, Yandex Data Streams
-- ============================================================

-- ============================================================
-- Раздел 1: Создание схемы потоковой обработки
-- ============================================================

CREATE SCHEMA IF NOT EXISTS streaming;
SET search_path TO streaming, ruda_plus, public;

SELECT '=== Раздел 1: Создание схемы streaming ===' AS info;

-- ============================================================
-- 1.1 Посадочная зона: события датчиков (raw_sensor_events)
-- Основная таблица для потоковой телеметрии
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.raw_sensor_events (
    event_id        VARCHAR(20) PRIMARY KEY,            -- Уникальный ID события
    event_timestamp TIMESTAMP(3) NOT NULL,              -- Время события (мс точность)
    equipment_id    VARCHAR(10) NOT NULL,                -- ID оборудования
    event_type      VARCHAR(30) NOT NULL,                -- Тип датчика: temperature, vibration, pressure, speed, fuel_level
    sensor_value    NUMERIC(10,2) NOT NULL,              -- Показание датчика
    unit            VARCHAR(20) NOT NULL,                -- Единица измерения
    quality_flag    VARCHAR(10) NOT NULL DEFAULT 'good', -- Качество: good, suspect, bad
    payload         JSONB,                               -- Дополнительные данные в JSON
    -- Метаданные потоковой обработки
    ingestion_time  TIMESTAMP(3) NOT NULL DEFAULT NOW(), -- Время поступления в систему
    processing_time TIMESTAMP(3),                        -- Время обработки
    partition_key   VARCHAR(50)                           -- Ключ партиционирования (для Kafka)
);

COMMENT ON TABLE streaming.raw_sensor_events IS
    'Посадочная зона: сырые события телеметрии датчиков шахтного оборудования';

-- Индексы для быстрого доступа по типичным запросам потоковой обработки
CREATE INDEX IF NOT EXISTS idx_sensor_evt_time
    ON streaming.raw_sensor_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_evt_equip_time
    ON streaming.raw_sensor_events(equipment_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_evt_type_time
    ON streaming.raw_sensor_events(event_type, event_timestamp);

-- ============================================================
-- 1.2 Посадочная зона: события оборудования (raw_equipment_events)
-- Старт/стоп, смена режима, ошибки
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.raw_equipment_events (
    event_id        VARCHAR(20) PRIMARY KEY,
    event_timestamp TIMESTAMP(3) NOT NULL,
    equipment_id    VARCHAR(10) NOT NULL,
    event_type      VARCHAR(30) NOT NULL,                -- start, stop, mode_change, error, maintenance_due
    previous_state  VARCHAR(30),                          -- Предыдущее состояние
    new_state       VARCHAR(30),                          -- Новое состояние
    payload         JSONB,                               -- Дополнительные данные
    operator_id     VARCHAR(10),                          -- Оператор (если применимо)
    ingestion_time  TIMESTAMP(3) NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE streaming.raw_equipment_events IS
    'Посадочная зона: события жизненного цикла оборудования (старт, стоп, ошибки)';

CREATE INDEX IF NOT EXISTS idx_equip_evt_time
    ON streaming.raw_equipment_events(equipment_id, event_timestamp);

-- ============================================================
-- 1.3 Посадочная зона: события навигации (raw_navigation_events)
-- GPS-координаты машин в шахте
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.raw_navigation_events (
    event_id        VARCHAR(20) PRIMARY KEY,
    event_timestamp TIMESTAMP(3) NOT NULL,
    equipment_id    VARCHAR(10) NOT NULL,
    x_coord         NUMERIC(10,3) NOT NULL,              -- Координата X (метры)
    y_coord         NUMERIC(10,3) NOT NULL,              -- Координата Y (метры)
    z_coord         NUMERIC(10,3) NOT NULL,              -- Глубина (метры, отрицательная)
    heading         NUMERIC(5,1),                        -- Направление (градусы, 0-360)
    speed_kmh       NUMERIC(5,1),                        -- Скорость (км/ч)
    zone_id         VARCHAR(20),                          -- Зона в шахте
    horizon_id      VARCHAR(10),                          -- Горизонт
    payload         JSONB,
    ingestion_time  TIMESTAMP(3) NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE streaming.raw_navigation_events IS
    'Посадочная зона: события подземной навигационной системы (GPS)';

CREATE INDEX IF NOT EXISTS idx_nav_evt_time
    ON streaming.raw_navigation_events(equipment_id, event_timestamp);

-- ============================================================
-- 1.4 Таблица алертов
-- Генерируется на основе правил обработки потоковых данных
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.sensor_alerts (
    alert_id        SERIAL PRIMARY KEY,
    equipment_id    VARCHAR(10) NOT NULL,
    event_type      VARCHAR(30) NOT NULL,                -- Тип датчика, вызвавшего алерт
    alert_level     VARCHAR(20) NOT NULL                  -- warning, critical, emergency
                    CHECK (alert_level IN ('warning', 'critical', 'emergency')),
    alert_message   TEXT NOT NULL,                        -- Описание алерта
    sensor_value    NUMERIC(10,2),                       -- Значение, вызвавшее алерт
    threshold_value NUMERIC(10,2),                       -- Пороговое значение
    event_timestamp TIMESTAMP(3) NOT NULL,               -- Время исходного события
    created_at      TIMESTAMP(3) NOT NULL DEFAULT NOW(), -- Время создания алерта
    acknowledged_at TIMESTAMP(3),                        -- Время подтверждения оператором
    acknowledged_by VARCHAR(10),                          -- ID оператора, подтвердившего алерт
    resolved_at     TIMESTAMP(3),                        -- Время разрешения
    source_event_id VARCHAR(20)                           -- Ссылка на исходное событие
);

COMMENT ON TABLE streaming.sensor_alerts IS
    'Алерты: автоматически генерируемые предупреждения на основе пороговых правил';

CREATE INDEX IF NOT EXISTS idx_alerts_equip
    ON streaming.sensor_alerts(equipment_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_level
    ON streaming.sensor_alerts(alert_level, created_at);

-- ============================================================
-- 1.5 Таблица оконных агрегаций (материализованные результаты)
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.window_aggregations (
    agg_id          SERIAL PRIMARY KEY,
    equipment_id    VARCHAR(10) NOT NULL,
    event_type      VARCHAR(30) NOT NULL,
    window_type     VARCHAR(20) NOT NULL                  -- tumbling_5min, tumbling_1h, sliding_10min
                    CHECK (window_type IN ('tumbling_5min', 'tumbling_1h', 'sliding_10min')),
    window_start    TIMESTAMP(3) NOT NULL,
    window_end      TIMESTAMP(3) NOT NULL,
    reading_count   INTEGER NOT NULL DEFAULT 0,
    avg_value       NUMERIC(10,2),
    min_value       NUMERIC(10,2),
    max_value       NUMERIC(10,2),
    stddev_value    NUMERIC(10,2),
    sum_value       NUMERIC(12,2),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (equipment_id, event_type, window_type, window_start)
);

COMMENT ON TABLE streaming.window_aggregations IS
    'Материализованные оконные агрегации: результаты Tumbling/Sliding Windows';

CREATE INDEX IF NOT EXISTS idx_window_agg_lookup
    ON streaming.window_aggregations(equipment_id, event_type, window_start);

-- ============================================================
-- 1.6 Таблица пороговых правил (конфигурация алертов)
-- ============================================================

CREATE TABLE IF NOT EXISTS streaming.alert_rules (
    rule_id         SERIAL PRIMARY KEY,
    event_type      VARCHAR(30) NOT NULL,                -- Тип датчика
    alert_level     VARCHAR(20) NOT NULL,                -- warning / critical
    condition_type  VARCHAR(20) NOT NULL                  -- gt (>), lt (<), range
                    CHECK (condition_type IN ('gt', 'lt', 'range')),
    threshold_low   NUMERIC(10,2),                       -- Нижний порог (для lt и range)
    threshold_high  NUMERIC(10,2),                       -- Верхний порог (для gt и range)
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE streaming.alert_rules IS
    'Правила алертов: конфигурация пороговых значений для каждого типа датчика';

-- Заполняем правила алертов для оборудования "Руда+"
INSERT INTO streaming.alert_rules (event_type, alert_level, condition_type, threshold_low, threshold_high, description)
VALUES
    -- Температура (celsius)
    ('temperature', 'warning',  'gt', NULL, 85.0,   'Температура выше 85°C — предупреждение'),
    ('temperature', 'critical', 'gt', NULL, 95.0,   'Температура выше 95°C — критическая'),
    -- Вибрация (mm/s)
    ('vibration',   'warning',  'gt', NULL, 4.5,    'Вибрация выше 4.5 мм/с — предупреждение'),
    ('vibration',   'critical', 'gt', NULL, 7.0,    'Вибрация выше 7.0 мм/с — критическая'),
    -- Давление (bar)
    ('pressure',    'warning',  'lt', 2.0, NULL,    'Давление ниже 2.0 бар — предупреждение'),
    ('pressure',    'warning',  'gt', NULL, 8.0,    'Давление выше 8.0 бар — предупреждение'),
    ('pressure',    'critical', 'lt', 1.0, NULL,    'Давление ниже 1.0 бар — критическое'),
    ('pressure',    'critical', 'gt', NULL, 10.0,   'Давление выше 10.0 бар — критическое'),
    -- Скорость (km/h) — подземная ограничена
    ('speed',       'warning',  'gt', NULL, 12.0,   'Скорость выше 12 км/ч — предупреждение (шахта)'),
    ('speed',       'critical', 'gt', NULL, 15.0,   'Скорость выше 15 км/ч — критическая (шахта)'),
    -- Уровень топлива (percent)
    ('fuel_level',  'warning',  'lt', 15.0, NULL,   'Уровень топлива ниже 15% — предупреждение'),
    ('fuel_level',  'critical', 'lt', 5.0,  NULL,   'Уровень топлива ниже 5% — критическое')
ON CONFLICT DO NOTHING;

-- ============================================================
-- Раздел 2: Имитация потоковых событий (2 часа работы)
-- Датчики генерируют данные каждые ~3-5 секунд
-- ============================================================

SELECT '=== Раздел 2: Имитация потоковых событий ===' AS info;

-- Очищаем старые данные (для повторного запуска)
TRUNCATE TABLE streaming.raw_sensor_events CASCADE;
TRUNCATE TABLE streaming.raw_equipment_events CASCADE;
TRUNCATE TABLE streaming.raw_navigation_events CASCADE;
TRUNCATE TABLE streaming.sensor_alerts CASCADE;
TRUNCATE TABLE streaming.window_aggregations CASCADE;

-- 2.1 Генерация событий телеметрии датчиков
-- Имитируем 2 часа работы (08:00 — 10:00, 15 марта 2025)
-- 6 единиц оборудования × 5 типов датчиков

DO $$
DECLARE
    v_event_counter  INTEGER := 0;
    v_equip_id       VARCHAR(10);
    v_sensor_type    VARCHAR(30);
    v_base_value     NUMERIC(10,2);
    v_unit           VARCHAR(20);
    v_value          NUMERIC(10,2);
    v_quality        VARCHAR(10);
    v_timestamp      TIMESTAMP;
    v_interval_sec   INTEGER;
    v_payload        JSONB;
    v_event_id       VARCHAR(20);
    v_equip_ids      VARCHAR[] := ARRAY['EQ-001', 'EQ-002', 'EQ-003', 'EQ-004', 'EQ-005', 'EQ-006'];
BEGIN
    -- Для каждого оборудования
    FOR i IN 1..6 LOOP
        v_equip_id := v_equip_ids[i];

        -- Для каждого типа датчика
        FOREACH v_sensor_type IN ARRAY ARRAY['temperature', 'vibration', 'pressure', 'speed', 'fuel_level'] LOOP

            -- Базовые значения для каждого типа датчика
            CASE v_sensor_type
                WHEN 'temperature' THEN v_base_value := 65.0 + (i * 3); v_unit := 'celsius';
                WHEN 'vibration'   THEN v_base_value := 2.0 + (i * 0.3); v_unit := 'mm_s';
                WHEN 'pressure'    THEN v_base_value := 5.5 + (i * 0.2); v_unit := 'bar';
                WHEN 'speed'       THEN v_base_value := 6.0 + (i * 0.5); v_unit := 'km_h';
                WHEN 'fuel_level'  THEN v_base_value := 80.0 - (i * 8); v_unit := 'percent';
            END CASE;

            -- Генерируем ~35 событий за 2 часа (каждые ~3-4 минуты)
            v_timestamp := '2025-03-15 08:00:00'::TIMESTAMP + (random() * INTERVAL '5 seconds');

            FOR j IN 1..35 LOOP
                v_event_counter := v_event_counter + 1;
                v_event_id := 'EVT-' || LPAD(v_event_counter::TEXT, 5, '0');

                -- Случайный интервал 2-5 минут между измерениями
                v_interval_sec := 120 + floor(random() * 180)::int;
                v_timestamp := v_timestamp + (v_interval_sec || ' seconds')::INTERVAL;

                -- Выход из цикла, если вышли за 10:00
                EXIT WHEN v_timestamp > '2025-03-15 10:00:00'::TIMESTAMP;

                -- Значение с нормальным шумом
                v_value := v_base_value + (random() - 0.5) * v_base_value * 0.15;

                -- Иногда (2%) — аномальные значения
                IF random() < 0.02 THEN
                    v_value := v_base_value * (1.5 + random() * 0.5);
                    v_quality := 'suspect';
                -- Иногда (1%) — плохое качество датчика
                ELSIF random() < 0.01 THEN
                    v_quality := 'bad';
                ELSE
                    v_quality := 'good';
                END IF;

                -- Для temperature: плавный тренд вверх (нагрев двигателя)
                IF v_sensor_type = 'temperature' THEN
                    v_value := v_value + (EXTRACT(EPOCH FROM (v_timestamp - '2025-03-15 08:00:00'::TIMESTAMP)) / 7200.0) * 10;
                END IF;

                -- Для fuel_level: плавный тренд вниз (расход)
                IF v_sensor_type = 'fuel_level' THEN
                    v_value := v_value - (EXTRACT(EPOCH FROM (v_timestamp - '2025-03-15 08:00:00'::TIMESTAMP)) / 7200.0) * 15;
                    IF v_value < 0 THEN v_value := 2.0 + random() * 3; END IF;
                END IF;

                -- JSON payload с дополнительными метаданными
                v_payload := jsonb_build_object(
                    'sensor_serial', 'SNS-' || v_equip_id || '-' || UPPER(LEFT(v_sensor_type, 3)),
                    'firmware_version', '2.4.1',
                    'battery_pct', 85 + floor(random() * 15)::int,
                    'signal_strength', -40 - floor(random() * 30)::int
                );

                INSERT INTO streaming.raw_sensor_events (
                    event_id, event_timestamp, equipment_id, event_type,
                    sensor_value, unit, quality_flag, payload,
                    partition_key
                )
                VALUES (
                    v_event_id, v_timestamp, v_equip_id, v_sensor_type,
                    ROUND(v_value, 2), v_unit, v_quality, v_payload,
                    v_equip_id || '_' || v_sensor_type
                );
            END LOOP;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'Сгенерировано % событий телеметрии', v_event_counter;
END $$;

-- 2.2 Генерация событий оборудования (старт/стоп)
INSERT INTO streaming.raw_equipment_events (
    event_id, event_timestamp, equipment_id, event_type,
    previous_state, new_state, payload, operator_id
)
VALUES
    -- EQ-001: старт смены
    ('EE-001', '2025-03-15 07:55:00.000', 'EQ-001', 'start',
     'idle', 'working', '{"shift": 1, "location": "Горизонт -320м"}'::jsonb, 'OP-001'),
    -- EQ-002: старт
    ('EE-002', '2025-03-15 07:58:00.000', 'EQ-002', 'start',
     'idle', 'working', '{"shift": 1, "location": "Горизонт -280м"}'::jsonb, 'OP-002'),
    -- EQ-003: старт
    ('EE-003', '2025-03-15 08:02:00.000', 'EQ-003', 'start',
     'idle', 'working', '{"shift": 1, "location": "Горизонт -320м"}'::jsonb, 'OP-003'),
    -- EQ-001: остановка на перерыв
    ('EE-004', '2025-03-15 09:00:00.000', 'EQ-001', 'stop',
     'working', 'break', '{"reason": "Перерыв оператора"}'::jsonb, 'OP-001'),
    -- EQ-001: возобновление
    ('EE-005', '2025-03-15 09:15:00.000', 'EQ-001', 'start',
     'break', 'working', '{"reason": "Возобновление после перерыва"}'::jsonb, 'OP-001'),
    -- EQ-004: ошибка
    ('EE-006', '2025-03-15 09:22:00.000', 'EQ-004', 'error',
     'working', 'error', '{"error_code": "HYD-034", "description": "Перегрев гидравлики"}'::jsonb, NULL),
    -- EQ-004: остановка после ошибки
    ('EE-007', '2025-03-15 09:23:00.000', 'EQ-004', 'stop',
     'error', 'maintenance', '{"reason": "Аварийная остановка"}'::jsonb, 'OP-004'),
    -- EQ-005: старт
    ('EE-008', '2025-03-15 08:10:00.000', 'EQ-005', 'start',
     'idle', 'working', '{"shift": 1, "location": "Горизонт -350м"}'::jsonb, 'OP-005'),
    -- EQ-006: старт
    ('EE-009', '2025-03-15 08:05:00.000', 'EQ-006', 'start',
     'idle', 'working', '{"shift": 1, "location": "Горизонт -280м"}'::jsonb, 'OP-006'),
    -- EQ-002: режим транспортировки
    ('EE-010', '2025-03-15 08:45:00.000', 'EQ-002', 'mode_change',
     'loading', 'transporting', '{"payload_tons": 22.5}'::jsonb, 'OP-002');

-- 2.3 Генерация навигационных событий
-- Имитируем перемещение EQ-001 по горизонту шахты
INSERT INTO streaming.raw_navigation_events (
    event_id, event_timestamp, equipment_id,
    x_coord, y_coord, z_coord, heading, speed_kmh,
    zone_id, horizon_id
)
SELECT
    'NAV-' || LPAD(row_number() OVER ()::TEXT, 5, '0'),
    '2025-03-15 08:00:00'::TIMESTAMP + (n * INTERVAL '30 seconds'),
    'EQ-001',
    -- Движение по X: вперёд и обратно
    100 + 50 * SIN(n::NUMERIC * 0.1),
    -- Движение по Y: медленное смещение
    200 + 20 * COS(n::NUMERIC * 0.05),
    -- Z: постоянная глубина горизонта
    -320.0,
    -- Heading: меняется при поворотах
    CASE WHEN MOD(n, 20) < 10 THEN 90.0 ELSE 270.0 END,
    -- Скорость: переменная
    CASE WHEN MOD(n, 5) = 0 THEN 0.0 -- Остановки
         ELSE 4.0 + random() * 5 END,
    -- Зона
    CASE WHEN n < 80 THEN 'ZONE-A-H3' WHEN n < 160 THEN 'ZONE-B-H3' ELSE 'ZONE-C-H3' END,
    'HRZ-003'
FROM generate_series(1, 240) AS n;  -- 240 × 30 сек = 2 часа

-- Проверка загруженных событий
SELECT '--- Статистика потоковых событий ---' AS info;

SELECT 'raw_sensor_events' AS table_name, COUNT(*) AS total,
       COUNT(DISTINCT equipment_id) AS unique_equipment,
       COUNT(DISTINCT event_type) AS unique_types,
       MIN(event_timestamp) AS first_event,
       MAX(event_timestamp) AS last_event
FROM streaming.raw_sensor_events

UNION ALL

SELECT 'raw_equipment_events', COUNT(*),
       COUNT(DISTINCT equipment_id), COUNT(DISTINCT event_type),
       MIN(event_timestamp), MAX(event_timestamp)
FROM streaming.raw_equipment_events

UNION ALL

SELECT 'raw_navigation_events', COUNT(*),
       COUNT(DISTINCT equipment_id), 0,
       MIN(event_timestamp), MAX(event_timestamp)
FROM streaming.raw_navigation_events;

-- Распределение по типам датчиков
SELECT event_type, COUNT(*) AS events,
       ROUND(AVG(sensor_value), 2) AS avg_value,
       ROUND(MIN(sensor_value), 2) AS min_value,
       ROUND(MAX(sensor_value), 2) AS max_value
FROM streaming.raw_sensor_events
GROUP BY event_type
ORDER BY event_type;

-- ============================================================
-- Раздел 3: Оконные агрегации (Tumbling Windows)
-- Неперекрывающиеся окна фиксированной ширины
-- ============================================================

SELECT '=== Раздел 3: Оконные агрегации ===' AS info;

-- 3.1 Tumbling Window: 5 минут
-- Каждое событие попадает ровно в одно окно

SELECT '--- Tumbling Window 5 минут (temperature, EQ-001) ---' AS info;

-- Визуализация оконной агрегации
INSERT INTO streaming.window_aggregations (
    equipment_id, event_type, window_type,
    window_start, window_end,
    reading_count, avg_value, min_value, max_value, stddev_value, sum_value
)
SELECT
    equipment_id,
    event_type,
    'tumbling_5min',
    -- Начало 5-минутного окна
    date_trunc('hour', event_timestamp)
        + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5),
    -- Конец окна
    date_trunc('hour', event_timestamp)
        + INTERVAL '5 min' * (FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5) + 1),
    COUNT(*),
    ROUND(AVG(sensor_value), 2),
    ROUND(MIN(sensor_value), 2),
    ROUND(MAX(sensor_value), 2),
    ROUND(STDDEV(sensor_value), 2),
    ROUND(SUM(sensor_value), 2)
FROM streaming.raw_sensor_events
WHERE quality_flag IN ('good', 'suspect')  -- Исключаем bad
GROUP BY equipment_id, event_type,
         date_trunc('hour', event_timestamp)
             + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5)
ON CONFLICT (equipment_id, event_type, window_type, window_start) DO UPDATE
SET reading_count = EXCLUDED.reading_count,
    avg_value = EXCLUDED.avg_value,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    stddev_value = EXCLUDED.stddev_value,
    sum_value = EXCLUDED.sum_value;

-- Показываем результат для EQ-001, temperature
SELECT equipment_id, window_start, window_end,
       reading_count, avg_value, min_value, max_value
FROM streaming.window_aggregations
WHERE equipment_id = 'EQ-001' AND event_type = 'temperature'
  AND window_type = 'tumbling_5min'
ORDER BY window_start
LIMIT 15;

-- 3.2 Tumbling Window: 1 час
INSERT INTO streaming.window_aggregations (
    equipment_id, event_type, window_type,
    window_start, window_end,
    reading_count, avg_value, min_value, max_value, stddev_value, sum_value
)
SELECT
    equipment_id,
    event_type,
    'tumbling_1h',
    date_trunc('hour', event_timestamp),
    date_trunc('hour', event_timestamp) + INTERVAL '1 hour',
    COUNT(*),
    ROUND(AVG(sensor_value), 2),
    ROUND(MIN(sensor_value), 2),
    ROUND(MAX(sensor_value), 2),
    ROUND(STDDEV(sensor_value), 2),
    ROUND(SUM(sensor_value), 2)
FROM streaming.raw_sensor_events
WHERE quality_flag IN ('good', 'suspect')
GROUP BY equipment_id, event_type,
         date_trunc('hour', event_timestamp)
ON CONFLICT (equipment_id, event_type, window_type, window_start) DO UPDATE
SET reading_count = EXCLUDED.reading_count,
    avg_value = EXCLUDED.avg_value,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    stddev_value = EXCLUDED.stddev_value,
    sum_value = EXCLUDED.sum_value;

SELECT '--- Tumbling Window 1 час (все типы, EQ-001) ---' AS info;
SELECT event_type, window_start, window_end,
       reading_count, avg_value, min_value, max_value
FROM streaming.window_aggregations
WHERE equipment_id = 'EQ-001' AND window_type = 'tumbling_1h'
ORDER BY event_type, window_start;

-- ============================================================
-- Раздел 4: Скользящие средние (Moving Averages)
-- ============================================================

SELECT '=== Раздел 4: Скользящие средние и аномалии ===' AS info;

-- 4.1 Скользящее среднее по 10 последним измерениям
SELECT '--- Скользящее среднее (10 точек) для temperature, EQ-001 ---' AS info;

SELECT
    event_id,
    event_timestamp,
    sensor_value,
    -- Скользящее среднее (10 предыдущих измерений)
    ROUND(AVG(sensor_value) OVER w, 2) AS moving_avg_10,
    -- Скользящий минимум и максимум
    ROUND(MIN(sensor_value) OVER w, 2) AS moving_min_10,
    ROUND(MAX(sensor_value) OVER w, 2) AS moving_max_10,
    -- Отклонение от скользящего среднего
    ROUND(sensor_value - AVG(sensor_value) OVER w, 2) AS deviation,
    -- Скользящее стандартное отклонение
    ROUND(STDDEV(sensor_value) OVER w, 2) AS moving_stddev
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND quality_flag = 'good'
WINDOW w AS (
    PARTITION BY equipment_id
    ORDER BY event_timestamp
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
)
ORDER BY event_timestamp;

-- 4.2 Обнаружение аномалий (Z-score > 2)
-- Аномалия = значение отклоняется более чем на 2 стандартных отклонения
SELECT '--- Аномальные показания (Z-score > 2) ---' AS info;

WITH stats AS (
    SELECT equipment_id, event_type,
           AVG(sensor_value) AS mean_val,
           STDDEV(sensor_value) AS stddev_val
    FROM streaming.raw_sensor_events
    WHERE quality_flag = 'good'
    GROUP BY equipment_id, event_type
    HAVING STDDEV(sensor_value) > 0  -- Исключаем нулевое отклонение
)
SELECT e.event_id, e.event_timestamp, e.equipment_id, e.event_type,
       ROUND(e.sensor_value, 2) AS sensor_value,
       e.unit,
       ROUND(s.mean_val, 2) AS mean_val,
       ROUND(s.stddev_val, 2) AS stddev_val,
       ROUND(ABS(e.sensor_value - s.mean_val) / s.stddev_val, 2) AS z_score
FROM streaming.raw_sensor_events e
JOIN stats s ON e.equipment_id = s.equipment_id AND e.event_type = s.event_type
WHERE ABS(e.sensor_value - s.mean_val) / s.stddev_val > 2
ORDER BY e.event_timestamp;

-- 4.3 Обнаружение трендов (рост температуры)
SELECT '--- Тренд температуры: EQ-001 (почасово) ---' AS info;

SELECT
    date_trunc('hour', event_timestamp) AS hour_start,
    ROUND(AVG(sensor_value), 2) AS avg_temp,
    ROUND(AVG(sensor_value) - LAG(ROUND(AVG(sensor_value), 2))
        OVER (ORDER BY date_trunc('hour', event_timestamp)), 2) AS temp_change,
    CASE
        WHEN AVG(sensor_value) - LAG(AVG(sensor_value))
            OVER (ORDER BY date_trunc('hour', event_timestamp)) > 3
        THEN 'РАСТЁТ!'
        WHEN AVG(sensor_value) - LAG(AVG(sensor_value))
            OVER (ORDER BY date_trunc('hour', event_timestamp)) < -3
        THEN 'ПАДАЕТ!'
        ELSE 'Стабильно'
    END AS trend
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND quality_flag = 'good'
GROUP BY date_trunc('hour', event_timestamp)
ORDER BY hour_start;

-- ============================================================
-- Раздел 5: Правила алертов (пороговая обработка)
-- ============================================================

SELECT '=== Раздел 5: Генерация алертов ===' AS info;

-- 5.1 Применение правил: temperature > warning_threshold
INSERT INTO streaming.sensor_alerts (
    equipment_id, event_type, alert_level,
    alert_message, sensor_value, threshold_value,
    event_timestamp, source_event_id
)
SELECT
    e.equipment_id,
    e.event_type,
    r.alert_level,
    r.description || ' (значение: ' || ROUND(e.sensor_value, 1) || ' ' || e.unit || ')',
    e.sensor_value,
    COALESCE(r.threshold_high, r.threshold_low),
    e.event_timestamp,
    e.event_id
FROM streaming.raw_sensor_events e
JOIN streaming.alert_rules r ON e.event_type = r.event_type AND r.is_active = TRUE
WHERE
    -- Правило "больше порога" (gt)
    (r.condition_type = 'gt' AND e.sensor_value > r.threshold_high)
    OR
    -- Правило "меньше порога" (lt)
    (r.condition_type = 'lt' AND e.sensor_value < r.threshold_low)
ORDER BY e.event_timestamp;

-- 5.2 Статистика алертов
SELECT '--- Статистика алертов ---' AS info;
SELECT alert_level, event_type, COUNT(*) AS alert_count,
       ROUND(MIN(sensor_value), 2) AS min_trigger_value,
       ROUND(MAX(sensor_value), 2) AS max_trigger_value
FROM streaming.sensor_alerts
GROUP BY alert_level, event_type
ORDER BY alert_level, event_type;

-- 5.3 Алерты по оборудованию
SELECT '--- Алерты по оборудованию ---' AS info;
SELECT equipment_id, alert_level, COUNT(*) AS alert_count
FROM streaming.sensor_alerts
GROUP BY equipment_id, alert_level
ORDER BY equipment_id, alert_level;

-- 5.4 Детальный список критических алертов
SELECT '--- Критические алерты (детально) ---' AS info;
SELECT alert_id, equipment_id, event_type,
       ROUND(sensor_value, 2) AS sensor_value,
       ROUND(threshold_value, 2) AS threshold,
       alert_message,
       event_timestamp
FROM streaming.sensor_alerts
WHERE alert_level = 'critical'
ORDER BY event_timestamp;

-- ============================================================
-- Раздел 6: Сравнение Batch vs Stream на одних данных
-- ============================================================

SELECT '=== Раздел 6: Сравнение Batch vs Stream ===' AS info;

-- 6.1 Batch-подход: агрегация за весь период
SELECT '--- BATCH: средняя температура EQ-001 за 08:00-09:00 ---' AS info;
SELECT
    equipment_id,
    'batch' AS approach,
    ROUND(AVG(sensor_value), 4) AS avg_temp,
    COUNT(*) AS readings,
    ROUND(MIN(sensor_value), 2) AS min_temp,
    ROUND(MAX(sensor_value), 2) AS max_temp
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND event_timestamp >= '2025-03-15 08:00:00'
  AND event_timestamp < '2025-03-15 09:00:00'
  AND quality_flag = 'good'
GROUP BY equipment_id;

-- 6.2 Stream-подход: агрегация из окон
SELECT '--- STREAM: средняя температура EQ-001 за 08:00-09:00 (из окон) ---' AS info;
SELECT
    equipment_id,
    'stream' AS approach,
    -- Среднее взвешенное по окнам (с учётом количества измерений в окне)
    ROUND(SUM(avg_value * reading_count) / NULLIF(SUM(reading_count), 0), 4) AS avg_temp,
    SUM(reading_count) AS total_readings,
    MIN(min_value) AS min_temp,
    MAX(max_value) AS max_temp
FROM streaming.window_aggregations
WHERE equipment_id = 'EQ-001'
  AND event_type = 'temperature'
  AND window_type = 'tumbling_5min'
  AND window_start >= '2025-03-15 08:00:00'
  AND window_end <= '2025-03-15 09:00:00'
GROUP BY equipment_id;

-- 6.3 Визуализация: Timeline оборудования EQ-001
-- Объединяем события разных типов в единую хронологию
SELECT '--- Timeline EQ-001 (первые 30 минут) ---' AS info;

SELECT event_timestamp, source, event_desc
FROM (
    -- Телеметрия (температура)
    SELECT event_timestamp,
           'SENSOR' AS source,
           event_type || ': ' || ROUND(sensor_value, 1) || ' ' || unit AS event_desc
    FROM streaming.raw_sensor_events
    WHERE equipment_id = 'EQ-001'
      AND event_type = 'temperature'
      AND event_timestamp < '2025-03-15 08:30:00'

    UNION ALL

    -- События оборудования
    SELECT event_timestamp,
           'EQUIP' AS source,
           event_type || ': ' || previous_state || ' → ' || new_state AS event_desc
    FROM streaming.raw_equipment_events
    WHERE equipment_id = 'EQ-001'
      AND event_timestamp < '2025-03-15 08:30:00'

    UNION ALL

    -- Алерты
    SELECT event_timestamp,
           'ALERT' AS source,
           '[' || alert_level || '] ' || alert_message AS event_desc
    FROM streaming.sensor_alerts
    WHERE equipment_id = 'EQ-001'
      AND event_timestamp < '2025-03-15 08:30:00'
) combined
ORDER BY event_timestamp
LIMIT 20;

-- ============================================================
-- Раздел 7: Итоговая проверка
-- ============================================================

SELECT '=== Раздел 7: Итоговая проверка ===' AS info;

-- Все объекты streaming-схемы
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'streaming'
ORDER BY table_name;

-- Статистика
SELECT 'raw_sensor_events' AS table_name, COUNT(*) AS rows FROM streaming.raw_sensor_events
UNION ALL SELECT 'raw_equipment_events', COUNT(*) FROM streaming.raw_equipment_events
UNION ALL SELECT 'raw_navigation_events', COUNT(*) FROM streaming.raw_navigation_events
UNION ALL SELECT 'sensor_alerts', COUNT(*) FROM streaming.sensor_alerts
UNION ALL SELECT 'window_aggregations', COUNT(*) FROM streaming.window_aggregations
UNION ALL SELECT 'alert_rules', COUNT(*) FROM streaming.alert_rules
ORDER BY table_name;

SELECT '=== Потоковая обработка завершена ===' AS info;
