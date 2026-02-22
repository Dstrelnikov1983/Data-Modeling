-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 5. Специализированное моделирование данных
-- Скрипт 2: Генерация реалистичных данных временных рядов
-- Предприятие: "Руда+" — добыча железной руды
--
-- Генерирует ~26 000+ строк показаний датчиков за март 2025
-- Включает реалистичные паттерны и аномалии
--
-- ВАЖНО: Выполните скрипт 01_timescaledb_setup.sql перед этим.
-- ============================================================

SET search_path TO timeseries, public;

-- ============================================================
-- Шаг 1: Очистка предыдущих данных (если перезапуск)
-- ============================================================

TRUNCATE TABLE timeseries.sensor_readings;
SELECT '--- Предыдущие данные очищены ---' AS info;

-- ============================================================
-- Шаг 2: Генерация данных температуры двигателя
-- ============================================================
-- Паттерн:
--   Базовая: 65°C
--   Суточный цикл: ±10°C (теплее днём 12:00, холоднее ночью 3:00)
--   Шум: ±3°C (случайный)
--   Аномалия: 15 марта EQ-001, скачок до 105°C (перегрев двигателя)

SELECT '--- Генерация данных температуры ---' AS info;

INSERT INTO timeseries.sensor_readings (reading_time, equipment_id, sensor_type, value, unit, quality)
SELECT
    ts AS reading_time,
    eq.equipment_id,
    'temperature' AS sensor_type,
    -- Базовая температура + суточный цикл + шум + аномалия
    ROUND((
        65.0
        -- Суточный цикл: синусоида с пиком в 14:00 и минимумом в 2:00
        + 10.0 * sin(2.0 * pi() * (EXTRACT(HOUR FROM ts) - 2.0) / 24.0)
        -- Случайный шум ±3°C
        + (random() * 6.0 - 3.0)
        -- Аномалия: EQ-001, 15 марта 10:00-12:00, резкий скачок
        + CASE
            WHEN eq.equipment_id = 'EQ-001'
                 AND ts >= '2025-03-15 10:00:00'::timestamptz
                 AND ts <= '2025-03-15 12:00:00'::timestamptz
            THEN 35.0 + (random() * 10.0)
            -- Небольшое повышение для EQ-001 15 марта (нарастание проблемы)
            WHEN eq.equipment_id = 'EQ-001'
                 AND ts >= '2025-03-15 08:00:00'::timestamptz
                 AND ts < '2025-03-15 10:00:00'::timestamptz
            THEN 10.0 + (random() * 5.0)
            ELSE 0.0
          END
    )::numeric, 1)::double precision AS value,
    'celsius' AS unit,
    -- Качество: suspect при >90, bad при >100
    CASE
        WHEN (65.0
              + 10.0 * sin(2.0 * pi() * (EXTRACT(HOUR FROM ts) - 2.0) / 24.0)
              + CASE
                  WHEN eq.equipment_id = 'EQ-001'
                       AND ts >= '2025-03-15 10:00:00'::timestamptz
                       AND ts <= '2025-03-15 12:00:00'::timestamptz
                  THEN 35.0
                  WHEN eq.equipment_id = 'EQ-001'
                       AND ts >= '2025-03-15 08:00:00'::timestamptz
                       AND ts < '2025-03-15 10:00:00'::timestamptz
                  THEN 10.0
                  ELSE 0.0
                END
             ) > 100 THEN 'bad'
        WHEN (65.0
              + 10.0 * sin(2.0 * pi() * (EXTRACT(HOUR FROM ts) - 2.0) / 24.0)
              + CASE
                  WHEN eq.equipment_id = 'EQ-001'
                       AND ts >= '2025-03-15 08:00:00'::timestamptz
                       AND ts < '2025-03-15 10:00:00'::timestamptz
                  THEN 10.0
                  ELSE 0.0
                END
             ) > 90 THEN 'suspect'
        ELSE 'good'
    END AS quality
FROM generate_series(
    '2025-03-01 00:00:00'::timestamptz,
    '2025-03-31 23:50:00'::timestamptz,
    '10 minutes'::interval
) AS ts
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002'), ('EQ-003'),
           ('EQ-004'), ('EQ-005'), ('EQ-006')
) AS eq(equipment_id);

SELECT '--- Температура: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.sensor_readings
WHERE sensor_type = 'temperature';

-- ============================================================
-- Шаг 3: Генерация данных вибрации
-- ============================================================
-- Паттерн:
--   Базовая: 1.5 мм/с
--   Постепенный рост за месяц (моделирует износ подшипников)
--   Шум: ±0.5 мм/с
--   Аномалия: EQ-003, 20 марта, внезапный скачок до 8 мм/с
--   Для EQ-003 дополнительно: ускоренный износ

SELECT '--- Генерация данных вибрации ---' AS info;

INSERT INTO timeseries.sensor_readings (reading_time, equipment_id, sensor_type, value, unit, quality)
SELECT
    ts AS reading_time,
    eq.equipment_id,
    'vibration' AS sensor_type,
    ROUND((
        -- Базовая вибрация
        1.5
        -- Постепенный рост (износ): +0.5 мм/с за месяц для обычного оборудования
        + 0.5 * (EXTRACT(EPOCH FROM (ts - '2025-03-01'::timestamptz)) /
                  EXTRACT(EPOCH FROM ('2025-04-01'::timestamptz - '2025-03-01'::timestamptz)))
        -- Ускоренный износ для EQ-003: +2.0 мм/с за месяц
        + CASE WHEN eq.equipment_id = 'EQ-003'
               THEN 2.0 * (EXTRACT(EPOCH FROM (ts - '2025-03-01'::timestamptz)) /
                            EXTRACT(EPOCH FROM ('2025-04-01'::timestamptz - '2025-03-01'::timestamptz)))
               ELSE 0.0
          END
        -- Случайный шум ±0.5 мм/с
        + (random() * 1.0 - 0.5)
        -- Аномалия: EQ-003, 20 марта 14:00-15:00 (дефект подшипника)
        + CASE
            WHEN eq.equipment_id = 'EQ-003'
                 AND ts >= '2025-03-20 14:00:00'::timestamptz
                 AND ts <= '2025-03-20 15:00:00'::timestamptz
            THEN 5.0 + (random() * 3.0)
            ELSE 0.0
          END
    )::numeric, 2)::double precision AS value,
    'mm_s' AS unit,
    CASE
        WHEN eq.equipment_id = 'EQ-003'
             AND ts >= '2025-03-20 14:00:00'::timestamptz
             AND ts <= '2025-03-20 15:00:00'::timestamptz
        THEN 'bad'
        WHEN (1.5 + 0.5 * (EXTRACT(EPOCH FROM (ts - '2025-03-01'::timestamptz)) /
              EXTRACT(EPOCH FROM ('2025-04-01'::timestamptz - '2025-03-01'::timestamptz)))
              + CASE WHEN eq.equipment_id = 'EQ-003'
                     THEN 2.0 * (EXTRACT(EPOCH FROM (ts - '2025-03-01'::timestamptz)) /
                                  EXTRACT(EPOCH FROM ('2025-04-01'::timestamptz - '2025-03-01'::timestamptz)))
                     ELSE 0.0 END
             ) > 4.0
        THEN 'suspect'
        ELSE 'good'
    END AS quality
FROM generate_series(
    '2025-03-01 00:00:00'::timestamptz,
    '2025-03-31 23:50:00'::timestamptz,
    '10 minutes'::interval
) AS ts
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002'), ('EQ-003'),
           ('EQ-004'), ('EQ-005'), ('EQ-006')
) AS eq(equipment_id);

SELECT '--- Вибрация: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration';

-- ============================================================
-- Шаг 4: Генерация данных давления гидравлики
-- ============================================================
-- Паттерн:
--   Базовая: 150 бар
--   Колебания ±15 бар (зависят от нагрузки)
--   Шум: ±5 бар
--   Аномалия: EQ-002, 25 марта, падение до 80 бар (утечка)

SELECT '--- Генерация данных давления ---' AS info;

INSERT INTO timeseries.sensor_readings (reading_time, equipment_id, sensor_type, value, unit, quality)
SELECT
    ts AS reading_time,
    eq.equipment_id,
    'pressure' AS sensor_type,
    ROUND((
        -- Базовое давление
        150.0
        -- Колебания нагрузки (днём больше, ночью меньше)
        + 15.0 * sin(2.0 * pi() * (EXTRACT(HOUR FROM ts) - 6.0) / 24.0)
        -- Случайный шум ±5 бар
        + (random() * 10.0 - 5.0)
        -- Аномалия: EQ-002, 25 марта — постепенное падение давления (утечка)
        + CASE
            WHEN eq.equipment_id = 'EQ-002'
                 AND ts >= '2025-03-25 06:00:00'::timestamptz
                 AND ts <= '2025-03-25 18:00:00'::timestamptz
            THEN -5.0 * (EXTRACT(EPOCH FROM (ts - '2025-03-25 06:00:00'::timestamptz)) /
                          EXTRACT(EPOCH FROM ('12 hours'::interval)))
                 - 20.0  -- падение от -20 до -80 бар
            WHEN eq.equipment_id = 'EQ-002'
                 AND ts > '2025-03-25 18:00:00'::timestamptz
                 AND ts <= '2025-03-25 23:59:59'::timestamptz
            THEN -65.0  -- давление остаётся низким после утечки
            ELSE 0.0
          END
    )::numeric, 1)::double precision AS value,
    'bar' AS unit,
    CASE
        WHEN eq.equipment_id = 'EQ-002'
             AND ts >= '2025-03-25 12:00:00'::timestamptz
             AND ts <= '2025-03-25 23:59:59'::timestamptz
        THEN 'bad'
        WHEN eq.equipment_id = 'EQ-002'
             AND ts >= '2025-03-25 06:00:00'::timestamptz
             AND ts < '2025-03-25 12:00:00'::timestamptz
        THEN 'suspect'
        ELSE 'good'
    END AS quality
FROM generate_series(
    '2025-03-01 00:00:00'::timestamptz,
    '2025-03-31 23:50:00'::timestamptz,
    '10 minutes'::interval
) AS ts
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002'), ('EQ-003'),
           ('EQ-004'), ('EQ-005'), ('EQ-006')
) AS eq(equipment_id);

SELECT '--- Давление: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.sensor_readings
WHERE sensor_type = 'pressure';

-- ============================================================
-- Шаг 5: Генерация данных скорости движения
-- ============================================================
-- Паттерн:
--   Рабочие часы (06:00-22:00): базовая 5 км/ч ± колебания
--   Ночные часы (22:00-06:00): 0 км/ч (оборудование стоит)
--   Перерывы: скорость 0 в обеденное время (12:00-13:00)

SELECT '--- Генерация данных скорости ---' AS info;

INSERT INTO timeseries.sensor_readings (reading_time, equipment_id, sensor_type, value, unit, quality)
SELECT
    ts AS reading_time,
    eq.equipment_id,
    'speed' AS sensor_type,
    ROUND((
        CASE
            -- Ночное время (22:00-06:00) — оборудование стоит
            WHEN EXTRACT(HOUR FROM ts) < 6 OR EXTRACT(HOUR FROM ts) >= 22 THEN 0.0
            -- Обеденный перерыв (12:00-13:00) — оборудование стоит
            WHEN EXTRACT(HOUR FROM ts) = 12 THEN 0.0
            -- Рабочие часы — нормальная скорость
            ELSE
                5.0
                -- Колебания скорости в зависимости от нагрузки
                + 2.0 * sin(2.0 * pi() * EXTRACT(MINUTE FROM ts) / 60.0)
                -- Случайный шум ±1 км/ч
                + (random() * 2.0 - 1.0)
        END
    )::numeric, 1)::double precision AS value,
    'km_h' AS unit,
    'good' AS quality
FROM generate_series(
    '2025-03-01 00:00:00'::timestamptz,
    '2025-03-31 23:50:00'::timestamptz,
    '10 minutes'::interval
) AS ts
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002'), ('EQ-003'),
           ('EQ-004'), ('EQ-005'), ('EQ-006')
) AS eq(equipment_id);

SELECT '--- Скорость: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.sensor_readings
WHERE sensor_type = 'speed';

-- ============================================================
-- Шаг 6: Генерация данных уровня топлива
-- ============================================================
-- Паттерн:
--   Начало смены (06:00): 100% (заправка)
--   В течение смены: постепенное уменьшение (-5% / час)
--   Вторая смена (14:00): снова 100% (заправка)
--   Ночь: уровень стабилен (расход минимален)

SELECT '--- Генерация данных топлива ---' AS info;

INSERT INTO timeseries.sensor_readings (reading_time, equipment_id, sensor_type, value, unit, quality)
SELECT
    ts AS reading_time,
    eq.equipment_id,
    'fuel_level' AS sensor_type,
    ROUND((
        CASE
            -- Ночное время (22:00-06:00) — минимальный расход
            WHEN EXTRACT(HOUR FROM ts) < 6 OR EXTRACT(HOUR FROM ts) >= 22
            THEN 30.0 + (random() * 5.0)
            -- Первая смена (06:00-14:00): от 100 до ~60%
            WHEN EXTRACT(HOUR FROM ts) >= 6 AND EXTRACT(HOUR FROM ts) < 14
            THEN GREATEST(
                100.0 - 5.0 * (EXTRACT(HOUR FROM ts) - 6 + EXTRACT(MINUTE FROM ts) / 60.0)
                + (random() * 3.0 - 1.5),
                20.0
            )
            -- Вторая смена (14:00-22:00): от 100 до ~60%
            WHEN EXTRACT(HOUR FROM ts) >= 14 AND EXTRACT(HOUR FROM ts) < 22
            THEN GREATEST(
                100.0 - 5.0 * (EXTRACT(HOUR FROM ts) - 14 + EXTRACT(MINUTE FROM ts) / 60.0)
                + (random() * 3.0 - 1.5),
                20.0
            )
            ELSE 50.0
        END
    )::numeric, 1)::double precision AS value,
    'percent' AS unit,
    CASE
        WHEN (CASE
                WHEN EXTRACT(HOUR FROM ts) >= 6 AND EXTRACT(HOUR FROM ts) < 14
                THEN 100.0 - 5.0 * (EXTRACT(HOUR FROM ts) - 6)
                WHEN EXTRACT(HOUR FROM ts) >= 14 AND EXTRACT(HOUR FROM ts) < 22
                THEN 100.0 - 5.0 * (EXTRACT(HOUR FROM ts) - 14)
                ELSE 50.0
              END) < 25.0
        THEN 'suspect'
        ELSE 'good'
    END AS quality
FROM generate_series(
    '2025-03-01 00:00:00'::timestamptz,
    '2025-03-31 23:50:00'::timestamptz,
    '10 minutes'::interval
) AS ts
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002'), ('EQ-003'),
           ('EQ-004'), ('EQ-005'), ('EQ-006')
) AS eq(equipment_id);

SELECT '--- Топливо: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.sensor_readings
WHERE sensor_type = 'fuel_level';

-- ============================================================
-- Шаг 7: Генерация данных производственного временного ряда
-- ============================================================
-- Добыча руды по сменам (2 смены в день, 8 часов каждая)

SELECT '--- Генерация данных добычи ---' AS info;

INSERT INTO timeseries.production_timeseries
    (production_time, mine_id, equipment_id, operator_id, tonnage, fe_content, moisture, ore_type, horizon)
SELECT
    ts AS production_time,
    mine.mine_id,
    eq.equipment_id,
    op.operator_id,
    -- Тоннаж: 15-25 тонн за интервал (дневные смены), 0 ночью
    CASE
        WHEN EXTRACT(HOUR FROM ts) >= 6 AND EXTRACT(HOUR FROM ts) < 22
        THEN ROUND((15.0 + random() * 10.0)::numeric, 1)
        ELSE 0.0
    END AS tonnage,
    -- Содержание Fe: 28-36%
    ROUND((28.0 + random() * 8.0)::numeric, 1) AS fe_content,
    -- Влажность: 3-8%
    ROUND((3.0 + random() * 5.0)::numeric, 1) AS moisture,
    -- Тип руды
    (ARRAY['Магнетит', 'Гематит', 'Сидерит'])[1 + floor(random() * 3)::int] AS ore_type,
    -- Горизонт
    mine.horizon
FROM generate_series(
    '2025-03-01 06:00:00'::timestamptz,
    '2025-03-31 22:00:00'::timestamptz,
    '4 hours'::interval
) AS ts
CROSS JOIN (
    VALUES ('MINE-001', 'Горизонт -350м'),
           ('MINE-002', 'Горизонт -280м'),
           ('MINE-001', 'Горизонт -500м')
) AS mine(mine_id, horizon)
CROSS JOIN (
    VALUES ('EQ-001'), ('EQ-002')
) AS eq(equipment_id)
CROSS JOIN (
    VALUES ('OP-001'), ('OP-003')
) AS op(operator_id)
-- Ограничиваем, чтобы каждая комбинация была уникальной по времени
WHERE (EXTRACT(HOUR FROM ts) + ASCII(eq.equipment_id)) % 2 = 0;

SELECT '--- Добыча: ' || COUNT(*) || ' строк ---' AS info
FROM timeseries.production_timeseries;

-- ============================================================
-- Шаг 8: Генерация алертов на основе аномалий
-- ============================================================

SELECT '--- Генерация алертов ---' AS info;

-- Алерт 1: Перегрев двигателя EQ-001, 15 марта
INSERT INTO timeseries.alerts (alert_time, equipment_id, sensor_type, severity, trigger_value, threshold, message)
VALUES
('2025-03-15 10:10:00'::timestamptz, 'EQ-001', 'temperature', 'warning',
 92.5, 90.0, 'Температура двигателя приближается к критическому уровню'),
('2025-03-15 10:30:00'::timestamptz, 'EQ-001', 'temperature', 'critical',
 101.3, 100.0, 'КРИТИЧНО: Температура двигателя превысила 100°C'),
('2025-03-15 10:50:00'::timestamptz, 'EQ-001', 'temperature', 'emergency',
 108.7, 100.0, 'АВАРИЙНАЯ ОСТАНОВКА: Перегрев двигателя ПДМ-01');

-- Алерт 2: Высокая вибрация EQ-003, 20 марта
INSERT INTO timeseries.alerts (alert_time, equipment_id, sensor_type, severity, trigger_value, threshold, message)
VALUES
('2025-03-20 14:10:00'::timestamptz, 'EQ-003', 'vibration', 'warning',
 5.2, 5.0, 'Повышенная вибрация: возможен дефект подшипника'),
('2025-03-20 14:30:00'::timestamptz, 'EQ-003', 'vibration', 'critical',
 7.8, 7.0, 'КРИТИЧНО: Вибрация превысила допустимый уровень'),
('2025-03-20 14:50:00'::timestamptz, 'EQ-003', 'vibration', 'emergency',
 9.1, 7.0, 'АВАРИЙНАЯ ОСТАНОВКА: Разрушение подшипника вероятно');

-- Алерт 3: Падение давления EQ-002, 25 марта
INSERT INTO timeseries.alerts (alert_time, equipment_id, sensor_type, severity, trigger_value, threshold, message)
VALUES
('2025-03-25 09:00:00'::timestamptz, 'EQ-002', 'pressure', 'warning',
 120.5, 130.0, 'Давление гидравлики снижается: проверьте систему'),
('2025-03-25 12:00:00'::timestamptz, 'EQ-002', 'pressure', 'critical',
 95.2, 100.0, 'КРИТИЧНО: Давление гидравлики ниже рабочего диапазона'),
('2025-03-25 15:00:00'::timestamptz, 'EQ-002', 'pressure', 'emergency',
 82.1, 100.0, 'АВАРИЙНАЯ ОСТАНОВКА: Утечка гидравлической жидкости');

SELECT '--- Алерты: ' || COUNT(*) || ' записей ---' AS info
FROM timeseries.alerts;

-- ============================================================
-- Шаг 9: Итоговая проверка
-- ============================================================

SELECT '=== ИТОГОВАЯ СТАТИСТИКА ===' AS info;

-- Общее количество показаний
SELECT
    COUNT(*) AS total_readings,
    COUNT(DISTINCT equipment_id) AS equipment_count,
    COUNT(DISTINCT sensor_type) AS sensor_types,
    MIN(reading_time) AS first_reading,
    MAX(reading_time) AS last_reading
FROM timeseries.sensor_readings;

-- Распределение по типам датчиков
SELECT sensor_type,
       COUNT(*) AS readings,
       ROUND(AVG(value)::numeric, 2) AS avg_value,
       ROUND(MIN(value)::numeric, 2) AS min_value,
       ROUND(MAX(value)::numeric, 2) AS max_value
FROM timeseries.sensor_readings
GROUP BY sensor_type
ORDER BY sensor_type;

-- Распределение по качеству
SELECT quality,
       COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
FROM timeseries.sensor_readings
GROUP BY quality
ORDER BY quality;

-- Количество записей добычи
SELECT COUNT(*) AS production_records,
       ROUND(SUM(tonnage)::numeric, 0) AS total_tonnage
FROM timeseries.production_timeseries
WHERE tonnage > 0;

-- Алерты
SELECT severity, COUNT(*) AS count
FROM timeseries.alerts
GROUP BY severity
ORDER BY
    CASE severity
        WHEN 'warning' THEN 1
        WHEN 'critical' THEN 2
        WHEN 'emergency' THEN 3
    END;

SELECT '=== Загрузка данных завершена ===' AS info;
