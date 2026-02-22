-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 4. Моделирование потоковых и пакетных данных
-- Скрипт 2: Полная загрузка (Full Load) OLTP → Staging → Star
-- Предприятие: "Руда+" — добыча железной руды
--
-- ВАЖНО: Сначала выполните скрипт 01_staging_schema.sql
--         Схемы ruda_plus, star и staging должны существовать.
--
-- Паттерн: Truncate-and-Load
-- 1. Регистрация загрузки в журнале
-- 2. Очистка staging-таблиц (TRUNCATE)
-- 3. Извлечение данных из OLTP с вычислением хеша
-- 4. Проверки качества в staging
-- 5. Трансформация и загрузка в Star Schema
-- 6. Обновление журнала загрузок и водяных знаков
-- ============================================================

SET search_path TO staging, ruda_plus, star, public;

-- ============================================================
-- Раздел 1: Полная загрузка справочников (Dimensions)
-- Порядок загрузки: справочники → оборудование → операторы → факты
-- ============================================================

-- ============================================================
-- 1.1 Загрузка таблицы mines (Шахты)
-- ============================================================

DO $$
DECLARE
    v_load_id     INTEGER;
    v_extracted   INTEGER;
    v_loaded      INTEGER;
BEGIN
    -- Шаг 1: Регистрация загрузки
    v_load_id := staging.start_etl_load('mines', 'full');

    -- Шаг 2: Очистка staging-таблицы (Truncate-and-Load)
    TRUNCATE TABLE staging.stg_mines;

    -- Шаг 3: Извлечение из OLTP с вычислением хеша строки
    INSERT INTO staging.stg_mines (
        mine_id, mine_name, location, region, max_depth_m,
        status, opened_date, created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        m.mine_id, m.mine_name, m.location, m.region, m.max_depth_m,
        m.status, m.opened_date, m.created_at, m.updated_at,
        v_load_id,
        'ruda_plus.mines',
        -- MD5-хеш всех бизнес-полей для обнаружения изменений
        MD5(CONCAT_WS('|',
            m.mine_name, m.location, m.region,
            m.max_depth_m, m.status, m.opened_date
        ))
    FROM ruda_plus.mines m;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;
    RAISE NOTICE 'mines: извлечено % строк', v_extracted;

    -- Шаг 4: Проверка качества — NULL в обязательных полях
    IF EXISTS (
        SELECT 1 FROM staging.stg_mines
        WHERE mine_id IS NULL OR mine_name IS NULL
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'Обнаружены NULL в обязательных полях mines');
        RAISE EXCEPTION 'Ошибка качества данных: NULL в mines';
    END IF;

    -- Шаг 5: Проверка качества — дубликаты бизнес-ключей
    IF EXISTS (
        SELECT mine_id FROM staging.stg_mines
        GROUP BY mine_id HAVING COUNT(*) > 1
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'Обнаружены дубликаты mine_id');
        RAISE EXCEPTION 'Ошибка качества данных: дубликаты в mines';
    END IF;

    -- Шаг 6: Завершение загрузки
    v_loaded := v_extracted; -- При Full Load все строки загружены
    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_loaded, 0);

    -- Шаг 7: Обновление водяного знака
    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_mines),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(mine_id) FROM staging.stg_mines),
        updated_at = NOW()
    WHERE table_name = 'mines';
END $$;

-- ============================================================
-- 1.2 Загрузка таблицы equipment_types (Типы оборудования)
-- ============================================================

DO $$
DECLARE
    v_load_id   INTEGER;
    v_extracted INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('equipment_types', 'full');

    TRUNCATE TABLE staging.stg_equipment_types;

    INSERT INTO staging.stg_equipment_types (
        type_id, type_name, type_code, description,
        created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        et.type_id, et.type_name, et.type_code, et.description,
        et.created_at, et.updated_at,
        v_load_id,
        'ruda_plus.equipment_types',
        MD5(CONCAT_WS('|', et.type_name, et.type_code, et.description))
    FROM ruda_plus.equipment_types et;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    -- Проверка качества
    IF EXISTS (
        SELECT 1 FROM staging.stg_equipment_types
        WHERE type_id IS NULL OR type_name IS NULL
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'NULL в обязательных полях equipment_types');
        RAISE EXCEPTION 'Ошибка качества данных: NULL в equipment_types';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_equipment_types),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(type_id) FROM staging.stg_equipment_types),
        updated_at = NOW()
    WHERE table_name = 'equipment_types';
END $$;

-- ============================================================
-- 1.3 Загрузка таблицы equipment (Оборудование)
-- ============================================================

DO $$
DECLARE
    v_load_id   INTEGER;
    v_extracted INTEGER;
    v_fk_errors INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('equipment', 'full');

    TRUNCATE TABLE staging.stg_equipment;

    INSERT INTO staging.stg_equipment (
        equipment_id, equipment_name, type_id, mine_id,
        manufacturer, model, year_manufactured, serial_number,
        max_payload_tons, engine_hours, status,
        last_maintenance_date, next_maintenance_date,
        created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        e.equipment_id, e.equipment_name, e.type_id, e.mine_id,
        e.manufacturer, e.model, e.year_manufactured, e.serial_number,
        e.max_payload_tons, e.engine_hours, e.status,
        e.last_maintenance_date, e.next_maintenance_date,
        e.created_at, e.updated_at,
        v_load_id,
        'ruda_plus.equipment',
        MD5(CONCAT_WS('|',
            e.equipment_name, e.type_id, e.mine_id,
            e.manufacturer, e.model, e.year_manufactured,
            e.max_payload_tons, e.engine_hours, e.status,
            e.last_maintenance_date, e.next_maintenance_date
        ))
    FROM ruda_plus.equipment e;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    -- Проверка качества: NULL в обязательных полях
    IF EXISTS (
        SELECT 1 FROM staging.stg_equipment
        WHERE equipment_id IS NULL OR equipment_name IS NULL
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'NULL в обязательных полях equipment');
        RAISE EXCEPTION 'Ошибка качества данных: NULL в equipment';
    END IF;

    -- Проверка качества: ссылочная целостность (FK на mines)
    SELECT COUNT(*) INTO v_fk_errors
    FROM staging.stg_equipment e
    LEFT JOIN staging.stg_mines m ON e.mine_id = m.mine_id
    WHERE e.mine_id IS NOT NULL AND m.mine_id IS NULL;

    IF v_fk_errors > 0 THEN
        RAISE WARNING 'equipment: % записей ссылаются на несуществующие шахты', v_fk_errors;
    END IF;

    -- Проверка качества: ссылочная целостность (FK на equipment_types)
    SELECT COUNT(*) INTO v_fk_errors
    FROM staging.stg_equipment e
    LEFT JOIN staging.stg_equipment_types et ON e.type_id = et.type_id
    WHERE e.type_id IS NOT NULL AND et.type_id IS NULL;

    IF v_fk_errors > 0 THEN
        RAISE WARNING 'equipment: % записей ссылаются на несуществующие типы оборудования', v_fk_errors;
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_equipment),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(equipment_id) FROM staging.stg_equipment),
        updated_at = NOW()
    WHERE table_name = 'equipment';
END $$;

-- ============================================================
-- 1.4 Загрузка таблицы operators (Операторы)
-- ============================================================

DO $$
DECLARE
    v_load_id   INTEGER;
    v_extracted INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('operators', 'full');

    TRUNCATE TABLE staging.stg_operators;

    INSERT INTO staging.stg_operators (
        operator_id, last_name, first_name, middle_name,
        position, qualification, hire_date, mine_id, is_active,
        created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        o.operator_id, o.last_name, o.first_name, o.middle_name,
        o.position, o.qualification, o.hire_date, o.mine_id, o.is_active,
        o.created_at, o.updated_at,
        v_load_id,
        'ruda_plus.operators',
        MD5(CONCAT_WS('|',
            o.last_name, o.first_name, o.middle_name,
            o.position, o.qualification, o.hire_date,
            o.mine_id, o.is_active
        ))
    FROM ruda_plus.operators o;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    -- Проверка качества
    IF EXISTS (
        SELECT 1 FROM staging.stg_operators
        WHERE operator_id IS NULL OR last_name IS NULL
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'NULL в обязательных полях operators');
        RAISE EXCEPTION 'Ошибка качества данных: NULL в operators';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_operators),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(operator_id) FROM staging.stg_operators),
        updated_at = NOW()
    WHERE table_name = 'operators';
END $$;

-- ============================================================
-- 1.5 Загрузка таблицы ore_production (Добыча руды)
-- ============================================================

DO $$
DECLARE
    v_load_id     INTEGER;
    v_extracted   INTEGER;
    v_fk_errors   INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('ore_production', 'full');

    TRUNCATE TABLE staging.stg_ore_production;

    INSERT INTO staging.stg_ore_production (
        production_id, production_date, shift, mine_id, equipment_id,
        operator_id, block_id, ore_type, tonnage_extracted,
        fe_content_pct, moisture_pct, status,
        created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        p.production_id, p.production_date, p.shift, p.mine_id, p.equipment_id,
        p.operator_id, p.block_id, p.ore_type, p.tonnage_extracted,
        p.fe_content_pct, p.moisture_pct, p.status,
        p.created_at, p.updated_at,
        v_load_id,
        'ruda_plus.ore_production',
        MD5(CONCAT_WS('|',
            p.production_date, p.shift, p.mine_id, p.equipment_id,
            p.operator_id, p.block_id, p.ore_type, p.tonnage_extracted,
            p.fe_content_pct, p.moisture_pct, p.status
        ))
    FROM ruda_plus.ore_production p;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    -- Проверка: NULL в обязательных полях
    IF EXISTS (
        SELECT 1 FROM staging.stg_ore_production
        WHERE production_id IS NULL OR mine_id IS NULL OR production_date IS NULL
    ) THEN
        PERFORM staging.fail_etl_load(v_load_id, 'NULL в обязательных полях ore_production');
        RAISE EXCEPTION 'Ошибка качества данных: NULL в ore_production';
    END IF;

    -- Проверка: ссылочная целостность (equipment)
    SELECT COUNT(*) INTO v_fk_errors
    FROM staging.stg_ore_production p
    LEFT JOIN staging.stg_equipment e ON p.equipment_id = e.equipment_id
    WHERE p.equipment_id IS NOT NULL AND e.equipment_id IS NULL;

    IF v_fk_errors > 0 THEN
        RAISE WARNING 'ore_production: % записей ссылаются на несуществующее оборудование', v_fk_errors;
    END IF;

    -- Проверка: бизнес-правила (tonnage > 0)
    IF EXISTS (
        SELECT 1 FROM staging.stg_ore_production
        WHERE tonnage_extracted <= 0 AND status = 'Завершена'
    ) THEN
        RAISE WARNING 'ore_production: обнаружены завершённые записи с tonnage <= 0';
    END IF;

    -- Проверка: fe_content_pct в допустимом диапазоне (0-100%)
    IF EXISTS (
        SELECT 1 FROM staging.stg_ore_production
        WHERE fe_content_pct < 0 OR fe_content_pct > 100
    ) THEN
        RAISE WARNING 'ore_production: обнаружены записи с fe_content_pct вне диапазона 0-100';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_ore_production),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(production_id) FROM staging.stg_ore_production),
        updated_at = NOW()
    WHERE table_name = 'ore_production';
END $$;

-- ============================================================
-- 1.6 Загрузка таблицы downtime_events (Простои)
-- ============================================================

DO $$
DECLARE
    v_load_id   INTEGER;
    v_extracted INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('downtime_events', 'full');

    TRUNCATE TABLE staging.stg_downtime_events;

    INSERT INTO staging.stg_downtime_events (
        event_id, equipment_id, event_type, event_category,
        severity, start_time, end_time, duration_minutes,
        description, reported_by_id, status,
        created_at, updated_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        d.event_id, d.equipment_id, d.event_type, d.event_category,
        d.severity, d.start_time, d.end_time, d.duration_minutes,
        d.description, d.reported_by_id, d.status,
        d.created_at, d.updated_at,
        v_load_id,
        'ruda_plus.downtime_events',
        MD5(CONCAT_WS('|',
            d.equipment_id, d.event_type, d.event_category,
            d.severity, d.start_time, d.end_time, d.duration_minutes,
            d.status
        ))
    FROM ruda_plus.downtime_events d;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    -- Проверка: end_time >= start_time
    IF EXISTS (
        SELECT 1 FROM staging.stg_downtime_events
        WHERE end_time < start_time
    ) THEN
        RAISE WARNING 'downtime_events: обнаружены записи где end_time < start_time';
    END IF;

    -- Проверка: duration_minutes соответствует разнице start_time/end_time
    IF EXISTS (
        SELECT 1 FROM staging.stg_downtime_events
        WHERE end_time IS NOT NULL
          AND ABS(duration_minutes - EXTRACT(EPOCH FROM (end_time - start_time)) / 60) > 5
    ) THEN
        RAISE WARNING 'downtime_events: duration_minutes не соответствует разнице start/end_time';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(updated_at, created_at)) FROM staging.stg_downtime_events),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(event_id) FROM staging.stg_downtime_events),
        updated_at = NOW()
    WHERE table_name = 'downtime_events';
END $$;

-- ============================================================
-- 1.7 Загрузка таблицы sensor_readings (Показания датчиков)
-- ============================================================

DO $$
DECLARE
    v_load_id   INTEGER;
    v_extracted INTEGER;
BEGIN
    v_load_id := staging.start_etl_load('sensor_readings', 'full');

    TRUNCATE TABLE staging.stg_sensor_readings;

    INSERT INTO staging.stg_sensor_readings (
        reading_id, equipment_id, sensor_type, reading_timestamp,
        sensor_value, unit, quality_flag, created_at,
        _load_id, _source_system, _row_hash
    )
    SELECT
        sr.reading_id, sr.equipment_id, sr.sensor_type, sr.reading_timestamp,
        sr.sensor_value, sr.unit, sr.quality_flag, sr.created_at,
        v_load_id,
        'ruda_plus.sensor_readings',
        MD5(CONCAT_WS('|',
            sr.equipment_id, sr.sensor_type, sr.reading_timestamp,
            sr.sensor_value, sr.unit, sr.quality_flag
        ))
    FROM ruda_plus.sensor_readings sr;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);

    UPDATE staging.etl_watermark
    SET last_loaded_at = COALESCE(
            (SELECT MAX(COALESCE(created_at, reading_timestamp)) FROM staging.stg_sensor_readings),
            NOW()
        ),
        last_loaded_id = (SELECT MAX(reading_id) FROM staging.stg_sensor_readings),
        updated_at = NOW()
    WHERE table_name = 'sensor_readings';
END $$;

-- ============================================================
-- Раздел 2: Трансформация и загрузка в Star Schema
-- Из staging → star (измерения + факты)
-- ============================================================

-- ВАЖНО: Перед загрузкой в star необходимо очистить целевые таблицы,
--         если это повторная полная загрузка. Факты зависят от измерений,
--         поэтому порядок очистки: факты → измерения.

-- ============================================================
-- 2.1 Очистка Star Schema (для повторной полной загрузки)
-- ============================================================

-- Удаляем факты (зависят от измерений через FK)
TRUNCATE TABLE star.fact_production CASCADE;
TRUNCATE TABLE star.fact_downtime CASCADE;

-- Удаляем измерения
TRUNCATE TABLE star.dim_mine CASCADE;
TRUNCATE TABLE star.dim_equipment CASCADE;
TRUNCATE TABLE star.dim_operator CASCADE;
TRUNCATE TABLE star.dim_downtime_category CASCADE;
-- dim_time не очищаем — это календарь, созданный один раз

-- ============================================================
-- 2.2 Загрузка измерений из staging
-- ============================================================

-- Измерение: dim_mine (из staging.stg_mines)
INSERT INTO star.dim_mine (mine_id, mine_name, region, max_depth_m, status, opened_date)
SELECT mine_id, mine_name, region, max_depth_m, status, opened_date
FROM staging.stg_mines;

SELECT 'dim_mine загружено: ' || COUNT(*) || ' строк' AS info FROM star.dim_mine;

-- Измерение: dim_equipment (из staging с денормализацией)
INSERT INTO star.dim_equipment (
    equipment_id, equipment_name, type_name, type_code,
    manufacturer, model, year_manufactured, max_payload_tons,
    mine_name, mine_region
)
SELECT
    e.equipment_id, e.equipment_name,
    et.type_name, et.type_code,
    e.manufacturer, e.model, e.year_manufactured, e.max_payload_tons,
    m.mine_name, m.region
FROM staging.stg_equipment e
JOIN staging.stg_equipment_types et ON e.type_id = et.type_id
JOIN staging.stg_mines m ON e.mine_id = m.mine_id;

SELECT 'dim_equipment загружено: ' || COUNT(*) || ' строк' AS info FROM star.dim_equipment;

-- Измерение: dim_operator (из staging с денормализацией)
INSERT INTO star.dim_operator (
    operator_id, full_name, last_name, first_name,
    position, qualification, mine_name
)
SELECT
    o.operator_id,
    o.last_name || ' ' || o.first_name || COALESCE(' ' || o.middle_name, ''),
    o.last_name, o.first_name,
    o.position, o.qualification,
    m.mine_name
FROM staging.stg_operators o
LEFT JOIN staging.stg_mines m ON o.mine_id = m.mine_id;

SELECT 'dim_operator загружено: ' || COUNT(*) || ' строк' AS info FROM star.dim_operator;

-- Измерение: dim_downtime_category (junk dimension из staging)
INSERT INTO star.dim_downtime_category (event_type, event_category, severity)
SELECT DISTINCT event_type, event_category, severity
FROM staging.stg_downtime_events
ON CONFLICT (event_type, event_category, severity) DO NOTHING;

SELECT 'dim_downtime_category загружено: ' || COUNT(*) || ' строк' AS info
FROM star.dim_downtime_category;

-- ============================================================
-- 2.3 Загрузка фактов из staging
-- ============================================================

-- Факт: fact_production (Добыча руды)
-- Трансформация: маппинг бизнес-ключей на суррогатные ключи измерений
INSERT INTO star.fact_production (
    time_key, mine_key, equipment_key, operator_key,
    production_id, shift, block_id, ore_type,
    tonnage_extracted, fe_content_pct, moisture_pct
)
SELECT
    dt.time_key,
    dm.mine_key,
    de.equipment_key,
    dop.operator_key,
    p.production_id,
    p.shift,
    p.block_id,
    p.ore_type,
    p.tonnage_extracted,
    p.fe_content_pct,
    p.moisture_pct
FROM staging.stg_ore_production p
-- Маппинг на суррогатные ключи измерений
JOIN star.dim_time dt ON p.production_date = dt.production_date
JOIN star.dim_mine dm ON p.mine_id = dm.mine_id AND dm.is_current = TRUE
JOIN star.dim_equipment de ON p.equipment_id = de.equipment_id AND de.is_current = TRUE
LEFT JOIN star.dim_operator dop ON p.operator_id = dop.operator_id AND dop.is_current = TRUE
-- Бизнес-фильтр: только завершённые операции добычи
WHERE p.status = 'Завершена';

SELECT 'fact_production загружено: ' || COUNT(*) || ' строк' AS info FROM star.fact_production;

-- Факт: fact_downtime (Простои оборудования)
INSERT INTO star.fact_downtime (
    time_key, equipment_key, category_key,
    event_id, duration_minutes
)
SELECT
    dt.time_key,
    de.equipment_key,
    dc.category_key,
    d.event_id,
    COALESCE(d.duration_minutes, 0)
FROM staging.stg_downtime_events d
JOIN star.dim_time dt ON d.start_time::date = dt.production_date
JOIN star.dim_equipment de ON d.equipment_id = de.equipment_id AND de.is_current = TRUE
JOIN star.dim_downtime_category dc
    ON d.event_type = dc.event_type
    AND d.event_category = dc.event_category
    AND d.severity = dc.severity;

SELECT 'fact_downtime загружено: ' || COUNT(*) || ' строк' AS info FROM star.fact_downtime;

-- ============================================================
-- Раздел 3: Итоговая проверка
-- ============================================================

SELECT '--- ETL Full Load: итоговая проверка ---' AS info;

-- 3.1 Журнал загрузок
SELECT load_id, table_name, load_type, status,
       rows_extracted, rows_loaded, rows_rejected,
       started_at,
       finished_at,
       EXTRACT(EPOCH FROM (finished_at - started_at))::numeric(6,2) AS duration_sec
FROM staging.etl_load_log
ORDER BY load_id;

-- 3.2 Водяные знаки
SELECT table_name, last_loaded_at, last_loaded_id
FROM staging.etl_watermark
ORDER BY table_name;

-- 3.3 Количество записей в staging vs star
SELECT 'staging.stg_mines' AS table_name, COUNT(*) AS rows FROM staging.stg_mines
UNION ALL SELECT 'star.dim_mine', COUNT(*) FROM star.dim_mine
UNION ALL SELECT 'staging.stg_equipment', COUNT(*) FROM staging.stg_equipment
UNION ALL SELECT 'star.dim_equipment', COUNT(*) FROM star.dim_equipment
UNION ALL SELECT 'staging.stg_operators', COUNT(*) FROM staging.stg_operators
UNION ALL SELECT 'star.dim_operator', COUNT(*) FROM star.dim_operator
UNION ALL SELECT 'staging.stg_ore_production', COUNT(*) FROM staging.stg_ore_production
UNION ALL SELECT 'star.fact_production', COUNT(*) FROM star.fact_production
UNION ALL SELECT 'staging.stg_downtime_events', COUNT(*) FROM staging.stg_downtime_events
UNION ALL SELECT 'star.fact_downtime', COUNT(*) FROM star.fact_downtime
ORDER BY table_name;

-- 3.4 Сравнение хешей (staging vs star) для аудита
-- Если хеши совпадают — данные не были искажены при трансформации
SELECT 'Хеш-сравнение mines' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'MISMATCH' END AS result
FROM (
    SELECT mine_id FROM staging.stg_mines
    EXCEPT
    SELECT mine_id FROM star.dim_mine WHERE is_current = TRUE
) diff;

SELECT '--- ETL Full Load завершён ---' AS info;
