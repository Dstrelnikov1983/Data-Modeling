-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 4. Моделирование потоковых и пакетных данных
-- Скрипт 3: Инкрементальная загрузка с SCD Type 2
-- Предприятие: "Руда+" — добыча железной руды
--
-- ВАЖНО: Сначала выполните скрипты 01 и 02.
--         Полная загрузка (Full Load) уже выполнена.
--
-- Этот скрипт демонстрирует:
-- 1. Имитацию новых данных в OLTP-источнике
-- 2. Инкрементальное извлечение (только изменения)
-- 3. SCD Type 2 обновление измерений
-- 4. Инкрементальная загрузка фактов
-- 5. Обновление водяных знаков
-- ============================================================

SET search_path TO staging, ruda_plus, star, public;

-- ============================================================
-- Раздел 1: Имитация изменений в OLTP-источнике
-- В реальной MES-системе эти данные поступают непрерывно
-- ============================================================

SELECT '=== Раздел 1: Имитация новых данных в OLTP ===' AS info;

-- 1.1 Добавляем 3 новые записи о добыче руды
-- (имитация: MES зафиксировала новые смены)

-- Проверяем, не были ли уже добавлены тестовые данные
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ruda_plus.ore_production WHERE production_id = 'PRD-016') THEN
        INSERT INTO ruda_plus.ore_production (
            production_id, production_date, shift, mine_id, equipment_id,
            operator_id, block_id, ore_type, tonnage_extracted,
            fe_content_pct, moisture_pct, status, created_at
        )
        VALUES
            -- Новая запись 1: добыча 16 марта, 1-я смена
            ('PRD-016', '2025-03-16', 1, 'MINE-01', 'EQ-001',
             'OP-001', 'BLK-H1-003', 'Магнетит', 185.0,
             62.5, 4.1, 'Завершена', NOW()),
            -- Новая запись 2: добыча 16 марта, 2-я смена
            ('PRD-017', '2025-03-16', 2, 'MINE-01', 'EQ-003',
             'OP-003', 'BLK-H1-004', 'Гематит', 142.0,
             58.3, 5.2, 'Завершена', NOW()),
            -- Новая запись 3: добыча 16 марта, 3-я смена (незавершена — не должна попасть в star)
            ('PRD-018', '2025-03-16', 3, 'MINE-02', 'EQ-005',
             'OP-005', 'BLK-H2-002', 'Магнетит', 95.0,
             60.1, 4.8, 'В работе', NOW());

        RAISE NOTICE 'Добавлены 3 новые записи добычи (PRD-016, PRD-017, PRD-018)';
    ELSE
        RAISE NOTICE 'Тестовые записи добычи уже существуют — пропускаем вставку';
    END IF;
END $$;

-- 1.2 Добавляем 2 новых события простоя
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ruda_plus.downtime_events WHERE event_id = 'EVT-011') THEN
        INSERT INTO ruda_plus.downtime_events (
            event_id, equipment_id, event_type, event_category,
            severity, start_time, end_time, duration_minutes,
            description, reported_by_id, status, created_at
        )
        VALUES
            ('EVT-011', 'EQ-002', 'Поломка', 'Механическая', 'Высокая',
             '2025-03-16 10:30:00', '2025-03-16 14:15:00', 225,
             'Разрыв гидравлического шланга ПДМ-2', 'OP-002', 'Закрыто', NOW()),
            ('EVT-012', 'EQ-004', 'Плановое ТО', 'Техобслуживание', 'Низкая',
             '2025-03-16 06:00:00', '2025-03-16 08:00:00', 120,
             'Плановая замена масла и фильтров', 'OP-004', 'Закрыто', NOW());

        RAISE NOTICE 'Добавлены 2 новых события простоя (EVT-011, EVT-012)';
    ELSE
        RAISE NOTICE 'Тестовые события простоя уже существуют — пропускаем вставку';
    END IF;
END $$;

-- 1.3 Изменение данных оператора (для демонстрации SCD Type 2)
-- Оператор OP-007 повысил квалификацию с "4 разряд" до "5 разряд"
DO $$
BEGIN
    -- Обновляем оператора в OLTP (MES зафиксировала изменение)
    UPDATE ruda_plus.operators
    SET qualification = '5 разряд',
        updated_at = NOW()
    WHERE operator_id = 'OP-007'
      AND qualification = '4 разряд';

    IF FOUND THEN
        RAISE NOTICE 'Оператор OP-007: квалификация обновлена на "5 разряд"';
    ELSE
        RAISE NOTICE 'Оператор OP-007: квалификация уже была обновлена ранее';
    END IF;
END $$;

-- 1.4 Проверяем текущее состояние источника
SELECT '--- Новые записи в ore_production ---' AS info;
SELECT production_id, production_date, shift, mine_id, status, created_at
FROM ruda_plus.ore_production
ORDER BY created_at DESC
LIMIT 5;

SELECT '--- Обновлённый оператор ---' AS info;
SELECT operator_id, last_name, first_name, qualification, updated_at
FROM ruda_plus.operators
WHERE operator_id = 'OP-007';

-- ============================================================
-- Раздел 2: Инкрементальное извлечение из OLTP
-- Извлекаем только записи, изменившиеся после последней загрузки
-- ============================================================

SELECT '=== Раздел 2: Инкрементальное извлечение ===' AS info;

-- 2.1 Проверяем текущие водяные знаки
SELECT table_name, last_loaded_at, last_loaded_id
FROM staging.etl_watermark
ORDER BY table_name;

-- 2.2 Инкрементальное извлечение ore_production
DO $$
DECLARE
    v_load_id       INTEGER;
    v_watermark     TIMESTAMP;
    v_extracted     INTEGER;
    v_new_watermark TIMESTAMP;
BEGIN
    -- Получаем текущий водяной знак
    SELECT last_loaded_at INTO v_watermark
    FROM staging.etl_watermark
    WHERE table_name = 'ore_production';

    RAISE NOTICE 'ore_production: водяной знак = %', v_watermark;

    -- Регистрируем инкрементальную загрузку
    v_load_id := staging.start_etl_load('ore_production', 'incremental');

    -- НЕ очищаем staging при инкрементальной загрузке!
    -- Вместо TRUNCATE используем DELETE для старых данных с тем же load_id
    -- (или просто добавляем новые)

    -- Извлекаем только новые/изменённые записи
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
    FROM ruda_plus.ore_production p
    -- Ключевое условие: только записи новее водяного знака
    WHERE COALESCE(p.updated_at, p.created_at) > v_watermark;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;
    RAISE NOTICE 'ore_production: извлечено % новых записей', v_extracted;

    -- Обновляем водяной знак
    SELECT MAX(COALESCE(updated_at, created_at)) INTO v_new_watermark
    FROM staging.stg_ore_production
    WHERE _load_id = v_load_id;

    IF v_new_watermark IS NOT NULL THEN
        UPDATE staging.etl_watermark
        SET last_loaded_at = v_new_watermark,
            last_loaded_id = (
                SELECT MAX(production_id)
                FROM staging.stg_ore_production
                WHERE _load_id = v_load_id
            ),
            updated_at = NOW()
        WHERE table_name = 'ore_production';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);
END $$;

-- 2.3 Инкрементальное извлечение downtime_events
DO $$
DECLARE
    v_load_id       INTEGER;
    v_watermark     TIMESTAMP;
    v_extracted     INTEGER;
    v_new_watermark TIMESTAMP;
BEGIN
    SELECT last_loaded_at INTO v_watermark
    FROM staging.etl_watermark
    WHERE table_name = 'downtime_events';

    v_load_id := staging.start_etl_load('downtime_events', 'incremental');

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
    FROM ruda_plus.downtime_events d
    WHERE COALESCE(d.updated_at, d.created_at) > v_watermark;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;
    RAISE NOTICE 'downtime_events: извлечено % новых записей', v_extracted;

    SELECT MAX(COALESCE(updated_at, created_at)) INTO v_new_watermark
    FROM staging.stg_downtime_events
    WHERE _load_id = v_load_id;

    IF v_new_watermark IS NOT NULL THEN
        UPDATE staging.etl_watermark
        SET last_loaded_at = v_new_watermark,
            last_loaded_id = (
                SELECT MAX(event_id)
                FROM staging.stg_downtime_events
                WHERE _load_id = v_load_id
            ),
            updated_at = NOW()
        WHERE table_name = 'downtime_events';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);
END $$;

-- 2.4 Инкрементальное извлечение операторов (для SCD Type 2)
DO $$
DECLARE
    v_load_id       INTEGER;
    v_watermark     TIMESTAMP;
    v_extracted     INTEGER;
    v_new_watermark TIMESTAMP;
BEGIN
    SELECT last_loaded_at INTO v_watermark
    FROM staging.etl_watermark
    WHERE table_name = 'operators';

    v_load_id := staging.start_etl_load('operators', 'incremental');

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
    FROM ruda_plus.operators o
    WHERE COALESCE(o.updated_at, o.created_at) > v_watermark;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;
    RAISE NOTICE 'operators: извлечено % изменённых записей', v_extracted;

    SELECT MAX(COALESCE(updated_at, created_at)) INTO v_new_watermark
    FROM staging.stg_operators
    WHERE _load_id = v_load_id;

    IF v_new_watermark IS NOT NULL THEN
        UPDATE staging.etl_watermark
        SET last_loaded_at = v_new_watermark,
            last_loaded_id = (
                SELECT MAX(operator_id)
                FROM staging.stg_operators
                WHERE _load_id = v_load_id
            ),
            updated_at = NOW()
        WHERE table_name = 'operators';
    END IF;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, v_extracted, 0);
END $$;

-- ============================================================
-- Раздел 3: SCD Type 2 — обновление измерения dim_operator
-- При изменении атрибутов оператора создаём новую версию
-- ============================================================

SELECT '=== Раздел 3: SCD Type 2 для dim_operator ===' AS info;

-- 3.1 Показываем текущее состояние dim_operator для OP-007
SELECT '--- До SCD Type 2 обновления ---' AS info;
SELECT operator_key, operator_id, full_name, qualification,
       effective_from, effective_to, is_current
FROM star.dim_operator
WHERE operator_id = 'OP-007'
ORDER BY effective_from;

-- 3.2 Выполняем SCD Type 2 обновление
-- Принцип: сравниваем хеш строки из staging с текущей версией в star
-- Если хеш отличается — закрываем текущую версию и создаём новую

DO $$
DECLARE
    v_operator RECORD;
    v_current_hash CHAR(32);
    v_new_hash     CHAR(32);
    v_updated_count INTEGER := 0;
BEGIN
    -- Перебираем все изменённые записи операторов из последнего инкрементального извлечения
    FOR v_operator IN
        SELECT o.operator_id, o.last_name, o.first_name, o.middle_name,
               o.position, o.qualification, o.mine_id, o._row_hash
        FROM staging.stg_operators o
        WHERE o._load_id = (
            SELECT MAX(load_id) FROM staging.etl_load_log
            WHERE table_name = 'operators' AND load_type = 'incremental'
        )
    LOOP
        -- Получаем хеш текущей версии в Star Schema
        SELECT MD5(CONCAT_WS('|',
            do2.last_name, do2.first_name, NULL,
            do2.position, do2.qualification, NULL,
            NULL, NULL
        )) INTO v_current_hash
        FROM star.dim_operator do2
        WHERE do2.operator_id = v_operator.operator_id
          AND do2.is_current = TRUE;

        v_new_hash := v_operator._row_hash;

        -- Если хеши различаются — данные изменились
        IF v_current_hash IS DISTINCT FROM v_new_hash THEN
            RAISE NOTICE 'SCD Type 2: изменение обнаружено для оператора %', v_operator.operator_id;

            -- Шаг А: Закрываем текущую версию
            UPDATE star.dim_operator
            SET effective_to = CURRENT_DATE,
                is_current = FALSE
            WHERE operator_id = v_operator.operator_id
              AND is_current = TRUE;

            -- Шаг Б: Создаём новую версию с обновлёнными данными
            INSERT INTO star.dim_operator (
                operator_id, full_name, last_name, first_name,
                position, qualification, mine_name,
                effective_from, effective_to, is_current
            )
            SELECT
                v_operator.operator_id,
                v_operator.last_name || ' ' || v_operator.first_name
                    || COALESCE(' ' || v_operator.middle_name, ''),
                v_operator.last_name,
                v_operator.first_name,
                v_operator.position,
                v_operator.qualification,
                m.mine_name,
                CURRENT_DATE,     -- Новая версия начинается сегодня
                '9999-12-31',     -- Бесконечность (текущая версия)
                TRUE
            FROM staging.stg_mines m
            WHERE m.mine_id = v_operator.mine_id;

            v_updated_count := v_updated_count + 1;
        END IF;
    END LOOP;

    RAISE NOTICE 'SCD Type 2: обновлено % измерений оператора', v_updated_count;
END $$;

-- 3.3 Проверяем результат SCD Type 2
SELECT '--- После SCD Type 2 обновления ---' AS info;
SELECT operator_key, operator_id, full_name, qualification,
       effective_from, effective_to, is_current
FROM star.dim_operator
WHERE operator_id = 'OP-007'
ORDER BY effective_from;

-- ============================================================
-- Раздел 4: Инкрементальная загрузка фактов
-- Загружаем только новые записи из staging в star
-- ============================================================

SELECT '=== Раздел 4: Инкрементальная загрузка фактов ===' AS info;

-- 4.1 Загрузка новых фактов добычи
-- Используем production_id для определения новых записей (отсутствующих в star)
DO $$
DECLARE
    v_new_facts INTEGER;
BEGIN
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
    -- Маппинг суррогатных ключей
    JOIN star.dim_time dt ON p.production_date = dt.production_date
    JOIN star.dim_mine dm ON p.mine_id = dm.mine_id AND dm.is_current = TRUE
    JOIN star.dim_equipment de ON p.equipment_id = de.equipment_id AND de.is_current = TRUE
    LEFT JOIN star.dim_operator dop ON p.operator_id = dop.operator_id AND dop.is_current = TRUE
    -- Только новые инкрементальные записи
    WHERE p._load_id = (
        SELECT MAX(load_id) FROM staging.etl_load_log
        WHERE table_name = 'ore_production' AND load_type = 'incremental'
    )
    -- Бизнес-фильтр: только завершённые операции
    AND p.status = 'Завершена'
    -- Защита от дубликатов: не загружать уже существующие в star
    AND p.production_id NOT IN (SELECT production_id FROM star.fact_production);

    GET DIAGNOSTICS v_new_facts = ROW_COUNT;
    RAISE NOTICE 'fact_production: загружено % новых фактов', v_new_facts;
END $$;

-- 4.2 Загрузка новых фактов простоев
DO $$
DECLARE
    v_new_facts INTEGER;
BEGIN
    -- Добавляем новые категории простоев (если появились)
    INSERT INTO star.dim_downtime_category (event_type, event_category, severity)
    SELECT DISTINCT d.event_type, d.event_category, d.severity
    FROM staging.stg_downtime_events d
    WHERE d._load_id = (
        SELECT MAX(load_id) FROM staging.etl_load_log
        WHERE table_name = 'downtime_events' AND load_type = 'incremental'
    )
    ON CONFLICT (event_type, event_category, severity) DO NOTHING;

    -- Загружаем факты простоев
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
        AND d.severity = dc.severity
    WHERE d._load_id = (
        SELECT MAX(load_id) FROM staging.etl_load_log
        WHERE table_name = 'downtime_events' AND load_type = 'incremental'
    )
    AND d.event_id NOT IN (SELECT event_id FROM star.fact_downtime);

    GET DIAGNOSTICS v_new_facts = ROW_COUNT;
    RAISE NOTICE 'fact_downtime: загружено % новых фактов', v_new_facts;
END $$;

-- ============================================================
-- Раздел 5: Итоговая проверка инкрементальной загрузки
-- ============================================================

SELECT '=== Раздел 5: Проверка результатов ===' AS info;

-- 5.1 Журнал загрузок (должны быть и full, и incremental записи)
SELECT load_id, table_name, load_type, status,
       rows_extracted, rows_loaded,
       started_at, finished_at
FROM staging.etl_load_log
ORDER BY load_id;

-- 5.2 Обновлённые водяные знаки
SELECT table_name, last_loaded_at, last_loaded_id, updated_at
FROM staging.etl_watermark
ORDER BY table_name;

-- 5.3 Новые факты добычи (должны появиться PRD-016 и PRD-017, но НЕ PRD-018)
SELECT '--- Факты добычи (последние 5) ---' AS info;
SELECT fp.production_id, dt.production_date, fp.shift,
       dm.mine_name, de.equipment_name,
       fp.tonnage_extracted, fp.fe_content_pct
FROM star.fact_production fp
JOIN star.dim_time dt ON fp.time_key = dt.time_key
JOIN star.dim_mine dm ON fp.mine_key = dm.mine_key
JOIN star.dim_equipment de ON fp.equipment_key = de.equipment_key
ORDER BY dt.production_date DESC, fp.shift DESC
LIMIT 5;

-- 5.4 Новые факты простоев
SELECT '--- Факты простоев (последние 5) ---' AS info;
SELECT fd.event_id, dt.production_date,
       de.equipment_name,
       dc.event_type, dc.event_category,
       fd.duration_minutes
FROM star.fact_downtime fd
JOIN star.dim_time dt ON fd.time_key = dt.time_key
JOIN star.dim_equipment de ON fd.equipment_key = de.equipment_key
JOIN star.dim_downtime_category dc ON fd.category_key = dc.category_key
ORDER BY dt.production_date DESC
LIMIT 5;

-- 5.5 SCD Type 2 — все версии OP-007
SELECT '--- SCD Type 2 для OP-007 ---' AS info;
SELECT operator_key, operator_id, full_name, qualification,
       effective_from, effective_to, is_current
FROM star.dim_operator
WHERE operator_id = 'OP-007'
ORDER BY effective_from;

-- 5.6 Общая статистика Star Schema после инкрементальной загрузки
SELECT '--- Итоговая статистика Star Schema ---' AS info;
SELECT 'dim_time' AS table_name, COUNT(*) AS rows FROM star.dim_time
UNION ALL SELECT 'dim_mine', COUNT(*) FROM star.dim_mine
UNION ALL SELECT 'dim_equipment', COUNT(*) FROM star.dim_equipment
UNION ALL SELECT 'dim_operator', COUNT(*) FROM star.dim_operator
UNION ALL SELECT 'dim_downtime_category', COUNT(*) FROM star.dim_downtime_category
UNION ALL SELECT 'fact_production', COUNT(*) FROM star.fact_production
UNION ALL SELECT 'fact_downtime', COUNT(*) FROM star.fact_downtime
ORDER BY table_name;

-- ============================================================
-- Раздел 6: Демонстрация идемпотентности
-- Повторный запуск инкрементальной загрузки не должен
-- создавать дубликатов благодаря проверке NOT IN
-- ============================================================

SELECT '=== Раздел 6: Проверка идемпотентности ===' AS info;

-- Запомним текущее количество строк
SELECT 'fact_production ДО повтора' AS check_name, COUNT(*) AS rows
FROM star.fact_production;

-- Попробуем ещё раз загрузить те же данные
-- (water mark уже сдвинут → новых записей не будет)
DO $$
DECLARE
    v_load_id   INTEGER;
    v_watermark TIMESTAMP;
    v_extracted INTEGER;
BEGIN
    SELECT last_loaded_at INTO v_watermark
    FROM staging.etl_watermark
    WHERE table_name = 'ore_production';

    v_load_id := staging.start_etl_load('ore_production', 'incremental');

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
    FROM ruda_plus.ore_production p
    WHERE COALESCE(p.updated_at, p.created_at) > v_watermark;

    GET DIAGNOSTICS v_extracted = ROW_COUNT;
    RAISE NOTICE 'Повторное извлечение: % записей (ожидается 0)', v_extracted;

    PERFORM staging.finish_etl_load(v_load_id, v_extracted, 0, 0);
END $$;

SELECT 'fact_production ПОСЛЕ повтора' AS check_name, COUNT(*) AS rows
FROM star.fact_production;

-- Результат: количество строк не изменилось — идемпотентность обеспечена!

SELECT '=== Инкрементальная загрузка завершена ===' AS info;
