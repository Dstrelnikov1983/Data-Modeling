-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 3. Практическая работа: OLTP, OLAP, Data Vault
-- Скрипт 4: Аналитические запросы по трём моделям
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

-- ============================================================
-- ЧАСТЬ А. Запросы к OLTP-модели (3НФ)
-- Схема: ruda_plus
-- ============================================================

SET search_path TO ruda_plus, public;

-- A.1. Добыча по шахтам с именами операторов и типами оборудования
-- Требуется 4 JOIN — цена нормализации
SELECT '--- A.1. OLTP: Добыча по шахтам (4 JOIN) ---' AS info;
SELECT m.mine_name      AS "Шахта",
       et.type_name     AS "Тип оборудования",
       o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS "Оператор",
       p.production_date AS "Дата",
       p.shift           AS "Смена",
       p.tonnage_extracted AS "Тоннаж",
       p.fe_content_pct  AS "Fe%"
FROM ore_production p
JOIN mines m ON p.mine_id = m.mine_id
JOIN equipment e ON p.equipment_id = e.equipment_id
JOIN equipment_types et ON e.type_id = et.type_id
LEFT JOIN operators o ON p.operator_id = o.operator_id
WHERE p.status = 'Завершена'
ORDER BY p.production_date, p.shift;

-- A.2. Суммарная добыча по шахтам
SELECT '--- A.2. OLTP: Суммарная добыча по шахтам ---' AS info;
SELECT m.mine_name,
       COUNT(*) AS shifts,
       ROUND(SUM(p.tonnage_extracted), 1) AS total_tons,
       ROUND(AVG(p.fe_content_pct), 2) AS avg_fe_pct
FROM ore_production p
JOIN mines m ON p.mine_id = m.mine_id
WHERE p.status = 'Завершена'
GROUP BY m.mine_name
ORDER BY total_tons DESC;

-- ============================================================
-- ЧАСТЬ Б. Запросы к Star Schema (Кимбалл)
-- Схема: star
-- ============================================================

SET search_path TO star, public;

-- Б.1. Та же добыча по шахтам — но проще!
-- Всего 2 JOIN (факт → dim_mine, факт → dim_time)
SELECT '--- Б.1. Star: Добыча по шахтам (2 JOIN) ---' AS info;
SELECT dm.mine_name     AS "Шахта",
       dm.region        AS "Регион",
       COUNT(*)         AS "Смен",
       ROUND(SUM(fp.tonnage_extracted), 1) AS "Тоннаж",
       ROUND(AVG(fp.fe_content_pct), 2)   AS "Среднее Fe%"
FROM fact_production fp
JOIN dim_mine dm ON fp.mine_key = dm.mine_key
GROUP BY dm.mine_name, dm.region
ORDER BY "Тоннаж" DESC;

-- Б.2. Добыча по операторам с квалификацией — 2 JOIN
SELECT '--- Б.2. Star: Производительность операторов ---' AS info;
SELECT dop.full_name      AS "Оператор",
       dop.qualification  AS "Квалификация",
       dop.mine_name      AS "Шахта",
       COUNT(*)           AS "Смен",
       ROUND(SUM(fp.tonnage_extracted), 1)  AS "Тоннаж",
       ROUND(AVG(fp.tonnage_extracted), 1)  AS "Среднее за смену"
FROM fact_production fp
JOIN dim_operator dop ON fp.operator_key = dop.operator_key
GROUP BY dop.full_name, dop.qualification, dop.mine_name
ORDER BY "Тоннаж" DESC;

-- Б.3. Добыча по месяцам (аналитика по времени)
SELECT '--- Б.3. Star: Добыча по месяцам ---' AS info;
SELECT dt.year            AS "Год",
       dt.month           AS "Месяц",
       dt.month_name      AS "Название месяца",
       COUNT(*)           AS "Смен",
       ROUND(SUM(fp.tonnage_extracted), 1) AS "Тоннаж",
       ROUND(AVG(fp.fe_content_pct), 2)    AS "Среднее Fe%"
FROM fact_production fp
JOIN dim_time dt ON fp.time_key = dt.time_key
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- Б.4. Анализ простоев по типам оборудования
SELECT '--- Б.4. Star: Простои по оборудованию ---' AS info;
SELECT de.type_name       AS "Тип оборудования",
       de.mine_name       AS "Шахта",
       dc.event_type      AS "Тип события",
       dc.severity        AS "Серьёзность",
       COUNT(*)           AS "Кол-во",
       SUM(fd.duration_minutes) AS "Всего минут",
       ROUND(AVG(fd.duration_minutes), 0) AS "Среднее, мин"
FROM fact_downtime fd
JOIN dim_equipment de ON fd.equipment_key = de.equipment_key
JOIN dim_downtime_category dc ON fd.category_key = dc.category_key
GROUP BY de.type_name, de.mine_name, dc.event_type, dc.severity
ORDER BY "Всего минут" DESC;

-- Б.5. Кросс-витринный запрос: добыча + простои
-- Conformed Dimensions позволяют объединять факты!
SELECT '--- Б.5. Star: Кросс-витринный отчёт ---' AS info;
SELECT de.equipment_name,
       de.type_name,
       de.mine_name,
       COALESCE(prod.total_tons, 0) AS "Добыча, т",
       COALESCE(down.total_downtime_min, 0) AS "Простои, мин",
       COALESCE(down.downtime_events, 0) AS "Событий простоя"
FROM dim_equipment de
LEFT JOIN (
    SELECT equipment_key,
           ROUND(SUM(tonnage_extracted), 1) AS total_tons
    FROM fact_production
    GROUP BY equipment_key
) prod ON de.equipment_key = prod.equipment_key
LEFT JOIN (
    SELECT equipment_key,
           SUM(duration_minutes) AS total_downtime_min,
           COUNT(*) AS downtime_events
    FROM fact_downtime
    GROUP BY equipment_key
) down ON de.equipment_key = down.equipment_key
WHERE de.is_current = TRUE
ORDER BY "Добыча, т" DESC NULLS LAST;

-- ============================================================
-- ЧАСТЬ В. Запросы к Data Vault
-- Схема: vault
-- ============================================================

SET search_path TO vault, public;

-- В.1. Получить текущие данные оборудования (через Hub + Satellite)
SELECT '--- В.1. Vault: Текущее состояние оборудования ---' AS info;
SELECT he.equipment_id,
       sd.equipment_name,
       sd.manufacturer,
       sd.model,
       ss.status,
       ss.engine_hours,
       ss.last_maintenance
FROM hub_equipment he
JOIN sat_equipment_details sd
    ON he.hub_equipment_hk = sd.hub_equipment_hk
    AND sd.load_end_dts = '9999-12-31'
JOIN sat_equipment_status ss
    ON he.hub_equipment_hk = ss.hub_equipment_hk
    AND ss.load_end_dts = '9999-12-31'
ORDER BY he.equipment_id;

-- В.2. Оборудование по шахтам (через Hub + Link + Hub)
SELECT '--- В.2. Vault: Оборудование по шахтам ---' AS info;
SELECT sm.mine_name,
       sd.equipment_name,
       sd.manufacturer,
       ss.status,
       ss.engine_hours
FROM link_equipment_mine lem
JOIN hub_mine hm ON lem.hub_mine_hk = hm.hub_mine_hk
JOIN sat_mine_details sm ON hm.hub_mine_hk = sm.hub_mine_hk AND sm.load_end_dts = '9999-12-31'
JOIN hub_equipment he ON lem.hub_equipment_hk = he.hub_equipment_hk
JOIN sat_equipment_details sd ON he.hub_equipment_hk = sd.hub_equipment_hk AND sd.load_end_dts = '9999-12-31'
JOIN sat_equipment_status ss ON he.hub_equipment_hk = ss.hub_equipment_hk AND ss.load_end_dts = '9999-12-31'
ORDER BY sm.mine_name, sd.equipment_name;

-- В.3. Добыча (через Link + Satellites)
SELECT '--- В.3. Vault: Данные добычи ---' AS info;
SELECT sm.mine_name      AS "Шахта",
       sd.equipment_name AS "Оборудование",
       so.last_name || ' ' || LEFT(so.first_name, 1) || '.' AS "Оператор",
       spm.production_date AS "Дата",
       spm.shift          AS "Смена",
       spm.tonnage_extracted AS "Тоннаж",
       spm.fe_content_pct AS "Fe%"
FROM link_production lp
JOIN hub_mine hm ON lp.hub_mine_hk = hm.hub_mine_hk
JOIN sat_mine_details sm ON hm.hub_mine_hk = sm.hub_mine_hk AND sm.load_end_dts = '9999-12-31'
JOIN hub_equipment he ON lp.hub_equipment_hk = he.hub_equipment_hk
JOIN sat_equipment_details sd ON he.hub_equipment_hk = sd.hub_equipment_hk AND sd.load_end_dts = '9999-12-31'
LEFT JOIN hub_operator ho ON lp.hub_operator_hk = ho.hub_operator_hk
LEFT JOIN sat_operator_details so ON ho.hub_operator_hk = so.hub_operator_hk AND so.load_end_dts = '9999-12-31'
JOIN sat_production_metrics spm ON lp.link_production_hk = spm.link_production_hk AND spm.load_end_dts = '9999-12-31'
WHERE spm.status = 'Завершена'
ORDER BY spm.production_date, spm.shift;

-- ============================================================
-- ЧАСТЬ Г. Сравнение подходов
-- ============================================================

SELECT '--- Г. Сравнение: количество JOIN для одного отчёта ---' AS info;
SELECT 'OLTP (3НФ)' AS model, '4 JOIN'  AS joins, 'Нормализовано, целостно' AS comment
UNION ALL
SELECT 'Star Schema', '2 JOIN', 'Денормализовано, быстро для BI'
UNION ALL
SELECT 'Data Vault', '7+ JOIN', 'Полная трассировка, аудит';

-- Подсчёт таблиц по схемам
SELECT '--- Г. Количество таблиц по моделям ---' AS info;
SELECT 'ruda_plus (OLTP)' AS schema_name,
       COUNT(*) AS table_count
FROM information_schema.tables
WHERE table_schema = 'ruda_plus'
UNION ALL
SELECT 'star (Кимбалл)',
       COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'star'
UNION ALL
SELECT 'vault (Data Vault)',
       COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'vault'
ORDER BY schema_name;
