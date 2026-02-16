-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 1. Практическая работа: Обзор структур хранения
-- Скрипт 3: Аналитические SQL-запросы
-- ============================================================

SET search_path TO ruda_plus, public;

-- ============================================================
-- ЧАСТЬ 1: Базовые запросы к справочнику оборудования
-- ============================================================

-- 1.1. Все оборудование, сгруппированное по типу
SELECT
    equipment_type,
    COUNT(*)              AS quantity,
    STRING_AGG(equipment_name, ', ') AS equipment_list
FROM equipment
GROUP BY equipment_type
ORDER BY quantity DESC;

-- 1.2. Оборудование, которому скоро потребуется ТО (в ближайшие 30 дней)
SELECT
    equipment_id,
    equipment_name,
    equipment_type,
    mine_name,
    next_maintenance_date,
    next_maintenance_date - CURRENT_DATE AS days_until_maintenance
FROM equipment
WHERE next_maintenance_date <= CURRENT_DATE + INTERVAL '30 days'
  AND status = 'В работе'
ORDER BY next_maintenance_date;

-- 1.3. Средняя наработка двигателя по производителю (исключая вагонетки)
SELECT
    manufacturer,
    COUNT(*)                          AS equipment_count,
    ROUND(AVG(engine_hours), 0)       AS avg_engine_hours,
    MAX(engine_hours)                 AS max_engine_hours
FROM equipment
WHERE engine_hours > 0
GROUP BY manufacturer
ORDER BY avg_engine_hours DESC;


-- ============================================================
-- ЧАСТЬ 2: Анализ показаний датчиков (телеметрия)
-- ============================================================

-- 2.1. Количество показаний по типу датчика и флагу качества
SELECT
    sensor_type,
    quality_flag,
    COUNT(*)                    AS readings_count,
    ROUND(AVG(reading_value), 2) AS avg_value,
    MIN(reading_value)          AS min_value,
    MAX(reading_value)          AS max_value
FROM sensor_readings
GROUP BY sensor_type, quality_flag
ORDER BY sensor_type, quality_flag;

-- 2.2. Аварийные и предупредительные показания с информацией об оборудовании
SELECT
    sr.reading_timestamp,
    e.equipment_name,
    e.equipment_type,
    e.mine_name,
    sr.sensor_type,
    sr.reading_value,
    sr.unit,
    sr.quality_flag
FROM sensor_readings sr
JOIN equipment e ON sr.equipment_id = e.equipment_id
WHERE sr.quality_flag IN ('WARN', 'ALARM')
ORDER BY sr.reading_timestamp;

-- 2.3. Динамика температуры двигателя ПДМ-01 (видим нарастание проблемы)
SELECT
    reading_timestamp,
    reading_value AS temperature,
    unit,
    quality_flag,
    reading_value - LAG(reading_value) OVER (ORDER BY reading_timestamp) AS delta
FROM sensor_readings
WHERE equipment_id = 'EQ-001'
  AND sensor_type = 'Температура двигателя'
ORDER BY reading_timestamp;


-- ============================================================
-- ЧАСТЬ 3: Анализ добычи руды
-- ============================================================

-- 3.1. Суммарная добыча по шахтам и дням
SELECT
    mine_name,
    production_date,
    COUNT(*)                            AS shifts_worked,
    ROUND(SUM(tonnage_extracted), 1)    AS total_tonnage,
    ROUND(AVG(fe_content_pct), 2)       AS avg_fe_content,
    ROUND(AVG(moisture_pct), 2)         AS avg_moisture
FROM ore_production
WHERE status = 'Завершена'
GROUP BY mine_name, production_date
ORDER BY mine_name, production_date;

-- 3.2. Производительность по операторам
SELECT
    operator_name,
    COUNT(*)                            AS shifts_count,
    ROUND(SUM(tonnage_extracted), 1)    AS total_tonnage,
    ROUND(AVG(tonnage_extracted), 1)    AS avg_tonnage_per_shift,
    ROUND(AVG(fe_content_pct), 2)       AS avg_fe_content
FROM ore_production
WHERE status = 'Завершена'
GROUP BY operator_name
ORDER BY total_tonnage DESC;

-- 3.3. Сравнение типов руды
SELECT
    ore_type,
    COUNT(*)                            AS extractions,
    ROUND(SUM(tonnage_extracted), 1)    AS total_tonnage,
    ROUND(AVG(fe_content_pct), 2)       AS avg_fe_pct,
    ROUND(MIN(fe_content_pct), 2)       AS min_fe_pct,
    ROUND(MAX(fe_content_pct), 2)       AS max_fe_pct
FROM ore_production
WHERE status = 'Завершена'
GROUP BY ore_type;


-- ============================================================
-- ЧАСТЬ 4: Анализ простоев
-- ============================================================

-- 4.1. Общая статистика простоев по типам
SELECT
    event_type,
    COUNT(*)                            AS events_count,
    SUM(duration_minutes)               AS total_minutes,
    ROUND(AVG(duration_minutes), 0)     AS avg_minutes,
    ROUND(SUM(duration_minutes) / 60.0, 1) AS total_hours
FROM downtime_events
GROUP BY event_type;

-- 4.2. ТОП оборудования по незапланированным простоям
SELECT
    e.equipment_name,
    e.equipment_type,
    e.mine_name,
    COUNT(d.event_id)                         AS breakdown_count,
    SUM(d.duration_minutes)                   AS total_downtime_min,
    ROUND(SUM(d.duration_minutes) / 60.0, 1)  AS total_downtime_hours
FROM downtime_events d
JOIN equipment e ON d.equipment_id = e.equipment_id
WHERE d.event_type = 'Незапланированный'
GROUP BY e.equipment_name, e.equipment_type, e.mine_name
ORDER BY total_downtime_min DESC;

-- 4.3. Категории поломок по частоте
SELECT
    event_category,
    COUNT(*)                    AS events_count,
    STRING_AGG(DISTINCT severity, ', ') AS severities,
    SUM(duration_minutes)       AS total_minutes
FROM downtime_events
WHERE event_type = 'Незапланированный'
GROUP BY event_category
ORDER BY events_count DESC;


-- ============================================================
-- ЧАСТЬ 5: Комплексные запросы (связь между таблицами)
-- ============================================================

-- 5.1. Связь между аварийными показаниями и последующими простоями
--      (ПДМ-01: перегрев → поломка)
SELECT
    'Показание датчика' AS event,
    sr.reading_timestamp AS event_time,
    sr.sensor_type || ': ' || sr.reading_value || ' ' || sr.unit AS details,
    sr.quality_flag AS flag
FROM sensor_readings sr
WHERE sr.equipment_id = 'EQ-001'
  AND sr.quality_flag IN ('WARN', 'ALARM')

UNION ALL

SELECT
    'Простой' AS event,
    d.start_time AS event_time,
    d.description AS details,
    d.severity AS flag
FROM downtime_events d
WHERE d.equipment_id = 'EQ-001'

ORDER BY event_time;

-- 5.2. Влияние простоев на добычу
--      (прерванная смена PRD-010 совпадает с поломкой ПДМ-01)
SELECT
    p.production_id,
    p.production_date,
    p.shift,
    e.equipment_name,
    p.tonnage_extracted,
    p.status AS prod_status,
    d.description AS downtime_reason,
    d.duration_minutes AS downtime_min
FROM ore_production p
JOIN equipment e ON p.equipment_id = e.equipment_id
LEFT JOIN downtime_events d
    ON p.equipment_id = d.equipment_id
   AND d.start_time::date = p.production_date
   AND d.event_type = 'Незапланированный'
WHERE p.equipment_id = 'EQ-001'
ORDER BY p.production_date, p.shift;
