-- ============================================================
-- Приложение 1. Упражнение 3: Запросы к JSON и XML данным
-- Предприятие "Руда+" — MES-система
-- ============================================================

-- ============================================================
-- ЧАСТЬ 1: Базовые запросы к JSONB (equipment_json)
-- ============================================================

-- 1.1 Извлечение основных полей оборудования
SELECT
    data->>'equipment_id' AS equipment_id,
    data->>'type' AS eq_type,
    data->>'model' AS model,
    data->'mine'->>'code' AS mine_code,
    data->'mine'->>'name' AS mine_name,
    (data->>'is_active')::boolean AS is_active
FROM equipment_json;

-- 1.2 Извлечение спецификаций
SELECT
    data->>'equipment_id' AS equipment_id,
    (data->'specifications'->>'payload_tonnes')::decimal AS payload,
    (data->'specifications'->>'engine_power_kw')::integer AS power_kw,
    (data->'specifications'->>'year_manufactured')::integer AS year
FROM equipment_json;

-- 1.3 Фильтрация: только ПДМ
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE data->>'type' = 'ПДМ';

-- 1.4 Фильтрация через оператор containment (@>)
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE data @> '{"type": "ПДМ"}'::jsonb;

-- 1.5 Оборудование с грузоподъёмностью > 15 тонн
SELECT
    data->>'equipment_id' AS id,
    data->>'model' AS model,
    (data->'specifications'->>'payload_tonnes')::decimal AS payload
FROM equipment_json
WHERE (data->'specifications'->>'payload_tonnes')::decimal > 15
ORDER BY payload DESC;

-- ============================================================
-- ЧАСТЬ 2: Работа с массивами в JSONB
-- ============================================================

-- 2.1 Извлечение датчиков с помощью jsonb_array_elements
SELECT
    data->>'equipment_id' AS equipment_id,
    sensor->>'sensor_id' AS sensor_id,
    sensor->>'type' AS sensor_type,
    sensor->>'location' AS location
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor;

-- 2.2 Количество датчиков на каждом оборудовании
SELECT
    data->>'equipment_id' AS equipment_id,
    data->>'model' AS model,
    jsonb_array_length(data->'sensors') AS sensor_count
FROM equipment_json
ORDER BY sensor_count DESC;

-- 2.3 Оборудование, имеющее датчик температуры
SELECT DISTINCT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor
WHERE sensor->>'type' = 'temperature';

-- 2.4 Проверка наличия ключа (оператор ?)
SELECT data->>'equipment_id' AS id
FROM equipment_json
WHERE data ? 'sensors';

-- ============================================================
-- ЧАСТЬ 3: JSONPath-запросы (PostgreSQL 12+)
-- ============================================================

-- 3.1 JSONPath: все идентификаторы датчиков
SELECT jsonb_path_query(data, '$.sensors[*].sensor_id') AS sensor_ids
FROM equipment_json;

-- 3.2 JSONPath: датчики температуры
SELECT
    data->>'equipment_id' AS equipment_id,
    jsonb_path_query(data, '$.sensors[*] ? (@.type == "temperature")') AS temp_sensor
FROM equipment_json;

-- 3.3 JSONPath: оборудование с мощностью > 200 кВт
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE data @@ '$.specifications.engine_power_kw > 200';

-- 3.4 JSONPath: проверка значения (jsonb_path_exists)
SELECT data->>'equipment_id' AS id
FROM equipment_json
WHERE jsonb_path_exists(data, '$.sensors[*] ? (@.type == "gps")');

-- ============================================================
-- ЧАСТЬ 4: Агрегация по JSONB
-- ============================================================

-- 4.1 Средняя грузоподъёмность по типу оборудования
SELECT
    data->>'type' AS eq_type,
    COUNT(*) AS count,
    ROUND(AVG((data->'specifications'->>'payload_tonnes')::decimal), 1) AS avg_payload,
    MAX((data->'specifications'->>'engine_power_kw')::integer) AS max_power
FROM equipment_json
GROUP BY data->>'type'
ORDER BY avg_payload DESC;

-- 4.2 Количество оборудования по шахтам
SELECT
    data->'mine'->>'code' AS mine_code,
    data->'mine'->>'name' AS mine_name,
    COUNT(*) AS equipment_count
FROM equipment_json
GROUP BY data->'mine'->>'code', data->'mine'->>'name';

-- 4.3 Общее количество датчиков по типам
SELECT
    sensor->>'type' AS sensor_type,
    COUNT(*) AS total_count
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor
GROUP BY sensor->>'type'
ORDER BY total_count DESC;

-- ============================================================
-- ЧАСТЬ 5: Обновление JSONB
-- ============================================================

-- 5.1 Обновление значения поля через jsonb_set
UPDATE equipment_json
SET data = jsonb_set(data, '{is_active}', 'false')
WHERE data->>'equipment_id' = 'VG-001';

-- Проверка
SELECT data->>'equipment_id' AS id, data->>'is_active' AS active
FROM equipment_json WHERE data->>'equipment_id' = 'VG-001';

-- 5.2 Добавление нового поля
UPDATE equipment_json
SET data = data || '{"last_inspection": "2025-02-20"}'::jsonb
WHERE data->>'equipment_id' = 'PDM-001';

-- Проверка
SELECT data->>'equipment_id' AS id, data->>'last_inspection' AS inspection
FROM equipment_json WHERE data->>'equipment_id' = 'PDM-001';

-- 5.3 Удаление поля
UPDATE equipment_json
SET data = data - 'last_inspection'
WHERE data->>'equipment_id' = 'PDM-001';

-- 5.4 Добавление элемента в массив sensors
UPDATE equipment_json
SET data = jsonb_set(
    data,
    '{sensors}',
    (data->'sensors') || '{"sensor_id": "S-105", "type": "video", "location": "Кабина", "unit": "stream"}'::jsonb
)
WHERE data->>'equipment_id' = 'PDM-001';

-- Проверка
SELECT jsonb_array_length(data->'sensors') AS sensor_count
FROM equipment_json WHERE data->>'equipment_id' = 'PDM-001';

-- ============================================================
-- ЧАСТЬ 6: Запросы к телеметрии
-- ============================================================

-- 6.1 Критические показания
SELECT
    data->>'reading_id' AS reading_id,
    data->>'equipment_id' AS equipment_id,
    data->>'type' AS sensor_type,
    (data->>'value')::decimal AS value,
    data->>'unit' AS unit,
    data->'alert'->>'message' AS alert_message
FROM sensor_readings_json
WHERE data->>'status' = 'critical';

-- 6.2 Показания по горизонтам
SELECT
    data->'location'->>'horizon' AS horizon,
    COUNT(*) AS reading_count,
    COUNT(*) FILTER (WHERE data->>'status' = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE data->>'status' = 'warning') AS warning_count
FROM sensor_readings_json
GROUP BY data->'location'->>'horizon';

-- 6.3 Средние значения по типу датчика
SELECT
    data->>'type' AS sensor_type,
    ROUND(AVG((data->>'value')::decimal), 2) AS avg_value,
    data->>'unit' AS unit
FROM sensor_readings_json
WHERE data->>'value' IS NOT NULL
  AND data->>'value' != 'null'
GROUP BY data->>'type', data->>'unit';

-- ============================================================
-- ЧАСТЬ 7: Запросы к XML (equipment_xml)
-- ============================================================

-- 7.1 Извлечение полей с помощью xpath()
-- Примечание: для работы с namespace используем массив namespace
SELECT
    (xpath('//n:id/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS equipment_id,
    (xpath('//n:type/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS eq_type,
    (xpath('//n:model/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS model,
    (xpath('//n:mine/@code', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS mine_code
FROM equipment_xml;

-- 7.2 Извлечение спецификаций из XML
SELECT
    (xpath('//n:id/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS equipment_id,
    (xpath('//n:payload/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text::decimal AS payload,
    (xpath('//n:engine_power/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text::integer AS power_kw
FROM equipment_xml;

-- ============================================================
-- ЧАСТЬ 8: Сравнение планов запросов (EXPLAIN)
-- ============================================================

-- 8.1 Запрос БЕЗ индекса (Seq Scan)
EXPLAIN ANALYZE
SELECT data->>'equipment_id'
FROM equipment_json
WHERE data->>'type' = 'ПДМ';

-- 8.2 Запрос С GIN-индексом (Bitmap Index Scan)
EXPLAIN ANALYZE
SELECT data->>'equipment_id'
FROM equipment_json
WHERE data @> '{"type": "ПДМ"}'::jsonb;

-- 8.3 Гибридная таблица: реляционный индекс
EXPLAIN ANALYZE
SELECT equipment_id, model
FROM equipment_hybrid
WHERE type = 'ПДМ';
