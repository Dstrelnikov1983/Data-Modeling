-- ============================================================
-- Приложение 1. Упражнение 4: XPath и JSONPath — ответы
-- Предприятие "Руда+" — MES-система
-- ============================================================

-- ============================================================
-- ЗАДАНИЕ: Напишите запросы XPath и JSONPath для каждой задачи
-- Ниже приведены ответы для самопроверки
-- ============================================================

-- ┌────┬─────────────────────────────────┬──────────────────────────────────────┬────────────────────────────────────────┐
-- │ #  │ Задача                          │ XPath                                │ JSONPath                               │
-- ├────┼─────────────────────────────────┼──────────────────────────────────────┼────────────────────────────────────────┤
-- │ 1  │ Все идентификаторы оборудования │ //equipment/id/text()                │ $[*].equipment_id                      │
-- │ 2  │ Датчики вибрации                │ //sensor[@type='vibration']           │ $[*].sensors[?@.type=="vibration"]     │
-- │ 3  │ Оборудование шахты SH-01       │ //equipment[mine/@code='SH-01']      │ $[?@.mine.code=="SH-01"]               │
-- │ 4  │ Грузоподъёмность > 15 тонн      │ //equipment[specifications/          │ $[?@.specifications.                   │
-- │    │                                 │   payload > 15]                      │   payload_tonnes > 15]                 │
-- │ 5  │ Модель первой машины            │ //equipment[1]/model/text()          │ $[0].model                             │
-- └────┴─────────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────┘

-- ============================================================
-- РЕАЛИЗАЦИЯ В PostgreSQL
-- ============================================================

-- Задача 1: Все идентификаторы оборудования
-- JSONPath в PostgreSQL:
SELECT jsonb_path_query(data, '$.equipment_id') AS equipment_id
FROM equipment_json;

-- XPath в PostgreSQL:
SELECT (xpath('//n:id/text()', data,
    ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS equipment_id
FROM equipment_xml;

-- Задача 2: Датчики вибрации
-- JSONPath:
SELECT
    data->>'equipment_id' AS equipment_id,
    jsonb_path_query(data, '$.sensors[*] ? (@.type == "vibration")') AS vibration_sensor
FROM equipment_json;

-- Задача 3: Оборудование шахты SH-01
-- JSONPath:
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE jsonb_path_exists(data, '$ ? (@.mine.code == "SH-01")');

-- Альтернативный вариант:
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE data->'mine'->>'code' = 'SH-01';

-- XPath:
SELECT (xpath('//n:id/text()', data,
    ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS equipment_id
FROM equipment_xml
WHERE (xpath('//n:mine/@code', data,
    ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text = 'SH-01';

-- Задача 4: Грузоподъёмность > 15 тонн
-- JSONPath:
SELECT data->>'equipment_id' AS id,
       (data->'specifications'->>'payload_tonnes')::decimal AS payload
FROM equipment_json
WHERE data @@ '$.specifications.payload_tonnes > 15';

-- XPath:
SELECT
    (xpath('//n:id/text()', data,
        ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS equipment_id,
    (xpath('//n:payload/text()', data,
        ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text::decimal AS payload
FROM equipment_xml
WHERE (xpath('//n:payload/text()', data,
    ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text::decimal > 15;

-- Задача 5: Модель первой машины
-- JSONPath:
SELECT jsonb_path_query_first(
    (SELECT jsonb_agg(data) FROM equipment_json),
    '$[0].model'
) AS first_model;

-- Простой вариант:
SELECT data->>'model' AS model
FROM equipment_json
ORDER BY id
LIMIT 1;

-- ============================================================
-- ДОПОЛНИТЕЛЬНЫЕ ЗАДАНИЯ (для продвинутых)
-- ============================================================

-- Д1: Найти все уникальные типы датчиков на предприятии
SELECT DISTINCT sensor->>'type' AS sensor_type
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor
ORDER BY sensor_type;

-- Д2: Найти оборудование без датчиков
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE jsonb_array_length(data->'sensors') = 0;

-- Д3: Для каждого типа оборудования показать все модели
SELECT
    data->>'type' AS eq_type,
    string_agg(data->>'model', ', ') AS models
FROM equipment_json
GROUP BY data->>'type';

-- Д4: Найти оборудование, у которого есть и GPS, и видео датчики
SELECT data->>'equipment_id' AS id, data->>'model' AS model
FROM equipment_json
WHERE jsonb_path_exists(data, '$.sensors[*] ? (@.type == "gps")')
  AND jsonb_path_exists(data, '$.sensors[*] ? (@.type == "video")');

-- Д5: Построить сводку: оборудование × типы датчиков (перекрёстный запрос)
SELECT
    data->>'equipment_id' AS equipment_id,
    COUNT(*) FILTER (WHERE sensor->>'type' = 'temperature') AS temperature,
    COUNT(*) FILTER (WHERE sensor->>'type' = 'vibration') AS vibration,
    COUNT(*) FILTER (WHERE sensor->>'type' = 'pressure') AS pressure,
    COUNT(*) FILTER (WHERE sensor->>'type' = 'gps') AS gps,
    COUNT(*) FILTER (WHERE sensor->>'type' = 'video') AS video
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor
GROUP BY data->>'equipment_id'
ORDER BY equipment_id;
