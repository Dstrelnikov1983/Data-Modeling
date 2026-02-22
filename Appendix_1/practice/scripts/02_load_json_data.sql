-- ============================================================
-- Приложение 1. Упражнение 3: Загрузка данных
-- Предприятие "Руда+" — MES-система
-- ============================================================

-- ============================================================
-- 1. Загрузка оборудования в JSONB-таблицу
-- ============================================================

INSERT INTO equipment_json (data) VALUES
('{
  "equipment_id": "PDM-001",
  "type": "ПДМ",
  "model": "Sandvik LH517i",
  "mine": {"code": "SH-01", "name": "Шахта Северная"},
  "specifications": {"payload_tonnes": 17.0, "engine_power_kw": 250, "year_manufactured": 2021},
  "is_active": true,
  "sensors": [
    {"sensor_id": "S-101", "type": "temperature", "location": "Двигатель", "unit": "°C"},
    {"sensor_id": "S-102", "type": "vibration", "location": "Ковш", "unit": "mm/s"},
    {"sensor_id": "S-103", "type": "pressure", "location": "Гидросистема", "unit": "bar"},
    {"sensor_id": "S-104", "type": "gps", "location": "Кабина", "unit": "coord"}
  ]
}'::jsonb),

('{
  "equipment_id": "PDM-002",
  "type": "ПДМ",
  "model": "Caterpillar R1700",
  "mine": {"code": "SH-01", "name": "Шахта Северная"},
  "specifications": {"payload_tonnes": 15.0, "engine_power_kw": 220, "year_manufactured": 2020},
  "is_active": true,
  "sensors": [
    {"sensor_id": "S-201", "type": "temperature", "location": "Двигатель", "unit": "°C"},
    {"sensor_id": "S-202", "type": "vibration", "location": "Ковш", "unit": "mm/s"}
  ]
}'::jsonb),

('{
  "equipment_id": "ST-001",
  "type": "Шахтный самосвал",
  "model": "Caterpillar AD45",
  "mine": {"code": "SH-02", "name": "Шахта Восточная"},
  "specifications": {"payload_tonnes": 45.0, "engine_power_kw": 415, "year_manufactured": 2022},
  "is_active": true,
  "sensors": [
    {"sensor_id": "S-301", "type": "temperature", "location": "Двигатель", "unit": "°C"},
    {"sensor_id": "S-302", "type": "gps", "location": "Кабина", "unit": "coord"},
    {"sensor_id": "S-303", "type": "video", "location": "Кабина", "unit": "stream"}
  ]
}'::jsonb),

('{
  "equipment_id": "VG-001",
  "type": "Вагонетка",
  "model": "ВГ-3.3",
  "mine": {"code": "SH-01", "name": "Шахта Северная"},
  "specifications": {"payload_tonnes": 3.3, "engine_power_kw": 0, "year_manufactured": 2019},
  "is_active": true,
  "sensors": []
}'::jsonb),

('{
  "equipment_id": "SK-001",
  "type": "Скиповый подъёмник",
  "model": "СКП-20",
  "mine": {"code": "SH-01", "name": "Шахта Северная"},
  "specifications": {"payload_tonnes": 20.0, "engine_power_kw": 500, "year_manufactured": 2018},
  "is_active": false,
  "sensors": [
    {"sensor_id": "S-501", "type": "vibration", "location": "Канат", "unit": "mm/s"},
    {"sensor_id": "S-502", "type": "pressure", "location": "Тормозная система", "unit": "bar"}
  ]
}'::jsonb);

-- ============================================================
-- 2. Загрузка телеметрии в JSONB-таблицу
-- ============================================================

INSERT INTO sensor_readings_json (data) VALUES
('{"reading_id":"R-000001","sensor_id":"S-101","equipment_id":"PDM-001","timestamp":"2025-01-15T10:30:00Z","type":"temperature","value":85.3,"unit":"°C","status":"normal","location":{"x":1250.5,"y":340.2,"z":-180.0,"horizon":"Горизонт -180"}}'::jsonb),
('{"reading_id":"R-000002","sensor_id":"S-102","equipment_id":"PDM-001","timestamp":"2025-01-15T10:30:01Z","type":"vibration","value":4.7,"unit":"mm/s","status":"normal","location":{"x":1250.5,"y":340.2,"z":-180.0,"horizon":"Горизонт -180"}}'::jsonb),
('{"reading_id":"R-000003","sensor_id":"S-103","equipment_id":"PDM-001","timestamp":"2025-01-15T10:30:02Z","type":"pressure","value":210.5,"unit":"bar","status":"warning","location":{"x":1251.0,"y":340.5,"z":-180.0,"horizon":"Горизонт -180"}}'::jsonb),
('{"reading_id":"R-000004","sensor_id":"S-201","equipment_id":"PDM-002","timestamp":"2025-01-15T10:30:00Z","type":"temperature","value":72.1,"unit":"°C","status":"normal","location":{"x":800.0,"y":150.3,"z":-180.0,"horizon":"Горизонт -180"}}'::jsonb),
('{"reading_id":"R-000005","sensor_id":"S-301","equipment_id":"ST-001","timestamp":"2025-01-15T10:30:05Z","type":"temperature","value":95.8,"unit":"°C","status":"warning","location":{"x":2100.0,"y":520.0,"z":-240.0,"horizon":"Горизонт -240"}}'::jsonb),
('{"reading_id":"R-000006","sensor_id":"S-302","equipment_id":"ST-001","timestamp":"2025-01-15T10:30:05Z","type":"gps","value":null,"unit":"coord","status":"normal","location":{"x":2100.0,"y":520.0,"z":-240.0,"horizon":"Горизонт -240"},"gps_data":{"latitude":55.7558,"longitude":37.6173,"speed_kmh":12.5}}'::jsonb),
('{"reading_id":"R-000007","sensor_id":"S-101","equipment_id":"PDM-001","timestamp":"2025-01-15T10:31:00Z","type":"temperature","value":112.4,"unit":"°C","status":"critical","location":{"x":1255.0,"y":342.0,"z":-180.0,"horizon":"Горизонт -180"},"alert":{"level":"critical","message":"Превышена максимальная температура двигателя","threshold":100.0}}'::jsonb),
('{"reading_id":"R-000008","sensor_id":"S-501","equipment_id":"SK-001","timestamp":"2025-01-15T10:30:10Z","type":"vibration","value":8.2,"unit":"mm/s","status":"warning","location":{"x":0,"y":0,"z":-300.0,"horizon":"Ствол шахты"}}'::jsonb),
('{"reading_id":"R-000009","sensor_id":"S-502","equipment_id":"SK-001","timestamp":"2025-01-15T10:30:10Z","type":"pressure","value":145.0,"unit":"bar","status":"normal","location":{"x":0,"y":0,"z":-300.0,"horizon":"Ствол шахты"}}'::jsonb),
('{"reading_id":"R-000010","sensor_id":"S-202","equipment_id":"PDM-002","timestamp":"2025-01-15T10:31:00Z","type":"vibration","value":15.6,"unit":"mm/s","status":"critical","location":{"x":805.0,"y":152.0,"z":-180.0,"horizon":"Горизонт -180"},"alert":{"level":"critical","message":"Критическая вибрация ковша","threshold":12.0}}'::jsonb);

-- ============================================================
-- 3. Загрузка в гибридную таблицу
-- ============================================================

INSERT INTO equipment_hybrid (equipment_id, type, model, mine_code, is_active, specifications, sensors) VALUES
('PDM-001', 'ПДМ', 'Sandvik LH517i', 'SH-01', true,
 '{"payload_tonnes": 17.0, "engine_power_kw": 250, "year_manufactured": 2021}'::jsonb,
 '[{"sensor_id":"S-101","type":"temperature"},{"sensor_id":"S-102","type":"vibration"},{"sensor_id":"S-103","type":"pressure"},{"sensor_id":"S-104","type":"gps"}]'::jsonb),

('PDM-002', 'ПДМ', 'Caterpillar R1700', 'SH-01', true,
 '{"payload_tonnes": 15.0, "engine_power_kw": 220, "year_manufactured": 2020}'::jsonb,
 '[{"sensor_id":"S-201","type":"temperature"},{"sensor_id":"S-202","type":"vibration"}]'::jsonb),

('ST-001', 'Шахтный самосвал', 'Caterpillar AD45', 'SH-02', true,
 '{"payload_tonnes": 45.0, "engine_power_kw": 415, "year_manufactured": 2022}'::jsonb,
 '[{"sensor_id":"S-301","type":"temperature"},{"sensor_id":"S-302","type":"gps"},{"sensor_id":"S-303","type":"video"}]'::jsonb),

('VG-001', 'Вагонетка', 'ВГ-3.3', 'SH-01', true,
 '{"payload_tonnes": 3.3, "engine_power_kw": 0, "year_manufactured": 2019}'::jsonb,
 '[]'::jsonb),

('SK-001', 'Скиповый подъёмник', 'СКП-20', 'SH-01', false,
 '{"payload_tonnes": 20.0, "engine_power_kw": 500, "year_manufactured": 2018}'::jsonb,
 '[{"sensor_id":"S-501","type":"vibration"},{"sensor_id":"S-502","type":"pressure"}]'::jsonb);

-- ============================================================
-- 4. Загрузка XML-данных
-- ============================================================

INSERT INTO equipment_xml (data) VALUES
(XMLPARSE(DOCUMENT '<?xml version="1.0" encoding="UTF-8"?>
<equipment xmlns="http://ruda-plus.ru/mes/equipment">
    <id>PDM-001</id>
    <type>ПДМ</type>
    <model>Sandvik LH517i</model>
    <mine code="SH-01">Шахта Северная</mine>
    <specifications>
        <payload unit="tonnes">17.0</payload>
        <engine_power unit="kW">250</engine_power>
        <year_manufactured>2021</year_manufactured>
    </specifications>
</equipment>')),

(XMLPARSE(DOCUMENT '<?xml version="1.0" encoding="UTF-8"?>
<equipment xmlns="http://ruda-plus.ru/mes/equipment">
    <id>ST-001</id>
    <type>Шахтный самосвал</type>
    <model>Caterpillar AD45</model>
    <mine code="SH-02">Шахта Восточная</mine>
    <specifications>
        <payload unit="tonnes">45.0</payload>
        <engine_power unit="kW">415</engine_power>
        <year_manufactured>2022</year_manufactured>
    </specifications>
</equipment>')),

(XMLPARSE(DOCUMENT '<?xml version="1.0" encoding="UTF-8"?>
<equipment xmlns="http://ruda-plus.ru/mes/equipment">
    <id>SK-001</id>
    <type>Скиповый подъёмник</type>
    <model>СКП-20</model>
    <mine code="SH-01">Шахта Северная</mine>
    <specifications>
        <payload unit="tonnes">20.0</payload>
        <engine_power unit="kW">500</engine_power>
        <year_manufactured>2018</year_manufactured>
    </specifications>
</equipment>'));

-- ============================================================
-- Проверка загруженных данных
-- ============================================================
SELECT 'equipment_json' AS table_name, COUNT(*) AS rows FROM equipment_json
UNION ALL
SELECT 'sensor_readings_json', COUNT(*) FROM sensor_readings_json
UNION ALL
SELECT 'equipment_hybrid', COUNT(*) FROM equipment_hybrid
UNION ALL
SELECT 'equipment_xml', COUNT(*) FROM equipment_xml;
