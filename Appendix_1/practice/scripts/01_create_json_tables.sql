-- ============================================================
-- Приложение 1. Упражнение 3: Создание таблиц с JSON/XML
-- Предприятие "Руда+" — MES-система
-- ============================================================

-- Очистка (если таблицы уже существуют)
DROP TABLE IF EXISTS sensor_readings_json CASCADE;
DROP TABLE IF EXISTS equipment_json CASCADE;
DROP TABLE IF EXISTS equipment_hybrid CASCADE;
DROP TABLE IF EXISTS equipment_xml CASCADE;

-- ============================================================
-- 1. Таблица с JSONB-документами оборудования
-- ============================================================
CREATE TABLE equipment_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE equipment_json IS 'Паспорта оборудования в формате JSONB';

-- ============================================================
-- 2. Таблица с JSONB-телеметрией датчиков
-- ============================================================
CREATE TABLE sensor_readings_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE sensor_readings_json IS 'Показания IoT-датчиков в формате JSONB';

-- ============================================================
-- 3. Гибридная таблица: реляционные поля + JSONB
-- ============================================================
CREATE TABLE equipment_hybrid (
    equipment_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    mine_code VARCHAR(10) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    -- Гибкие поля в JSONB
    specifications JSONB NOT NULL,
    sensors JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE equipment_hybrid IS 'Гибридная модель: реляционные столбцы + JSONB';

-- CHECK-ограничение для валидации JSONB
ALTER TABLE equipment_hybrid
ADD CONSTRAINT chk_specifications CHECK (
    specifications ? 'payload_tonnes'
    AND (specifications->>'payload_tonnes')::decimal >= 0
    AND (specifications->>'payload_tonnes')::decimal <= 100
);

-- ============================================================
-- 4. Таблица с XML-документами
-- ============================================================
CREATE TABLE equipment_xml (
    id SERIAL PRIMARY KEY,
    data XML NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE equipment_xml IS 'Паспорта оборудования в формате XML';

-- ============================================================
-- 5. Индексы для JSONB
-- ============================================================

-- GIN-индекс для общего поиска по JSONB
CREATE INDEX idx_equipment_json_data ON equipment_json USING GIN (data);

-- GIN-индекс для поиска по телеметрии
CREATE INDEX idx_readings_json_data ON sensor_readings_json USING GIN (data);

-- GIN-индекс для гибридной таблицы
CREATE INDEX idx_hybrid_specs ON equipment_hybrid USING GIN (specifications);
CREATE INDEX idx_hybrid_sensors ON equipment_hybrid USING GIN (sensors);

-- B-tree индекс на конкретное JSONB-поле (для гибридной таблицы)
CREATE INDEX idx_hybrid_type ON equipment_hybrid (type);

-- ============================================================
-- Проверка: список созданных таблиц
-- ============================================================
SELECT table_name, obj_description(oid) AS description
FROM information_schema.tables
JOIN pg_class ON relname = table_name
WHERE table_schema = 'public'
  AND table_name IN (
    'equipment_json', 'sensor_readings_json',
    'equipment_hybrid', 'equipment_xml'
  )
ORDER BY table_name;
