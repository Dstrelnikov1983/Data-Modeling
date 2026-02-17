-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 3. Практическая работа: OLTP, OLAP, Data Vault
-- Скрипт 2: Создание Star Schema (Кимбалл)
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

-- Создаём отдельную схему для аналитической модели
CREATE SCHEMA IF NOT EXISTS star;
SET search_path TO star, ruda_plus, public;

-- ============================================================
-- Измерение 1: dim_time (Время)
-- Conformed dimension, используется во всех витринах
-- ============================================================

CREATE TABLE IF NOT EXISTS star.dim_time (
    time_key        SERIAL PRIMARY KEY,
    production_date DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week            INTEGER NOT NULL,
    day_of_month    INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE star.dim_time IS 'Измерение: Время (календарь) — Conformed Dimension';

-- Заполняем календарь за 2025 год
INSERT INTO star.dim_time (production_date, year, quarter, month, month_name,
                           week, day_of_month, day_of_week, day_name, is_weekend)
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(QUARTER FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       TO_CHAR(d, 'TMMonth'),
       EXTRACT(WEEK FROM d)::int,
       EXTRACT(DAY FROM d)::int,
       EXTRACT(ISODOW FROM d)::int,
       TO_CHAR(d, 'TMDay'),
       EXTRACT(ISODOW FROM d)::int IN (6, 7)
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, '1 day') AS d
ON CONFLICT (production_date) DO NOTHING;

-- ============================================================
-- Измерение 2: dim_mine (Шахта)
-- Conformed dimension
-- ============================================================

CREATE TABLE IF NOT EXISTS star.dim_mine (
    mine_key        SERIAL PRIMARY KEY,
    mine_id         VARCHAR(10) NOT NULL,   -- бизнес-ключ
    mine_name       VARCHAR(50) NOT NULL,
    region          VARCHAR(50),
    max_depth_m     INTEGER,
    status          VARCHAR(20),
    opened_date     DATE,
    -- SCD Type 2 поля
    effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE star.dim_mine IS 'Измерение: Шахта (SCD Type 2)';

-- Загрузка из OLTP
INSERT INTO star.dim_mine (mine_id, mine_name, region, max_depth_m, status, opened_date)
SELECT mine_id, mine_name, region, max_depth_m, status, opened_date
FROM ruda_plus.mines
ON CONFLICT DO NOTHING;

-- ============================================================
-- Измерение 3: dim_equipment (Оборудование)
-- Conformed dimension, денормализованное
-- ============================================================

CREATE TABLE IF NOT EXISTS star.dim_equipment (
    equipment_key       SERIAL PRIMARY KEY,
    equipment_id        VARCHAR(10) NOT NULL,   -- бизнес-ключ
    equipment_name      VARCHAR(50) NOT NULL,
    type_name           VARCHAR(50),            -- из equipment_types (денормализовано)
    type_code           VARCHAR(10),            -- из equipment_types
    manufacturer        VARCHAR(50),
    model               VARCHAR(50),
    year_manufactured   INTEGER,
    max_payload_tons    NUMERIC(6,1),
    mine_name           VARCHAR(50),            -- из mines (денормализовано)
    mine_region         VARCHAR(50),            -- из mines (денормализовано)
    -- SCD Type 2
    effective_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to        DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE star.dim_equipment IS 'Измерение: Оборудование (денормализовано, SCD Type 2)';

-- Загрузка с денормализацией (JOIN трёх OLTP-таблиц)
INSERT INTO star.dim_equipment (
    equipment_id, equipment_name, type_name, type_code,
    manufacturer, model, year_manufactured, max_payload_tons,
    mine_name, mine_region
)
SELECT e.equipment_id, e.equipment_name,
       et.type_name, et.type_code,
       e.manufacturer, e.model, e.year_manufactured, e.max_payload_tons,
       m.mine_name, m.region
FROM ruda_plus.equipment e
JOIN ruda_plus.equipment_types et ON e.type_id = et.type_id
JOIN ruda_plus.mines m ON e.mine_id = m.mine_id
ON CONFLICT DO NOTHING;

-- ============================================================
-- Измерение 4: dim_operator (Оператор)
-- SCD Type 2 — отслеживаем изменения квалификации
-- ============================================================

CREATE TABLE IF NOT EXISTS star.dim_operator (
    operator_key    SERIAL PRIMARY KEY,
    operator_id     VARCHAR(10) NOT NULL,   -- бизнес-ключ
    full_name       VARCHAR(100) NOT NULL,
    last_name       VARCHAR(50),
    first_name      VARCHAR(50),
    position        VARCHAR(50),
    qualification   VARCHAR(30),
    mine_name       VARCHAR(50),            -- денормализовано из mines
    -- SCD Type 2
    effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE star.dim_operator IS 'Измерение: Оператор (SCD Type 2)';

-- Загрузка
INSERT INTO star.dim_operator (
    operator_id, full_name, last_name, first_name,
    position, qualification, mine_name
)
SELECT o.operator_id,
       o.last_name || ' ' || o.first_name || COALESCE(' ' || o.middle_name, ''),
       o.last_name, o.first_name,
       o.position, o.qualification,
       m.mine_name
FROM ruda_plus.operators o
LEFT JOIN ruda_plus.mines m ON o.mine_id = m.mine_id
ON CONFLICT DO NOTHING;

-- ============================================================
-- Измерение 5: dim_downtime_category (Категория простоя)
-- Junk dimension — собирает малые атрибуты
-- ============================================================

CREATE TABLE IF NOT EXISTS star.dim_downtime_category (
    category_key    SERIAL PRIMARY KEY,
    event_type      VARCHAR(30) NOT NULL,
    event_category  VARCHAR(40) NOT NULL,
    severity        VARCHAR(20) NOT NULL,
    UNIQUE (event_type, event_category, severity)
);

COMMENT ON TABLE star.dim_downtime_category IS 'Измерение: Категория простоя (junk dimension)';

-- Загрузка уникальных комбинаций
INSERT INTO star.dim_downtime_category (event_type, event_category, severity)
SELECT DISTINCT event_type, event_category, severity
FROM ruda_plus.downtime_events
ON CONFLICT DO NOTHING;

-- ============================================================
-- Таблица фактов 1: fact_production (Добыча руды)
-- Зерно: одна смена, один оператор, одно оборудование
-- ============================================================

CREATE TABLE IF NOT EXISTS star.fact_production (
    production_key      SERIAL PRIMARY KEY,
    -- FK на измерения (суррогатные ключи)
    time_key            INTEGER NOT NULL REFERENCES star.dim_time(time_key),
    mine_key            INTEGER NOT NULL REFERENCES star.dim_mine(mine_key),
    equipment_key       INTEGER NOT NULL REFERENCES star.dim_equipment(equipment_key),
    operator_key        INTEGER REFERENCES star.dim_operator(operator_key),
    -- Degenerate dimensions
    production_id       VARCHAR(10) NOT NULL,  -- бизнес-ключ операции
    shift               INTEGER NOT NULL,
    block_id            VARCHAR(15),
    ore_type            VARCHAR(20),
    -- Метрики (факты)
    tonnage_extracted   NUMERIC(10,1) NOT NULL DEFAULT 0,
    fe_content_pct      NUMERIC(5,2) NOT NULL DEFAULT 0,
    moisture_pct        NUMERIC(5,2) NOT NULL DEFAULT 0,
    -- Аудит
    etl_loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE star.fact_production IS 'Факт: Добыча руды по сменам (зерно: смена + оборудование + оператор)';

-- Создаём индексы для аналитических запросов
CREATE INDEX IF NOT EXISTS idx_fp_time ON star.fact_production(time_key);
CREATE INDEX IF NOT EXISTS idx_fp_mine ON star.fact_production(mine_key);
CREATE INDEX IF NOT EXISTS idx_fp_equipment ON star.fact_production(equipment_key);
CREATE INDEX IF NOT EXISTS idx_fp_operator ON star.fact_production(operator_key);

-- Загрузка фактов (ETL из OLTP)
INSERT INTO star.fact_production (
    time_key, mine_key, equipment_key, operator_key,
    production_id, shift, block_id, ore_type,
    tonnage_extracted, fe_content_pct, moisture_pct
)
SELECT dt.time_key,
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
FROM ruda_plus.ore_production p
JOIN star.dim_time dt ON p.production_date = dt.production_date
JOIN star.dim_mine dm ON p.mine_id = dm.mine_id AND dm.is_current = TRUE
JOIN star.dim_equipment de ON p.equipment_id = de.equipment_id AND de.is_current = TRUE
LEFT JOIN star.dim_operator dop ON p.operator_id = dop.operator_id AND dop.is_current = TRUE
WHERE p.status = 'Завершена';

-- ============================================================
-- Таблица фактов 2: fact_downtime (Простои)
-- Зерно: одно событие простоя
-- ============================================================

CREATE TABLE IF NOT EXISTS star.fact_downtime (
    downtime_key        SERIAL PRIMARY KEY,
    -- FK на измерения
    time_key            INTEGER NOT NULL REFERENCES star.dim_time(time_key),
    equipment_key       INTEGER NOT NULL REFERENCES star.dim_equipment(equipment_key),
    category_key        INTEGER NOT NULL REFERENCES star.dim_downtime_category(category_key),
    -- Degenerate dimension
    event_id            VARCHAR(10) NOT NULL,
    -- Метрики
    duration_minutes    INTEGER NOT NULL DEFAULT 0,
    -- Аудит
    etl_loaded_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE star.fact_downtime IS 'Факт: Простои оборудования (зерно: одно событие)';

CREATE INDEX IF NOT EXISTS idx_fd_time ON star.fact_downtime(time_key);
CREATE INDEX IF NOT EXISTS idx_fd_equipment ON star.fact_downtime(equipment_key);
CREATE INDEX IF NOT EXISTS idx_fd_category ON star.fact_downtime(category_key);

-- Загрузка
INSERT INTO star.fact_downtime (
    time_key, equipment_key, category_key,
    event_id, duration_minutes
)
SELECT dt.time_key,
       de.equipment_key,
       dc.category_key,
       d.event_id,
       COALESCE(d.duration_minutes, 0)
FROM ruda_plus.downtime_events d
JOIN star.dim_time dt ON d.start_time::date = dt.production_date
JOIN star.dim_equipment de ON d.equipment_id = de.equipment_id AND de.is_current = TRUE
JOIN star.dim_downtime_category dc
    ON d.event_type = dc.event_type
    AND d.event_category = dc.event_category
    AND d.severity = dc.severity;

-- ============================================================
-- Проверка загрузки
-- ============================================================

SELECT '--- Star Schema: проверка загрузки ---' AS info;
SELECT 'dim_time' AS table_name, COUNT(*) AS rows FROM star.dim_time
UNION ALL SELECT 'dim_mine', COUNT(*) FROM star.dim_mine
UNION ALL SELECT 'dim_equipment', COUNT(*) FROM star.dim_equipment
UNION ALL SELECT 'dim_operator', COUNT(*) FROM star.dim_operator
UNION ALL SELECT 'dim_downtime_category', COUNT(*) FROM star.dim_downtime_category
UNION ALL SELECT 'fact_production', COUNT(*) FROM star.fact_production
UNION ALL SELECT 'fact_downtime', COUNT(*) FROM star.fact_downtime
ORDER BY table_name;
