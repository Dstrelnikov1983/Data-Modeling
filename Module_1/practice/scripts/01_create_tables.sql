-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 1. Практическая работа: Обзор структур хранения
-- Скрипт 1: Создание таблиц в PostgreSQL
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

-- Создаём схему для проекта
CREATE SCHEMA IF NOT EXISTS ruda_plus;

-- Устанавливаем схему по умолчанию
SET search_path TO ruda_plus, public;

-- ============================================================
-- Таблица 1: Справочник оборудования
-- ============================================================
CREATE TABLE IF NOT EXISTS equipment (
    equipment_id    VARCHAR(10)   PRIMARY KEY,
    equipment_name  VARCHAR(50)   NOT NULL,
    equipment_type  VARCHAR(50)   NOT NULL,
    manufacturer    VARCHAR(50)   NOT NULL,
    model           VARCHAR(50)   NOT NULL,
    year_manufactured INTEGER     NOT NULL,
    mine_id         VARCHAR(10)   NOT NULL,
    mine_name       VARCHAR(50)   NOT NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'В работе',
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    engine_hours    NUMERIC(10,1) DEFAULT 0,
    max_payload_tons NUMERIC(6,1),

    CONSTRAINT chk_status CHECK (status IN ('В работе', 'На ТО', 'Простой', 'Списано')),
    CONSTRAINT chk_year CHECK (year_manufactured BETWEEN 2000 AND 2030),
    CONSTRAINT chk_engine_hours CHECK (engine_hours >= 0)
);

COMMENT ON TABLE equipment IS 'Справочник горнодобывающего оборудования предприятия Руда+';
COMMENT ON COLUMN equipment.equipment_type IS 'Тип: ПДМ, самосвал, вагонетка, скиповый подъемник';
COMMENT ON COLUMN equipment.engine_hours IS 'Наработка двигателя в моточасах (0 для вагонеток)';

-- ============================================================
-- Таблица 2: Показания датчиков (телеметрия)
-- ============================================================
CREATE TABLE IF NOT EXISTS sensor_readings (
    reading_id       VARCHAR(12)   PRIMARY KEY,
    equipment_id     VARCHAR(10)   NOT NULL REFERENCES equipment(equipment_id),
    sensor_type      VARCHAR(30)   NOT NULL,
    reading_value    NUMERIC(10,2) NOT NULL,
    unit             VARCHAR(10)   NOT NULL,
    reading_timestamp TIMESTAMP    NOT NULL,
    quality_flag     VARCHAR(10)   NOT NULL DEFAULT 'OK',

    CONSTRAINT chk_quality_flag CHECK (quality_flag IN ('OK', 'WARN', 'ALARM', 'ERROR'))
);

COMMENT ON TABLE sensor_readings IS 'Показания датчиков оборудования (телеметрия)';
COMMENT ON COLUMN sensor_readings.quality_flag IS 'Флаг качества: OK, WARN, ALARM, ERROR';

-- Индекс для быстрого поиска по оборудованию и времени
CREATE INDEX IF NOT EXISTS idx_sensor_equipment_time
    ON sensor_readings(equipment_id, reading_timestamp);

-- Индекс для поиска аварийных показаний
CREATE INDEX IF NOT EXISTS idx_sensor_quality
    ON sensor_readings(quality_flag)
    WHERE quality_flag IN ('WARN', 'ALARM');

-- ============================================================
-- Таблица 3: Добыча руды
-- ============================================================
CREATE TABLE IF NOT EXISTS ore_production (
    production_id    VARCHAR(10)   PRIMARY KEY,
    mine_id          VARCHAR(10)   NOT NULL,
    mine_name        VARCHAR(50)   NOT NULL,
    production_date  DATE          NOT NULL,
    shift            INTEGER       NOT NULL,
    horizon_level    VARCHAR(30)   NOT NULL,
    block_id         VARCHAR(15)   NOT NULL,
    ore_type         VARCHAR(20)   NOT NULL,
    tonnage_extracted NUMERIC(10,1) NOT NULL DEFAULT 0,
    fe_content_pct   NUMERIC(5,2)  NOT NULL DEFAULT 0,
    moisture_pct     NUMERIC(5,2)  NOT NULL DEFAULT 0,
    equipment_id     VARCHAR(10)   NOT NULL REFERENCES equipment(equipment_id),
    operator_name    VARCHAR(50)   NOT NULL,
    start_time       TIME          NOT NULL,
    end_time         TIME          NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'Завершена',

    CONSTRAINT chk_shift CHECK (shift BETWEEN 1 AND 3),
    CONSTRAINT chk_fe CHECK (fe_content_pct BETWEEN 0 AND 100),
    CONSTRAINT chk_moisture CHECK (moisture_pct BETWEEN 0 AND 100),
    CONSTRAINT chk_prod_status CHECK (status IN ('Завершена', 'Прервана', 'В процессе'))
);

COMMENT ON TABLE ore_production IS 'Журнал добычи руды по сменам';
COMMENT ON COLUMN ore_production.fe_content_pct IS 'Содержание железа (Fe) в процентах';
COMMENT ON COLUMN ore_production.horizon_level IS 'Горизонт (уровень) добычи в шахте';

-- Индекс для аналитики по дате
CREATE INDEX IF NOT EXISTS idx_production_date
    ON ore_production(production_date, mine_id);

-- ============================================================
-- Таблица 4: Простои оборудования
-- ============================================================
CREATE TABLE IF NOT EXISTS downtime_events (
    event_id          VARCHAR(10)   PRIMARY KEY,
    equipment_id      VARCHAR(10)   NOT NULL REFERENCES equipment(equipment_id),
    event_type        VARCHAR(30)   NOT NULL,
    event_category    VARCHAR(40)   NOT NULL,
    start_time        TIMESTAMP     NOT NULL,
    end_time          TIMESTAMP,
    duration_minutes  INTEGER,
    description       TEXT,
    severity          VARCHAR(20)   NOT NULL,
    reported_by       VARCHAR(50)   NOT NULL,

    CONSTRAINT chk_event_type CHECK (event_type IN ('Незапланированный', 'Плановое ТО')),
    CONSTRAINT chk_severity CHECK (severity IN ('Низкая', 'Средняя', 'Высокая', 'Критическая', 'Плановое')),
    CONSTRAINT chk_duration CHECK (duration_minutes >= 0)
);

COMMENT ON TABLE downtime_events IS 'Журнал простоев и событий обслуживания оборудования';

-- Индекс для анализа незапланированных простоев
CREATE INDEX IF NOT EXISTS idx_downtime_type
    ON downtime_events(event_type, start_time);
