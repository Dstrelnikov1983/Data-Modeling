-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 4. Моделирование потоковых и пакетных данных
-- Скрипт 1: Создание Staging-схемы и таблиц управления ETL
-- Предприятие: "Руда+" — добыча железной руды
--
-- ВАЖНО: Убедитесь, что скрипты модулей 1–3 уже выполнены.
--         Схемы ruda_plus и star должны существовать.
-- ============================================================

-- ============================================================
-- Раздел 1: Создание схемы staging
-- Промежуточная зона для ETL-процесса
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- Устанавливаем search_path для удобства
SET search_path TO staging, ruda_plus, star, public;

SELECT '--- Создание Staging-схемы ---' AS info;

-- ============================================================
-- Раздел 2: Таблицы управления ETL
-- Журнал загрузок и водяные знаки для инкрементальной загрузки
-- ============================================================

-- 2.1 Журнал загрузок (ETL Load Log)
-- Каждая загрузка (полная или инкрементальная) регистрируется здесь
CREATE TABLE IF NOT EXISTS staging.etl_load_log (
    load_id         SERIAL PRIMARY KEY,            -- Уникальный идентификатор загрузки
    table_name      VARCHAR(50) NOT NULL,           -- Имя загружаемой таблицы
    load_type       VARCHAR(20) NOT NULL             -- Тип загрузки: 'full' или 'incremental'
                    CHECK (load_type IN ('full', 'incremental')),
    status          VARCHAR(20) NOT NULL             -- Статус: 'running', 'completed', 'failed'
                    DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed')),
    rows_extracted  INTEGER DEFAULT 0,              -- Количество извлечённых строк
    rows_loaded     INTEGER DEFAULT 0,              -- Количество загруженных строк в целевую таблицу
    rows_rejected   INTEGER DEFAULT 0,              -- Количество отклонённых строк (ошибки качества)
    error_message   TEXT,                           -- Сообщение об ошибке (если status = 'failed')
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(), -- Время начала загрузки
    finished_at     TIMESTAMP                        -- Время завершения загрузки
);

COMMENT ON TABLE staging.etl_load_log IS 'Журнал ETL-загрузок: фиксирует каждую операцию извлечения и загрузки данных';
COMMENT ON COLUMN staging.etl_load_log.load_id IS 'Уникальный идентификатор загрузки (используется в staging-таблицах как _load_id)';
COMMENT ON COLUMN staging.etl_load_log.rows_extracted IS 'Количество строк, извлечённых из источника в staging';
COMMENT ON COLUMN staging.etl_load_log.rows_loaded IS 'Количество строк, загруженных из staging в целевое хранилище (star)';

-- 2.2 Водяные знаки (Watermarks)
-- Хранят точку последней успешной загрузки для каждой таблицы
CREATE TABLE IF NOT EXISTS staging.etl_watermark (
    table_name      VARCHAR(50) PRIMARY KEY,        -- Имя таблицы источника
    last_loaded_at  TIMESTAMP NOT NULL               -- Время последней загруженной записи
                    DEFAULT '1970-01-01 00:00:00',
    last_loaded_id  VARCHAR(50),                     -- ID последней загруженной записи (опционально)
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW() -- Время обновления водяного знака
);

COMMENT ON TABLE staging.etl_watermark IS 'Водяные знаки: точки последней успешной загрузки для инкрементального извлечения';
COMMENT ON COLUMN staging.etl_watermark.last_loaded_at IS 'Timestamp последней загруженной записи — основа для WHERE при инкрементальной загрузке';

-- ============================================================
-- Раздел 3: Staging-таблицы
-- Зеркальные копии OLTP-таблиц с добавлением ETL-метаданных
-- ============================================================

-- Каждая staging-таблица содержит:
--   _load_id        — ссылка на запись в etl_load_log
--   _load_timestamp — время загрузки строки
--   _source_system  — имя системы-источника
--   _row_hash       — MD5-хеш всех значимых полей (для обнаружения изменений)

-- 3.1 stg_mines — Шахты
CREATE TABLE IF NOT EXISTS staging.stg_mines (
    mine_id         VARCHAR(10),
    mine_name       VARCHAR(50),
    location        VARCHAR(100),
    region          VARCHAR(50),
    max_depth_m     INTEGER,
    status          VARCHAR(20),
    opened_date     DATE,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- ETL-метаданные
    _load_id        INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system  VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.mines',
    _row_hash       CHAR(32)            -- MD5 хеш бизнес-полей
);

COMMENT ON TABLE staging.stg_mines IS 'Staging: копия ruda_plus.mines с ETL-метаданными';

-- Индекс по бизнес-ключу для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_stg_mines_id ON staging.stg_mines(mine_id);

-- 3.2 stg_equipment_types — Типы оборудования
CREATE TABLE IF NOT EXISTS staging.stg_equipment_types (
    type_id         VARCHAR(10),
    type_name       VARCHAR(50),
    type_code       VARCHAR(10),
    description     TEXT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- ETL-метаданные
    _load_id        INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system  VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment_types',
    _row_hash       CHAR(32)
);

COMMENT ON TABLE staging.stg_equipment_types IS 'Staging: копия ruda_plus.equipment_types с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_eqtypes_id ON staging.stg_equipment_types(type_id);

-- 3.3 stg_equipment — Оборудование
CREATE TABLE IF NOT EXISTS staging.stg_equipment (
    equipment_id        VARCHAR(10),
    equipment_name      VARCHAR(50),
    type_id             VARCHAR(10),
    mine_id             VARCHAR(10),
    manufacturer        VARCHAR(50),
    model               VARCHAR(50),
    year_manufactured   INTEGER,
    serial_number       VARCHAR(30),
    max_payload_tons    NUMERIC(6,1),
    engine_hours        NUMERIC(10,1),
    status              VARCHAR(20),
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    -- ETL-метаданные
    _load_id            INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment',
    _row_hash           CHAR(32)
);

COMMENT ON TABLE staging.stg_equipment IS 'Staging: копия ruda_plus.equipment с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_equipment_id ON staging.stg_equipment(equipment_id);

-- 3.4 stg_operators — Операторы
CREATE TABLE IF NOT EXISTS staging.stg_operators (
    operator_id     VARCHAR(10),
    last_name       VARCHAR(50),
    first_name      VARCHAR(50),
    middle_name     VARCHAR(50),
    position        VARCHAR(50),
    qualification   VARCHAR(30),
    hire_date       DATE,
    mine_id         VARCHAR(10),
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- ETL-метаданные
    _load_id        INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system  VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.operators',
    _row_hash       CHAR(32)
);

COMMENT ON TABLE staging.stg_operators IS 'Staging: копия ruda_plus.operators с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_operators_id ON staging.stg_operators(operator_id);

-- 3.5 stg_ore_production — Добыча руды
CREATE TABLE IF NOT EXISTS staging.stg_ore_production (
    production_id       VARCHAR(10),
    production_date     DATE,
    shift               INTEGER,
    mine_id             VARCHAR(10),
    equipment_id        VARCHAR(10),
    operator_id         VARCHAR(10),
    block_id            VARCHAR(15),
    ore_type            VARCHAR(20),
    tonnage_extracted   NUMERIC(10,1),
    fe_content_pct      NUMERIC(5,2),
    moisture_pct        NUMERIC(5,2),
    status              VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    -- ETL-метаданные
    _load_id            INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.ore_production',
    _row_hash           CHAR(32)
);

COMMENT ON TABLE staging.stg_ore_production IS 'Staging: копия ruda_plus.ore_production с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_production_id ON staging.stg_ore_production(production_id);
CREATE INDEX IF NOT EXISTS idx_stg_production_date ON staging.stg_ore_production(production_date);

-- 3.6 stg_downtime_events — Простои
CREATE TABLE IF NOT EXISTS staging.stg_downtime_events (
    event_id            VARCHAR(10),
    equipment_id        VARCHAR(10),
    event_type          VARCHAR(30),
    event_category      VARCHAR(40),
    severity            VARCHAR(20),
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_minutes    INTEGER,
    description         TEXT,
    reported_by_id      VARCHAR(10),
    status              VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    -- ETL-метаданные
    _load_id            INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.downtime_events',
    _row_hash           CHAR(32)
);

COMMENT ON TABLE staging.stg_downtime_events IS 'Staging: копия ruda_plus.downtime_events с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_downtime_id ON staging.stg_downtime_events(event_id);
CREATE INDEX IF NOT EXISTS idx_stg_downtime_time ON staging.stg_downtime_events(start_time);

-- 3.7 stg_sensor_readings — Показания датчиков
CREATE TABLE IF NOT EXISTS staging.stg_sensor_readings (
    reading_id          VARCHAR(15),
    equipment_id        VARCHAR(10),
    sensor_type         VARCHAR(30),
    reading_timestamp   TIMESTAMP,
    sensor_value        NUMERIC(10,2),
    unit                VARCHAR(20),
    quality_flag        VARCHAR(10),
    created_at          TIMESTAMP,
    -- ETL-метаданные
    _load_id            INTEGER REFERENCES staging.etl_load_log(load_id),
    _load_timestamp     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_system      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.sensor_readings',
    _row_hash           CHAR(32)
);

COMMENT ON TABLE staging.stg_sensor_readings IS 'Staging: копия ruda_plus.sensor_readings с ETL-метаданными';
CREATE INDEX IF NOT EXISTS idx_stg_sensor_id ON staging.stg_sensor_readings(reading_id);
CREATE INDEX IF NOT EXISTS idx_stg_sensor_time ON staging.stg_sensor_readings(reading_timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_sensor_equip ON staging.stg_sensor_readings(equipment_id);

-- ============================================================
-- Раздел 4: Инициализация водяных знаков
-- Начальные значения для всех таблиц источника
-- ============================================================

INSERT INTO staging.etl_watermark (table_name, last_loaded_at, last_loaded_id)
VALUES
    ('mines',             '1970-01-01 00:00:00', NULL),
    ('equipment_types',   '1970-01-01 00:00:00', NULL),
    ('equipment',         '1970-01-01 00:00:00', NULL),
    ('operators',         '1970-01-01 00:00:00', NULL),
    ('ore_production',    '1970-01-01 00:00:00', NULL),
    ('downtime_events',   '1970-01-01 00:00:00', NULL),
    ('sensor_readings',   '1970-01-01 00:00:00', NULL)
ON CONFLICT (table_name) DO NOTHING;

-- ============================================================
-- Раздел 5: Вспомогательные функции
-- ============================================================

-- 5.1 Функция для создания новой записи в журнале загрузок
-- Возвращает load_id для использования в staging-таблицах
CREATE OR REPLACE FUNCTION staging.start_etl_load(
    p_table_name VARCHAR,
    p_load_type  VARCHAR DEFAULT 'full'
)
RETURNS INTEGER AS $$
DECLARE
    v_load_id INTEGER;
BEGIN
    INSERT INTO staging.etl_load_log (table_name, load_type, status)
    VALUES (p_table_name, p_load_type, 'running')
    RETURNING load_id INTO v_load_id;

    RAISE NOTICE 'ETL загрузка #% начата: таблица=%, тип=%',
                 v_load_id, p_table_name, p_load_type;
    RETURN v_load_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.start_etl_load IS 'Создаёт запись в журнале загрузок и возвращает load_id';

-- 5.2 Функция для завершения загрузки (успех)
CREATE OR REPLACE FUNCTION staging.finish_etl_load(
    p_load_id       INTEGER,
    p_rows_extracted INTEGER DEFAULT 0,
    p_rows_loaded    INTEGER DEFAULT 0,
    p_rows_rejected  INTEGER DEFAULT 0
)
RETURNS VOID AS $$
BEGIN
    UPDATE staging.etl_load_log
    SET status         = 'completed',
        rows_extracted = p_rows_extracted,
        rows_loaded    = p_rows_loaded,
        rows_rejected  = p_rows_rejected,
        finished_at    = NOW()
    WHERE load_id = p_load_id;

    RAISE NOTICE 'ETL загрузка #% завершена: извлечено=%, загружено=%, отклонено=%',
                 p_load_id, p_rows_extracted, p_rows_loaded, p_rows_rejected;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.finish_etl_load IS 'Отмечает загрузку как завершённую и записывает статистику';

-- 5.3 Функция для завершения загрузки (ошибка)
CREATE OR REPLACE FUNCTION staging.fail_etl_load(
    p_load_id      INTEGER,
    p_error_message TEXT
)
RETURNS VOID AS $$
BEGIN
    UPDATE staging.etl_load_log
    SET status        = 'failed',
        error_message = p_error_message,
        finished_at   = NOW()
    WHERE load_id = p_load_id;

    RAISE WARNING 'ETL загрузка #% ОШИБКА: %', p_load_id, p_error_message;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.fail_etl_load IS 'Отмечает загрузку как неуспешную и сохраняет сообщение об ошибке';

-- ============================================================
-- Раздел 6: Проверка результатов
-- ============================================================

SELECT '--- Staging: проверка создания объектов ---' AS info;

-- Все таблицы в staging
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'staging'
ORDER BY table_name;

-- Все функции в staging
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'staging'
ORDER BY routine_name;

-- Водяные знаки
SELECT table_name, last_loaded_at, last_loaded_id
FROM staging.etl_watermark
ORDER BY table_name;

SELECT '--- Staging-схема готова к использованию ---' AS info;
