-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 2. Практическая работа: Моделирование данных
-- Скрипт 1: Создание справочных таблиц
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

-- Устанавливаем схему по умолчанию
SET search_path TO ruda_plus, public;

-- ============================================================
-- Таблица: Справочник шахт
-- Связь: mines 1:N equipment, mines 1:N operators, mines 1:N ore_production
-- ============================================================
CREATE TABLE IF NOT EXISTS mines (
    mine_id       VARCHAR(10)  PRIMARY KEY,
    mine_name     VARCHAR(50)  NOT NULL,
    location      VARCHAR(100),            -- GPS-координаты (широта;долгота)
    region        VARCHAR(50)  NOT NULL,
    max_depth_m   INTEGER,
    status        VARCHAR(20)  DEFAULT 'Действующая',
    opened_date   DATE,

    CONSTRAINT chk_mine_status
        CHECK (status IN ('Действующая', 'Консервация', 'Закрыта')),
    CONSTRAINT chk_mine_depth
        CHECK (max_depth_m > 0 OR max_depth_m IS NULL)
);

COMMENT ON TABLE mines IS 'Справочник шахт предприятия Руда+';
COMMENT ON COLUMN mines.mine_id IS 'Уникальный идентификатор шахты (MINE-XX)';
COMMENT ON COLUMN mines.location IS 'GPS-координаты входа в шахту (широта;долгота)';
COMMENT ON COLUMN mines.max_depth_m IS 'Максимальная глубина шахты в метрах';

-- ============================================================
-- Таблица: Типы оборудования
-- Связь: equipment_types 1:N equipment
-- ============================================================
CREATE TABLE IF NOT EXISTS equipment_types (
    type_id       VARCHAR(10)  PRIMARY KEY,
    type_name     VARCHAR(50)  NOT NULL,
    type_code     VARCHAR(10)  NOT NULL UNIQUE,   -- Краткий код (ПДМ, СС, ВГ, СП)
    description   TEXT,

    CONSTRAINT chk_type_code_not_empty
        CHECK (LENGTH(TRIM(type_code)) > 0)
);

COMMENT ON TABLE equipment_types IS 'Справочник типов горнодобывающего оборудования';
COMMENT ON COLUMN equipment_types.type_code IS 'Краткий код типа: ПДМ, СС, ВГ, СП, БУ, ВУ';

-- ============================================================
-- Таблица: Операторы
-- Связь: operators N:1 mines, operators 1:N ore_production
-- ============================================================
CREATE TABLE IF NOT EXISTS operators (
    operator_id   VARCHAR(10)  PRIMARY KEY,
    last_name     VARCHAR(50)  NOT NULL,
    first_name    VARCHAR(50)  NOT NULL,
    middle_name   VARCHAR(50),
    position      VARCHAR(50)  NOT NULL,       -- Должность
    qualification VARCHAR(30),                 -- Разряд / квалификация
    hire_date     DATE         NOT NULL,
    mine_id       VARCHAR(10)  REFERENCES mines(mine_id),
    is_active     BOOLEAN      DEFAULT TRUE,

    CONSTRAINT chk_hire_date
        CHECK (hire_date <= CURRENT_DATE)
);

COMMENT ON TABLE operators IS 'Справочник операторов горнодобывающего оборудования';
COMMENT ON COLUMN operators.position IS 'Должность: Машинист ПДМ, Водитель самосвала и др.';
COMMENT ON COLUMN operators.qualification IS 'Квалификация: 4-6 разряд, Инженер-механик и др.';

-- Индекс для поиска операторов по шахте
CREATE INDEX IF NOT EXISTS idx_operators_mine
    ON operators(mine_id);

-- Индекс для поиска активных операторов
CREATE INDEX IF NOT EXISTS idx_operators_active
    ON operators(is_active)
    WHERE is_active = TRUE;

-- ============================================================
-- Проверка: все таблицы созданы
-- ============================================================
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'ruda_plus' AND c.table_name = t.table_name) AS columns_count
FROM information_schema.tables t
WHERE table_schema = 'ruda_plus'
ORDER BY table_name;
