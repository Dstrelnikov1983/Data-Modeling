-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 3. Практическая работа: OLTP, OLAP, Data Vault
-- Скрипт 3: Создание Data Vault 2.0
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

-- Создаём отдельную схему
CREATE SCHEMA IF NOT EXISTS vault;
SET search_path TO vault, ruda_plus, public;

-- ============================================================
-- Hub 1: hub_mine (Шахта)
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.hub_mine (
    hub_mine_hk     CHAR(32) PRIMARY KEY,       -- MD5 hash от mine_id
    mine_id         VARCHAR(10) NOT NULL UNIQUE, -- бизнес-ключ
    load_dts        TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source   VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.mines'
);

COMMENT ON TABLE vault.hub_mine IS 'Data Vault Hub: Шахта (бизнес-ключ: mine_id)';

-- ============================================================
-- Hub 2: hub_equipment (Оборудование)
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.hub_equipment (
    hub_equipment_hk CHAR(32) PRIMARY KEY,
    equipment_id     VARCHAR(10) NOT NULL UNIQUE,
    load_dts         TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source    VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment'
);

COMMENT ON TABLE vault.hub_equipment IS 'Data Vault Hub: Оборудование (бизнес-ключ: equipment_id)';

-- ============================================================
-- Hub 3: hub_operator (Оператор)
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.hub_operator (
    hub_operator_hk CHAR(32) PRIMARY KEY,
    operator_id     VARCHAR(10) NOT NULL UNIQUE,
    load_dts        TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source   VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.operators'
);

COMMENT ON TABLE vault.hub_operator IS 'Data Vault Hub: Оператор (бизнес-ключ: operator_id)';

-- ============================================================
-- Satellite: sat_mine_details
-- Описательные атрибуты шахты с историей
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.sat_mine_details (
    hub_mine_hk     CHAR(32) NOT NULL REFERENCES vault.hub_mine(hub_mine_hk),
    load_dts        TIMESTAMP NOT NULL,
    load_end_dts    TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    mine_name       VARCHAR(50),
    location        VARCHAR(100),
    region          VARCHAR(50),
    max_depth_m     INTEGER,
    status          VARCHAR(20),
    opened_date     DATE,
    hash_diff       CHAR(32),       -- MD5 hash атрибутов
    record_source   VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.mines',
    PRIMARY KEY (hub_mine_hk, load_dts)
);

COMMENT ON TABLE vault.sat_mine_details IS 'Data Vault Satellite: Детали шахты';

-- ============================================================
-- Satellite: sat_equipment_details
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.sat_equipment_details (
    hub_equipment_hk CHAR(32) NOT NULL REFERENCES vault.hub_equipment(hub_equipment_hk),
    load_dts         TIMESTAMP NOT NULL,
    load_end_dts     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    equipment_name   VARCHAR(50),
    manufacturer     VARCHAR(50),
    model            VARCHAR(50),
    year_manufactured INTEGER,
    max_payload_tons NUMERIC(6,1),
    hash_diff        CHAR(32),
    record_source    VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment',
    PRIMARY KEY (hub_equipment_hk, load_dts)
);

COMMENT ON TABLE vault.sat_equipment_details IS 'Data Vault Satellite: Характеристики оборудования';

-- ============================================================
-- Satellite: sat_equipment_status
-- Отдельный сателлит для часто меняющихся данных
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.sat_equipment_status (
    hub_equipment_hk CHAR(32) NOT NULL REFERENCES vault.hub_equipment(hub_equipment_hk),
    load_dts         TIMESTAMP NOT NULL,
    load_end_dts     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    status           VARCHAR(20),
    engine_hours     NUMERIC(10,1),
    last_maintenance DATE,
    next_maintenance DATE,
    hash_diff        CHAR(32),
    record_source    VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment',
    PRIMARY KEY (hub_equipment_hk, load_dts)
);

COMMENT ON TABLE vault.sat_equipment_status IS 'Data Vault Satellite: Статус и наработка оборудования';

-- ============================================================
-- Satellite: sat_operator_details
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.sat_operator_details (
    hub_operator_hk CHAR(32) NOT NULL REFERENCES vault.hub_operator(hub_operator_hk),
    load_dts        TIMESTAMP NOT NULL,
    load_end_dts    TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    last_name       VARCHAR(50),
    first_name      VARCHAR(50),
    middle_name     VARCHAR(50),
    position        VARCHAR(50),
    qualification   VARCHAR(30),
    hire_date       DATE,
    is_active       BOOLEAN,
    hash_diff       CHAR(32),
    record_source   VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.operators',
    PRIMARY KEY (hub_operator_hk, load_dts)
);

COMMENT ON TABLE vault.sat_operator_details IS 'Data Vault Satellite: Данные оператора';

-- ============================================================
-- Link 1: link_equipment_mine (Оборудование ↔ Шахта)
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.link_equipment_mine (
    link_equip_mine_hk CHAR(32) PRIMARY KEY,
    hub_equipment_hk   CHAR(32) NOT NULL REFERENCES vault.hub_equipment(hub_equipment_hk),
    hub_mine_hk        CHAR(32) NOT NULL REFERENCES vault.hub_mine(hub_mine_hk),
    load_dts           TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.equipment'
);

COMMENT ON TABLE vault.link_equipment_mine IS 'Data Vault Link: Оборудование → Шахта';

-- ============================================================
-- Link 2: link_operator_mine (Оператор ↔ Шахта)
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.link_operator_mine (
    link_oper_mine_hk CHAR(32) PRIMARY KEY,
    hub_operator_hk   CHAR(32) NOT NULL REFERENCES vault.hub_operator(hub_operator_hk),
    hub_mine_hk       CHAR(32) NOT NULL REFERENCES vault.hub_mine(hub_mine_hk),
    load_dts          TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source     VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.operators'
);

COMMENT ON TABLE vault.link_operator_mine IS 'Data Vault Link: Оператор → Шахта';

-- ============================================================
-- Link 3: link_production (Транзакция добычи)
-- Связывает Mine, Equipment, Operator
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.link_production (
    link_production_hk CHAR(32) PRIMARY KEY,
    hub_mine_hk        CHAR(32) NOT NULL REFERENCES vault.hub_mine(hub_mine_hk),
    hub_equipment_hk   CHAR(32) NOT NULL REFERENCES vault.hub_equipment(hub_equipment_hk),
    hub_operator_hk    CHAR(32) REFERENCES vault.hub_operator(hub_operator_hk),
    production_id      VARCHAR(10) NOT NULL,  -- degenerate dimension
    load_dts           TIMESTAMP NOT NULL DEFAULT NOW(),
    record_source      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.ore_production'
);

COMMENT ON TABLE vault.link_production IS 'Data Vault Link: Транзакция добычи (Mine + Equipment + Operator)';

-- ============================================================
-- Satellite на Link: sat_production_metrics
-- Метрики добычи
-- ============================================================

CREATE TABLE IF NOT EXISTS vault.sat_production_metrics (
    link_production_hk CHAR(32) NOT NULL REFERENCES vault.link_production(link_production_hk),
    load_dts           TIMESTAMP NOT NULL,
    load_end_dts       TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    production_date    DATE,
    shift              INTEGER,
    block_id           VARCHAR(15),
    ore_type           VARCHAR(20),
    tonnage_extracted  NUMERIC(10,1),
    fe_content_pct     NUMERIC(5,2),
    moisture_pct       NUMERIC(5,2),
    status             VARCHAR(20),
    hash_diff          CHAR(32),
    record_source      VARCHAR(50) NOT NULL DEFAULT 'ruda_plus.ore_production',
    PRIMARY KEY (link_production_hk, load_dts)
);

COMMENT ON TABLE vault.sat_production_metrics IS 'Data Vault Satellite: Метрики добычи на link_production';

-- ============================================================
-- Загрузка данных в Data Vault
-- ============================================================

-- Загрузка Hubs
INSERT INTO vault.hub_mine (hub_mine_hk, mine_id)
SELECT MD5(mine_id), mine_id
FROM ruda_plus.mines
ON CONFLICT (mine_id) DO NOTHING;

INSERT INTO vault.hub_equipment (hub_equipment_hk, equipment_id)
SELECT MD5(equipment_id), equipment_id
FROM ruda_plus.equipment
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO vault.hub_operator (hub_operator_hk, operator_id)
SELECT MD5(operator_id), operator_id
FROM ruda_plus.operators
ON CONFLICT (operator_id) DO NOTHING;

-- Загрузка Satellites
INSERT INTO vault.sat_mine_details (hub_mine_hk, load_dts, mine_name, location, region, max_depth_m, status, opened_date, hash_diff)
SELECT MD5(mine_id), NOW(),
       mine_name, location, region, max_depth_m, status, opened_date,
       MD5(CONCAT_WS('|', mine_name, location, region, max_depth_m, status, opened_date))
FROM ruda_plus.mines
ON CONFLICT DO NOTHING;

INSERT INTO vault.sat_equipment_details (hub_equipment_hk, load_dts, equipment_name, manufacturer, model, year_manufactured, max_payload_tons, hash_diff)
SELECT MD5(equipment_id), NOW(),
       equipment_name, manufacturer, model, year_manufactured, max_payload_tons,
       MD5(CONCAT_WS('|', equipment_name, manufacturer, model, year_manufactured, max_payload_tons))
FROM ruda_plus.equipment
ON CONFLICT DO NOTHING;

INSERT INTO vault.sat_equipment_status (hub_equipment_hk, load_dts, status, engine_hours, last_maintenance, next_maintenance, hash_diff)
SELECT MD5(equipment_id), NOW(),
       status, engine_hours, last_maintenance_date, next_maintenance_date,
       MD5(CONCAT_WS('|', status, engine_hours, last_maintenance_date, next_maintenance_date))
FROM ruda_plus.equipment
ON CONFLICT DO NOTHING;

INSERT INTO vault.sat_operator_details (hub_operator_hk, load_dts, last_name, first_name, middle_name, position, qualification, hire_date, is_active, hash_diff)
SELECT MD5(operator_id), NOW(),
       last_name, first_name, middle_name, position, qualification, hire_date, is_active,
       MD5(CONCAT_WS('|', last_name, first_name, position, qualification, hire_date, is_active))
FROM ruda_plus.operators
ON CONFLICT DO NOTHING;

-- Загрузка Links
INSERT INTO vault.link_equipment_mine (link_equip_mine_hk, hub_equipment_hk, hub_mine_hk)
SELECT MD5(CONCAT_WS('|', equipment_id, mine_id)),
       MD5(equipment_id),
       MD5(mine_id)
FROM ruda_plus.equipment
ON CONFLICT DO NOTHING;

INSERT INTO vault.link_operator_mine (link_oper_mine_hk, hub_operator_hk, hub_mine_hk)
SELECT MD5(CONCAT_WS('|', operator_id, mine_id)),
       MD5(operator_id),
       MD5(mine_id)
FROM ruda_plus.operators
WHERE mine_id IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO vault.link_production (link_production_hk, hub_mine_hk, hub_equipment_hk, hub_operator_hk, production_id)
SELECT MD5(CONCAT_WS('|', p.mine_id, p.equipment_id, COALESCE(p.operator_id, ''), p.production_id)),
       MD5(p.mine_id),
       MD5(p.equipment_id),
       CASE WHEN p.operator_id IS NOT NULL THEN MD5(p.operator_id) ELSE NULL END,
       p.production_id
FROM ruda_plus.ore_production p
ON CONFLICT DO NOTHING;

-- Загрузка Satellite на Link
INSERT INTO vault.sat_production_metrics (link_production_hk, load_dts, production_date, shift, block_id, ore_type, tonnage_extracted, fe_content_pct, moisture_pct, status, hash_diff)
SELECT MD5(CONCAT_WS('|', p.mine_id, p.equipment_id, COALESCE(p.operator_id, ''), p.production_id)),
       NOW(),
       p.production_date, p.shift, p.block_id, p.ore_type,
       p.tonnage_extracted, p.fe_content_pct, p.moisture_pct, p.status,
       MD5(CONCAT_WS('|', p.production_date, p.shift, p.block_id, p.tonnage_extracted, p.fe_content_pct))
FROM ruda_plus.ore_production p
ON CONFLICT DO NOTHING;

-- ============================================================
-- Проверка загрузки
-- ============================================================

SELECT '--- Data Vault: проверка загрузки ---' AS info;
SELECT 'hub_mine' AS table_name, COUNT(*) AS rows FROM vault.hub_mine
UNION ALL SELECT 'hub_equipment', COUNT(*) FROM vault.hub_equipment
UNION ALL SELECT 'hub_operator', COUNT(*) FROM vault.hub_operator
UNION ALL SELECT 'sat_mine_details', COUNT(*) FROM vault.sat_mine_details
UNION ALL SELECT 'sat_equipment_details', COUNT(*) FROM vault.sat_equipment_details
UNION ALL SELECT 'sat_equipment_status', COUNT(*) FROM vault.sat_equipment_status
UNION ALL SELECT 'sat_operator_details', COUNT(*) FROM vault.sat_operator_details
UNION ALL SELECT 'link_equipment_mine', COUNT(*) FROM vault.link_equipment_mine
UNION ALL SELECT 'link_operator_mine', COUNT(*) FROM vault.link_operator_mine
UNION ALL SELECT 'link_production', COUNT(*) FROM vault.link_production
UNION ALL SELECT 'sat_production_metrics', COUNT(*) FROM vault.sat_production_metrics
ORDER BY table_name;
