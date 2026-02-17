-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 3. Практическая работа: OLTP, OLAP, Data Vault
-- Скрипт 1: Проверка нормализации OLTP-модели (3НФ)
-- Предприятие: "Руда+" — добыча железной руды
--
-- ВАЖНО: Убедитесь, что скрипты модулей 1 и 2 уже выполнены.
-- ============================================================

SET search_path TO ruda_plus, public;

-- ============================================================
-- Шаг 1: Проверка текущей структуры (результат модулей 1–2)
-- ============================================================

-- 1.1 Все таблицы в схеме
SELECT '--- Текущие таблицы ---' AS info;
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'ruda_plus' AND c.table_name = t.table_name) AS columns_count
FROM information_schema.tables t
WHERE table_schema = 'ruda_plus'
ORDER BY table_name;

-- 1.2 Все FK-связи
SELECT '--- Внешние ключи ---' AS info;
SELECT tc.table_name  AS from_table,
       kcu.column_name AS from_column,
       ccu.table_name  AS to_table,
       ccu.column_name AS to_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'ruda_plus'
ORDER BY tc.table_name;

-- ============================================================
-- Шаг 2: Анализ на соответствие 1НФ
-- Проверяем: нет ли составных / многозначных полей
-- ============================================================

SELECT '--- Проверка 1НФ ---' AS info;

-- 2.1 Поля с запятыми или разделителями (подозрение на многозначность)
SELECT 'equipment' AS table_name, equipment_id, equipment_name
FROM equipment
WHERE equipment_name LIKE '%,%' OR equipment_name LIKE '%;%';
-- Ожидаемый результат: 0 строк (нет нарушений)

SELECT 'ore_production' AS table_name, production_id, operator_name
FROM ore_production
WHERE operator_name LIKE '%,%';
-- Ожидаемый результат: 0 строк

-- 2.2 Вывод: модель «Руда+» уже в 1НФ
-- Каждый столбец содержит атомарные значения.

-- ============================================================
-- Шаг 3: Анализ на соответствие 2НФ
-- Проверяем: все неключевые атрибуты зависят от полного PK
-- ============================================================

SELECT '--- Проверка 2НФ ---' AS info;

-- Все наши таблицы имеют простой PK (одно поле).
-- 2НФ автоматически выполняется, если PK не составной.

SELECT table_name, constraint_name,
       string_agg(column_name, ', ' ORDER BY ordinal_position) AS pk_columns,
       COUNT(*) AS pk_column_count
FROM information_schema.key_column_usage kcu
JOIN information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = 'ruda_plus'
GROUP BY table_name, constraint_name
ORDER BY table_name;
-- Все PK — одно поле → 2НФ выполняется автоматически

-- ============================================================
-- Шаг 4: Анализ на соответствие 3НФ
-- Проверяем: нет ли транзитивных зависимостей
-- ============================================================

SELECT '--- Проверка 3НФ: ищем дублирование ---' AS info;

-- 4.1 В equipment: есть ли текстовые поля, дублирующие справочники?
-- mine_name — дублирует mines.mine_name (есть mine_id FK)
-- equipment_type — дублирует equipment_types.type_name (есть type_id FK)
SELECT equipment_id, mine_name, mine_id, equipment_type, type_id
FROM equipment
LIMIT 5;
-- mine_name и equipment_type — транзитивные зависимости!
-- mine_name зависит от mine_id → mines.mine_name (через FK)
-- equipment_type зависит от type_id → equipment_types.type_name

-- 4.2 В ore_production: operator_name дублирует operators
SELECT production_id, operator_name, operator_id
FROM ore_production
LIMIT 5;
-- operator_name — транзитивная зависимость!

-- 4.3 В downtime_events: reported_by дублирует operators
SELECT event_id, reported_by, reported_by_id
FROM downtime_events
LIMIT 5;
-- reported_by — транзитивная зависимость!

-- ============================================================
-- Шаг 5: Устранение нарушений 3НФ
-- Удаляем текстовые поля, которые дублируют справочники
-- ============================================================

SELECT '--- Приведение к 3НФ ---' AS info;

-- 5.1 Сохраняем текущее состояние (для отката)
-- Создаём резервные столбцы (комментируем — на практике используйте бэкап)
-- ALTER TABLE equipment RENAME COLUMN mine_name TO _mine_name_backup;

-- 5.2 Удаляем дублирующие текстовые поля из equipment
-- ВНИМАНИЕ: выполняйте после проверки, что FK заполнены корректно!
DO $$
BEGIN
    -- Удаляем mine_name (дублируется через mine_id → mines)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus' AND table_name = 'equipment' AND column_name = 'mine_name'
    ) THEN
        ALTER TABLE equipment DROP COLUMN mine_name;
        RAISE NOTICE 'equipment.mine_name удалён (дубль mines.mine_name)';
    END IF;

    -- Удаляем equipment_type (дублируется через type_id → equipment_types)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus' AND table_name = 'equipment' AND column_name = 'equipment_type'
    ) THEN
        ALTER TABLE equipment DROP COLUMN equipment_type;
        RAISE NOTICE 'equipment.equipment_type удалён (дубль equipment_types.type_name)';
    END IF;
END $$;

-- 5.3 Удаляем дублирующие поля из ore_production
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus' AND table_name = 'ore_production' AND column_name = 'mine_name'
    ) THEN
        ALTER TABLE ore_production DROP COLUMN mine_name;
        RAISE NOTICE 'ore_production.mine_name удалён';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus' AND table_name = 'ore_production' AND column_name = 'operator_name'
    ) THEN
        ALTER TABLE ore_production DROP COLUMN operator_name;
        RAISE NOTICE 'ore_production.operator_name удалён';
    END IF;
END $$;

-- 5.4 Удаляем дублирующее поле из downtime_events
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus' AND table_name = 'downtime_events' AND column_name = 'reported_by'
    ) THEN
        ALTER TABLE downtime_events DROP COLUMN reported_by;
        RAISE NOTICE 'downtime_events.reported_by удалён';
    END IF;
END $$;

-- ============================================================
-- Шаг 6: Проверка — модель теперь полностью в 3НФ
-- ============================================================

SELECT '--- Проверка: итоговая структура 3НФ ---' AS info;

-- Все таблицы и столбцы
SELECT c.table_name,
       c.column_name,
       c.data_type,
       c.is_nullable,
       CASE
           WHEN pk.column_name IS NOT NULL THEN 'PK'
           WHEN fk.column_name IS NOT NULL THEN 'FK → ' || fk.to_table
           ELSE ''
       END AS key_type
FROM information_schema.columns c
LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'ruda_plus'
) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name, ccu.table_name AS to_table
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'ruda_plus'
) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
WHERE c.table_schema = 'ruda_plus'
ORDER BY c.table_name, c.ordinal_position;

-- Проверка: запрос работает через JOIN, а не через текстовые поля
SELECT '--- Проверочный запрос (должен работать без текстовых дублей) ---' AS info;
SELECT e.equipment_id,
       e.equipment_name,
       et.type_name AS equipment_type,
       m.mine_name
FROM equipment e
JOIN equipment_types et ON e.type_id = et.type_id
JOIN mines m ON e.mine_id = m.mine_id
ORDER BY e.equipment_id;
