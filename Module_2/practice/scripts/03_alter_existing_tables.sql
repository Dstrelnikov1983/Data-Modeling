-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 2. Практическая работа: Моделирование данных
-- Скрипт 3: Обновление существующих таблиц (миграция связей)
-- Предприятие: "Руда+" — добыча железной руды
--
-- ВАЖНО: Этот скрипт модифицирует таблицы, созданные в Модуле 1.
-- Убедитесь, что скрипты 01 и 02 из этого модуля уже выполнены.
-- ============================================================

SET search_path TO ruda_plus, public;

-- ============================================================
-- Шаг 1: Добавить type_id в таблицу equipment
-- Связь: equipment.type_id → equipment_types.type_id
-- ============================================================

-- 1.1 Добавляем столбец (если ещё не существует)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus'
          AND table_name = 'equipment'
          AND column_name = 'type_id'
    ) THEN
        ALTER TABLE equipment ADD COLUMN type_id VARCHAR(10);
    END IF;
END $$;

-- 1.2 Заполняем type_id на основе equipment_type (текстовое поле из Модуля 1)
UPDATE equipment SET type_id = 'ET-01' WHERE equipment_type = 'Погрузочно-доставочная машина';
UPDATE equipment SET type_id = 'ET-02' WHERE equipment_type = 'Шахтный самосвал';
UPDATE equipment SET type_id = 'ET-03' WHERE equipment_type = 'Вагонетка';
UPDATE equipment SET type_id = 'ET-04' WHERE equipment_type = 'Скиповый подъёмник';

-- 1.3 Проверяем, что все записи заполнены
SELECT equipment_id, equipment_name, equipment_type, type_id
FROM equipment
WHERE type_id IS NULL;
-- Если есть NULL — нужно добавить маппинг выше

-- 1.4 Создаём FK (если ещё не существует)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_equipment_type'
          AND table_schema = 'ruda_plus'
    ) THEN
        ALTER TABLE equipment
            ADD CONSTRAINT fk_equipment_type
            FOREIGN KEY (type_id) REFERENCES equipment_types(type_id);
    END IF;
END $$;

-- 1.5 Создаём индекс для нового FK
CREATE INDEX IF NOT EXISTS idx_equipment_type_id ON equipment(type_id);

-- ============================================================
-- Шаг 2: Связать equipment с mines через FK
-- В Модуле 1 mine_id уже есть, но FK не было
-- ============================================================

-- 2.1 Убедимся, что mine_id в equipment совпадает с mines
-- Маппинг: mine_name → mine_id
UPDATE equipment SET mine_id = 'MINE-01' WHERE mine_name = 'Северная';
UPDATE equipment SET mine_id = 'MINE-02' WHERE mine_name = 'Южная';
UPDATE equipment SET mine_id = 'MINE-03' WHERE mine_name = 'Восточная';

-- 2.2 Создаём FK (если ещё не существует)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_equipment_mine'
          AND table_schema = 'ruda_plus'
    ) THEN
        ALTER TABLE equipment
            ADD CONSTRAINT fk_equipment_mine
            FOREIGN KEY (mine_id) REFERENCES mines(mine_id);
    END IF;
END $$;

-- ============================================================
-- Шаг 3: Добавить operator_id в таблицу ore_production
-- Связь: ore_production.operator_id → operators.operator_id
-- ============================================================

-- 3.1 Добавляем столбец
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus'
          AND table_name = 'ore_production'
          AND column_name = 'operator_id'
    ) THEN
        ALTER TABLE ore_production ADD COLUMN operator_id VARCHAR(10);
    END IF;
END $$;

-- 3.2 Заполняем operator_id на основе operator_name (текстовое поле из Модуля 1)
UPDATE ore_production SET operator_id = 'OP-001' WHERE operator_name = 'Иванов А.А.';
UPDATE ore_production SET operator_id = 'OP-002' WHERE operator_name = 'Петров Б.Б.';
UPDATE ore_production SET operator_id = 'OP-003' WHERE operator_name = 'Сидоров В.В.';
UPDATE ore_production SET operator_id = 'OP-004' WHERE operator_name = 'Козлов Г.Г.';
UPDATE ore_production SET operator_id = 'OP-005' WHERE operator_name = 'Новиков Д.Д.';
UPDATE ore_production SET operator_id = 'OP-006' WHERE operator_name = 'Морозов Е.Е.';
UPDATE ore_production SET operator_id = 'OP-007' WHERE operator_name = 'Волков Ж.Ж.';
UPDATE ore_production SET operator_id = 'OP-008' WHERE operator_name = 'Соловьёв З.З.';
UPDATE ore_production SET operator_id = 'OP-009' WHERE operator_name = 'Васильев И.И.';

-- 3.3 Проверяем незаполненные
SELECT production_id, operator_name, operator_id
FROM ore_production
WHERE operator_id IS NULL;

-- 3.4 Создаём FK
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_production_operator'
          AND table_schema = 'ruda_plus'
    ) THEN
        ALTER TABLE ore_production
            ADD CONSTRAINT fk_production_operator
            FOREIGN KEY (operator_id) REFERENCES operators(operator_id);
    END IF;
END $$;

-- 3.5 Индекс
CREATE INDEX IF NOT EXISTS idx_production_operator ON ore_production(operator_id);

-- ============================================================
-- Шаг 4: Связать ore_production.mine_id с mines
-- ============================================================

-- 4.1 Обновляем mine_id в ore_production
UPDATE ore_production SET mine_id = 'MINE-01' WHERE mine_name = 'Северная';
UPDATE ore_production SET mine_id = 'MINE-02' WHERE mine_name = 'Южная';

-- 4.2 Создаём FK
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_production_mine'
          AND table_schema = 'ruda_plus'
    ) THEN
        ALTER TABLE ore_production
            ADD CONSTRAINT fk_production_mine
            FOREIGN KEY (mine_id) REFERENCES mines(mine_id);
    END IF;
END $$;

-- ============================================================
-- Шаг 5: Добавить reported_by_id в downtime_events
-- Связь: downtime_events.reported_by_id → operators.operator_id
-- ============================================================

-- 5.1 Добавляем столбец
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ruda_plus'
          AND table_name = 'downtime_events'
          AND column_name = 'reported_by_id'
    ) THEN
        ALTER TABLE downtime_events ADD COLUMN reported_by_id VARCHAR(10);
    END IF;
END $$;

-- 5.2 Маппинг reported_by → operator_id
UPDATE downtime_events SET reported_by_id = 'OP-001' WHERE reported_by = 'Иванов А.А.';
UPDATE downtime_events SET reported_by_id = 'OP-003' WHERE reported_by = 'Сидоров В.В.';
UPDATE downtime_events SET reported_by_id = 'OP-005' WHERE reported_by = 'Новиков Д.Д.';
UPDATE downtime_events SET reported_by_id = 'OP-006' WHERE reported_by = 'Морозов Е.Е.';
UPDATE downtime_events SET reported_by_id = 'OP-010' WHERE reported_by = 'Зайцев К.К.';
-- Для записей с другими значениями reported_by оставляем NULL

-- 5.3 Создаём FK
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_downtime_reported_by'
          AND table_schema = 'ruda_plus'
    ) THEN
        ALTER TABLE downtime_events
            ADD CONSTRAINT fk_downtime_reported_by
            FOREIGN KEY (reported_by_id) REFERENCES operators(operator_id);
    END IF;
END $$;

-- ============================================================
-- Проверка: итоговая структура модели
-- ============================================================

-- Все таблицы и количество столбцов
SELECT '--- Таблицы ---' AS info;
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'ruda_plus' AND c.table_name = t.table_name) AS columns_count
FROM information_schema.tables t
WHERE table_schema = 'ruda_plus'
ORDER BY table_name;

-- Все FK-связи
SELECT '--- Внешние ключи ---' AS info;
SELECT
    tc.table_name AS from_table,
    kcu.column_name AS from_column,
    ccu.table_name AS to_table,
    ccu.column_name AS to_column,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'ruda_plus'
ORDER BY tc.table_name, kcu.column_name;
