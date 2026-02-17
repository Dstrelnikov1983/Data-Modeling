-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 2. Практическая работа: Моделирование данных
-- Скрипт 4: Аналитические запросы по расширенной модели
-- Предприятие: "Руда+" — добыча железной руды
-- ============================================================

SET search_path TO ruda_plus, public;

-- ============================================================
-- Запрос 4.1. Оборудование по шахтам и типам
-- Сколько оборудования каждого типа в каждой шахте?
-- ============================================================
SELECT m.mine_name    AS "Шахта",
       et.type_name   AS "Тип оборудования",
       et.type_code   AS "Код",
       COUNT(*)       AS "Количество"
FROM equipment e
JOIN mines m ON e.mine_id = m.mine_id
JOIN equipment_types et ON e.type_id = et.type_id
GROUP BY m.mine_name, et.type_name, et.type_code
ORDER BY m.mine_name, "Количество" DESC;

-- ============================================================
-- Запрос 4.2. Производительность операторов
-- Средняя и суммарная добыча по операторам
-- ============================================================
SELECT o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS "Оператор",
       o.position       AS "Должность",
       o.qualification  AS "Квалификация",
       m.mine_name      AS "Шахта",
       COUNT(*)         AS "Кол-во смен",
       ROUND(AVG(p.tonnage_extracted), 1) AS "Средняя добыча, т",
       ROUND(SUM(p.tonnage_extracted), 1) AS "Всего добыто, т"
FROM ore_production p
JOIN operators o ON p.operator_id = o.operator_id
JOIN mines m ON o.mine_id = m.mine_id
WHERE p.status = 'Завершена'
GROUP BY o.operator_id, o.last_name, o.first_name, o.position, o.qualification, m.mine_name
ORDER BY "Средняя добыча, т" DESC;

-- ============================================================
-- Запрос 4.3. Простои по шахтам и типам оборудования
-- ============================================================
SELECT m.mine_name      AS "Шахта",
       et.type_name     AS "Тип оборудования",
       d.event_type     AS "Тип события",
       COUNT(*)         AS "Кол-во событий",
       SUM(d.duration_minutes)           AS "Всего минут",
       ROUND(AVG(d.duration_minutes), 0) AS "Средняя длительность"
FROM downtime_events d
JOIN equipment e ON d.equipment_id = e.equipment_id
JOIN mines m ON e.mine_id = m.mine_id
JOIN equipment_types et ON e.type_id = et.type_id
GROUP BY m.mine_name, et.type_name, d.event_type
ORDER BY "Всего минут" DESC;

-- ============================================================
-- Запрос 4.4. Комплексный отчёт — эффективность шахт
-- Сводная таблица по каждой действующей шахте
-- ============================================================
SELECT m.mine_name     AS "Шахта",
       m.region        AS "Регион",
       (SELECT COUNT(*)
        FROM equipment e
        WHERE e.mine_id = m.mine_id
       ) AS "Оборудование",
       (SELECT COUNT(*)
        FROM operators o
        WHERE o.mine_id = m.mine_id AND o.is_active = TRUE
       ) AS "Операторы",
       (SELECT ROUND(SUM(p.tonnage_extracted), 1)
        FROM ore_production p
        WHERE p.mine_id = m.mine_id AND p.status = 'Завершена'
       ) AS "Добыча (т)",
       (SELECT ROUND(AVG(p.fe_content_pct), 2)
        FROM ore_production p
        WHERE p.mine_id = m.mine_id AND p.status = 'Завершена'
       ) AS "Среднее Fe%",
       (SELECT COALESCE(SUM(d.duration_minutes), 0)
        FROM downtime_events d
        JOIN equipment e ON d.equipment_id = e.equipment_id
        WHERE e.mine_id = m.mine_id AND d.event_type = 'Незапланированный'
       ) AS "Незапл. простои (мин)"
FROM mines m
WHERE m.status = 'Действующая'
ORDER BY "Добыча (т)" DESC NULLS LAST;

-- ============================================================
-- Запрос 4.5. Операторы и их оборудование
-- На каком оборудовании работал каждый оператор
-- ============================================================
SELECT o.last_name || ' ' || o.first_name AS "Оператор",
       o.qualification AS "Квалификация",
       e.equipment_name AS "Оборудование",
       et.type_name AS "Тип",
       COUNT(p.production_id) AS "Смен",
       ROUND(SUM(p.tonnage_extracted), 1) AS "Добыча, т"
FROM operators o
JOIN ore_production p ON o.operator_id = p.operator_id
JOIN equipment e ON p.equipment_id = e.equipment_id
JOIN equipment_types et ON e.type_id = et.type_id
WHERE p.status = 'Завершена'
GROUP BY o.operator_id, o.last_name, o.first_name, o.qualification,
         e.equipment_id, e.equipment_name, et.type_name
ORDER BY o.last_name, "Добыча, т" DESC;

-- ============================================================
-- Запрос 4.6. Рейтинг оборудования по надёжности
-- Соотношение рабочих смен и простоев
-- ============================================================
SELECT e.equipment_name AS "Оборудование",
       et.type_name AS "Тип",
       m.mine_name AS "Шахта",
       (SELECT COUNT(*) FROM ore_production p
        WHERE p.equipment_id = e.equipment_id AND p.status = 'Завершена'
       ) AS "Завершённых смен",
       (SELECT COUNT(*) FROM downtime_events d
        WHERE d.equipment_id = e.equipment_id AND d.event_type = 'Незапланированный'
       ) AS "Незапл. простоев",
       (SELECT COALESCE(SUM(d.duration_minutes), 0) FROM downtime_events d
        WHERE d.equipment_id = e.equipment_id AND d.event_type = 'Незапланированный'
       ) AS "Время простоя, мин",
       e.engine_hours AS "Наработка, ч"
FROM equipment e
JOIN equipment_types et ON e.type_id = et.type_id
JOIN mines m ON e.mine_id = m.mine_id
ORDER BY "Незапл. простоев" DESC, "Завершённых смен" DESC;

-- ============================================================
-- Запрос 4.7. Преимущество нормализации:
-- Переименование шахты — достаточно обновить 1 запись
-- ============================================================

-- Без нормализации (Модуль 1): нужно обновить mine_name
-- в каждой таблице: equipment (12 строк), ore_production (15 строк) и т.д.
-- С нормализацией (Модуль 2): обновляем только в справочнике mines (1 строка)

-- Пример (НЕ выполняйте, только для демонстрации):
-- UPDATE mines SET mine_name = 'Северная-2' WHERE mine_id = 'MINE-01';
-- Все запросы автоматически покажут новое имя через JOIN!

SELECT '--- Демонстрация преимущества нормализации ---' AS info;
SELECT 'В Модуле 1: UPDATE equipment SET mine_name = ... WHERE mine_name = ''Северная''' AS "Без нормализации",
       'Затронуто строк: ' || (SELECT COUNT(*) FROM equipment WHERE mine_id = 'MINE-01') AS "equipment" ;
SELECT 'В Модуле 2: UPDATE mines SET mine_name = ... WHERE mine_id = ''MINE-01''' AS "С нормализацией",
       'Затронуто строк: 1' AS "mines";
