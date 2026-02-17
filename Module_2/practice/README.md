# Практическая работа 2. Моделирование данных MES «Руда+»

## Общая информация

| Параметр | Значение |
|---|---|
| **Модуль** | 2. Основы моделирования данных |
| **Темы** | 2.1–2.3. Концепции, концептуальное моделирование, инструменты |
| **Длительность** | 90–120 минут |
| **Формат** | Индивидуальная работа / работа в парах |
| **Среда** | dbdiagram.io + Yandex Cloud (Managed PostgreSQL) |

## Цель работы

Пройти полный цикл моделирования данных — от анализа бизнес-требований до реализации физической модели в PostgreSQL. Вы расширите модель данных из модуля 1, добавив новые сущности (шахты, операторы) и нормализовав структуру.

## Предварительные требования

- Выполнена практическая работа модуля 1 (таблицы `equipment`, `sensor_readings`, `ore_production`, `downtime_events` созданы и заполнены)
- Доступ к PostgreSQL (Yandex Cloud или локальный)
- Браузер для работы с dbdiagram.io
- Текстовый редактор или DBeaver

## Подготовленные материалы

```
practice/
├── README.md                          ← вы читаете этот файл
├── data/
│   ├── mines.csv                      ← справочник шахт (4 записи)
│   ├── horizons.csv                   ← горизонты шахт (12 записей)
│   ├── operators.csv                  ← операторы (10 записей)
│   ├── equipment_types.csv            ← типы оборудования (6 записей)
│   └── business_requirements.json     ← бизнес-требования MES в JSON
└── scripts/
    ├── 01_create_reference_tables.sql ← DDL: справочные таблицы (mines, operators и др.)
    ├── 02_load_reference_data.sql     ← загрузка данных в справочники
    ├── 03_alter_existing_tables.sql   ← обновление связей существующих таблиц
    └── 04_analytical_queries.sql      ← аналитические запросы по расширенной модели
```

---

## Часть 1. Анализ бизнес-требований (20 минут)

### Шаг 1.1. Изучите бизнес-требования

Откройте файл `data/business_requirements.json` — это структурированное описание требований к MES-системе «Руда+», собранное на этапе интервью с заказчиком.

Прочитайте требования и ответьте на вопросы:

1. Сколько подсистем включает MES «Руда+»?
2. Какие роли пользователей описаны?
3. Какие отчёты требуются руководству?

### Шаг 1.2. Выделите сущности

На основе бизнес-требований составьте список сущностей. Для каждой:

| № | Сущность | Описание | Примеры экземпляров |
|---|---|---|---|
| 1 | ? | ? | ? |
| 2 | ? | ? | ? |
| ... | | | |

> **Подсказка:** Ищите существительные в описании. «Шахта», «оборудование», «оператор», «датчик» — это сущности. «Добывать», «ремонтировать» — это процессы (не сущности).

### Шаг 1.3. Определите связи

Для каждой пары связанных сущностей определите:

| Сущность A | Связь (глагол) | Сущность B | Кардинальность |
|---|---|---|---|
| Шахта | содержит | Оборудование | 1 : N |
| ? | ? | ? | ? |

### Шаг 1.4. Нарисуйте концептуальную модель

На листе бумаги или в draw.io нарисуйте концептуальную модель:
- Прямоугольники = сущности
- Линии = связи
- Подписи на линиях = глаголы
- Кардинальность = 1:1, 1:N, N:M

> **Критерий готовности:** Модель содержит минимум 8 сущностей и 8 связей.

<details>
<summary>Ожидаемый результат (раскройте после выполнения)</summary>

Ключевые сущности:
1. **Шахта** (Mine)
2. **Горизонт** (Horizon)
3. **Оборудование** (Equipment)
4. **Тип оборудования** (Equipment Type)
5. **Датчик / Показание** (Sensor Reading)
6. **Оператор** (Operator)
7. **Смена добычи** (Ore Production)
8. **Событие простоя** (Downtime Event)

Ключевые связи:
- Шахта 1:N Горизонт
- Шахта 1:N Оборудование
- Тип оборудования 1:N Оборудование
- Оборудование 1:N Показания датчиков
- Оборудование 1:N Смена добычи
- Оборудование 1:N Событие простоя
- Оператор 1:N Смена добычи
- Горизонт 1:N Смена добычи

</details>

---

## Часть 2. Логическая модель в dbdiagram.io (30 минут)

### Шаг 2.1. Откройте dbdiagram.io

1. Перейдите на [dbdiagram.io](https://dbdiagram.io/)
2. Нажмите **Create new diagram**
3. Очистите редактор слева

### Шаг 2.2. Создайте справочные таблицы

Введите в редактор следующий DBML-код для справочников:

```dbml
// ================================
// MES «Руда+» — Логическая модель
// Модуль 2: Основы моделирования
// ================================

// --- Справочники ---

Table mines {
  mine_id varchar(10) [pk]
  mine_name varchar(50) [not null]
  location varchar(100)
  region varchar(50) [not null]
  max_depth_m integer
  status varchar(20) [default: 'Действующая']
  opened_date date
  Note: 'Справочник шахт предприятия Руда+'
}

Table equipment_types {
  type_id varchar(10) [pk]
  type_name varchar(50) [not null]
  type_code varchar(10) [not null, unique]
  description text
  Note: 'Типы горнодобывающего оборудования'
}

Table operators {
  operator_id varchar(10) [pk]
  last_name varchar(50) [not null]
  first_name varchar(50) [not null]
  middle_name varchar(50)
  position varchar(50) [not null]
  qualification varchar(30)
  hire_date date [not null]
  mine_id varchar(10) [ref: > mines.mine_id]
  is_active boolean [default: true]
  Note: 'Операторы горнодобывающего оборудования'
}

Table horizons {
  horizon_id varchar(10) [pk]
  mine_id varchar(10) [not null, ref: > mines.mine_id]
  level_name varchar(30) [not null]
  depth_m integer [not null]
  ore_body varchar(50)
  status varchar(20) [default: 'Активный']
  Note: 'Горизонты (уровни) шахт'
}
```

### Шаг 2.3. Создайте основные таблицы

Добавьте в редактор таблицы для основных сущностей:

```dbml
// --- Основные таблицы ---

Table equipment {
  equipment_id varchar(10) [pk]
  equipment_name varchar(50) [not null]
  type_id varchar(10) [not null, ref: > equipment_types.type_id]
  manufacturer varchar(50) [not null]
  model varchar(50) [not null]
  year_manufactured integer [not null]
  mine_id varchar(10) [not null, ref: > mines.mine_id]
  status varchar(20) [default: 'В работе']
  last_maintenance_date date
  next_maintenance_date date
  engine_hours decimal(10,1) [default: 0]
  max_payload_tons decimal(6,1)
  Note: 'Справочник горнодобывающего оборудования'
}

Table sensor_readings {
  reading_id varchar(12) [pk]
  equipment_id varchar(10) [not null, ref: > equipment.equipment_id]
  sensor_type varchar(30) [not null]
  reading_value decimal(10,2) [not null]
  unit varchar(10) [not null]
  reading_timestamp timestamp [not null]
  quality_flag varchar(10) [default: 'OK']
  Note: 'Показания датчиков оборудования (телеметрия)'
}

Table ore_production {
  production_id varchar(10) [pk]
  mine_id varchar(10) [not null, ref: > mines.mine_id]
  horizon_id varchar(10) [ref: > horizons.horizon_id]
  production_date date [not null]
  shift integer [not null]
  block_id varchar(15) [not null]
  ore_type varchar(20) [not null]
  tonnage_extracted decimal(10,1) [not null, default: 0]
  fe_content_pct decimal(5,2) [not null, default: 0]
  moisture_pct decimal(5,2) [not null, default: 0]
  equipment_id varchar(10) [not null, ref: > equipment.equipment_id]
  operator_id varchar(10) [ref: > operators.operator_id]
  start_time time [not null]
  end_time time [not null]
  status varchar(20) [default: 'Завершена']
  Note: 'Журнал добычи руды по сменам'
}

Table downtime_events {
  event_id varchar(10) [pk]
  equipment_id varchar(10) [not null, ref: > equipment.equipment_id]
  event_type varchar(30) [not null]
  event_category varchar(40) [not null]
  start_time timestamp [not null]
  end_time timestamp
  duration_minutes integer
  description text
  severity varchar(20) [not null]
  reported_by varchar(10) [ref: > operators.operator_id]
  Note: 'Журнал простоев и событий обслуживания'
}
```

### Шаг 2.4. Проверьте диаграмму

После ввода кода dbdiagram.io автоматически построит ER-диаграмму. Проверьте:

1. Все ли таблицы отображаются? (Ожидается 8 таблиц)
2. Все ли связи прорисованы? (Ожидается 10 связей)
3. Верна ли кардинальность? (Все связи 1:N)
4. Нет ли «сиротских» таблиц без связей?

### Шаг 2.5. Экспортируйте SQL

1. Нажмите **Export** → **PostgreSQL**
2. Сохраните файл — это ваша логическая модель, транслированная в SQL
3. Сравните с нашим скриптом `scripts/01_create_reference_tables.sql`

> **Вопрос для размышления:** Чем экспортированный SQL отличается от нашего? Какие элементы добавляет инженер при переходе к физической модели?

---

## Часть 3. Физическая модель в PostgreSQL (40 минут)

### Шаг 3.1. Подключение к базе данных

Подключитесь к базе данных `ruda_plus_db` (должна остаться от модуля 1):

#### Yandex Cloud:
```bash
psql "host=<хост-кластера> \
      port=6432 \
      dbname=ruda_plus_db \
      user=student \
      sslmode=verify-full"
```

#### Локальный PostgreSQL:
```bash
psql -U postgres -d ruda_plus_db
```

Убедитесь, что таблицы модуля 1 на месте:

```sql
SET search_path TO ruda_plus, public;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'ruda_plus'
ORDER BY table_name;
```

Ожидаемый результат: `downtime_events`, `equipment`, `ore_production`, `sensor_readings`.

### Шаг 3.2. Создание справочных таблиц

Выполните скрипт `scripts/01_create_reference_tables.sql`.

Этот скрипт создаёт 3 новые таблицы:
- `mines` — справочник шахт
- `equipment_types` — типы оборудования
- `operators` — операторы

Проверьте результат:

```sql
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c
        WHERE c.table_schema = 'ruda_plus' AND c.table_name = t.table_name) AS columns_count
FROM information_schema.tables t
WHERE table_schema = 'ruda_plus'
ORDER BY table_name;
```

Ожидаемый результат: 7 таблиц (4 старые + 3 новые).

### Шаг 3.3. Загрузка справочных данных

Выполните скрипт `scripts/02_load_reference_data.sql`.

Проверьте данные:

```sql
-- Шахты
SELECT * FROM ruda_plus.mines ORDER BY mine_id;

-- Типы оборудования
SELECT * FROM ruda_plus.equipment_types ORDER BY type_id;

-- Операторы
SELECT operator_id, last_name, first_name, position, qualification
FROM ruda_plus.operators
ORDER BY operator_id;
```

### Шаг 3.4. Обновление существующих таблиц

Теперь самый важный шаг — **обновление связей** существующих таблиц. Выполните скрипт `scripts/03_alter_existing_tables.sql`.

Этот скрипт:
1. Добавляет столбец `type_id` в таблицу `equipment` и заполняет его из справочника
2. Добавляет столбец `operator_id` в таблицу `ore_production` и связывает с операторами
3. Добавляет столбец `reported_by_id` в таблицу `downtime_events`
4. Создаёт внешние ключи для связей с новыми справочниками
5. Добавляет `horizon_id` в таблицу `ore_production`

> **Важно:** Изучите скрипт перед выполнением! Обратите внимание, как данные мигрируются: сначала добавляется столбец, затем заполняется, затем создаётся FK.

Проверьте обновлённую структуру:

```sql
-- Проверим, что FK созданы
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table,
    ccu.column_name AS foreign_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'ruda_plus'
ORDER BY tc.table_name;
```

### Шаг 3.5. Сравните «до» и «после»

| Аспект | Модуль 1 (до) | Модуль 2 (после) |
|---|---|---|
| Таблицы | 4 | 7 |
| Справочники | 0 | 3 (mines, equipment_types, operators) |
| FK-связи | 3 | 9+ |
| mine_name в equipment | Текстовое поле (дублирование) | FK на справочник mines |
| operator_name в ore_production | Текстовое поле | FK на справочник operators |
| Тип оборудования | Текстовое поле | FK на справочник equipment_types |

---

## Часть 4. Аналитические запросы (20 минут)

Выполните скрипт `scripts/04_analytical_queries.sql` и ответьте на вопросы.

### Запрос 4.1. Оборудование по шахтам

```sql
-- Сколько оборудования каждого типа в каждой шахте?
SELECT m.mine_name,
       et.type_name,
       COUNT(*) AS equipment_count
FROM ruda_plus.equipment e
JOIN ruda_plus.mines m ON e.mine_id = m.mine_id
JOIN ruda_plus.equipment_types et ON e.type_id = et.type_id
GROUP BY m.mine_name, et.type_name
ORDER BY m.mine_name, equipment_count DESC;
```

**Вопрос:** В какой шахте больше всего ПДМ?

### Запрос 4.2. Производительность операторов

```sql
-- Средняя добыча по операторам
SELECT o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator,
       o.position,
       o.qualification,
       COUNT(*) AS shifts_count,
       ROUND(AVG(p.tonnage_extracted), 1) AS avg_tonnage,
       ROUND(SUM(p.tonnage_extracted), 1) AS total_tonnage
FROM ruda_plus.ore_production p
JOIN ruda_plus.operators o ON p.operator_id = o.operator_id
WHERE p.status = 'Завершена'
GROUP BY o.operator_id, o.last_name, o.first_name, o.position, o.qualification
ORDER BY avg_tonnage DESC;
```

**Вопрос:** Есть ли корреляция между квалификацией оператора и средней добычей?

### Запрос 4.3. Простои по шахтам и типам

```sql
-- Анализ простоев с учётом справочников
SELECT m.mine_name,
       et.type_name AS equipment_type,
       d.event_type,
       COUNT(*) AS events_count,
       SUM(d.duration_minutes) AS total_minutes,
       ROUND(AVG(d.duration_minutes), 0) AS avg_minutes
FROM ruda_plus.downtime_events d
JOIN ruda_plus.equipment e ON d.equipment_id = e.equipment_id
JOIN ruda_plus.mines m ON e.mine_id = m.mine_id
JOIN ruda_plus.equipment_types et ON e.type_id = et.type_id
GROUP BY m.mine_name, et.type_name, d.event_type
ORDER BY total_minutes DESC;
```

**Вопрос:** Какой тип оборудования в какой шахте теряет больше всего времени на простоях?

### Запрос 4.4. Комплексный отчёт — эффективность шахты

```sql
-- Сводный отчёт по эффективности шахт
SELECT m.mine_name,
       m.region,
       (SELECT COUNT(*) FROM ruda_plus.equipment e WHERE e.mine_id = m.mine_id) AS total_equipment,
       (SELECT COUNT(*) FROM ruda_plus.operators o WHERE o.mine_id = m.mine_id) AS total_operators,
       (SELECT ROUND(SUM(p.tonnage_extracted), 1)
        FROM ruda_plus.ore_production p WHERE p.mine_id = m.mine_id AND p.status = 'Завершена'
       ) AS total_tonnage,
       (SELECT ROUND(AVG(p.fe_content_pct), 2)
        FROM ruda_plus.ore_production p WHERE p.mine_id = m.mine_id AND p.status = 'Завершена'
       ) AS avg_fe_pct,
       (SELECT COALESCE(SUM(d.duration_minutes), 0)
        FROM ruda_plus.downtime_events d
        JOIN ruda_plus.equipment e ON d.equipment_id = e.equipment_id
        WHERE e.mine_id = m.mine_id AND d.event_type = 'Незапланированный'
       ) AS unplanned_downtime_min
FROM ruda_plus.mines m
WHERE m.status = 'Действующая'
ORDER BY total_tonnage DESC NULLS LAST;
```

**Вопрос:** Какая шахта самая эффективная по соотношению добычи и незапланированных простоев?

---

## Самостоятельное задание

### Задание А. Добавьте таблицу «Горизонты»

Используя файл `data/horizons.csv`, самостоятельно:
1. Создайте таблицу `horizons` в схеме `ruda_plus`
2. Загрузите данные (12 записей)
3. Добавьте FK `horizon_id` в таблицу `ore_production`

<details>
<summary>Подсказка: DDL</summary>

```sql
CREATE TABLE IF NOT EXISTS ruda_plus.horizons (
    horizon_id  VARCHAR(10)  PRIMARY KEY,
    mine_id     VARCHAR(10)  NOT NULL REFERENCES ruda_plus.mines(mine_id),
    level_name  VARCHAR(30)  NOT NULL,
    depth_m     INTEGER      NOT NULL,
    ore_body    VARCHAR(50),
    status      VARCHAR(20)  DEFAULT 'Активный',

    CONSTRAINT chk_horizon_status CHECK (status IN ('Активный', 'Законсервирован', 'Исчерпан'))
);

COMMENT ON TABLE ruda_plus.horizons IS 'Горизонты (уровни) добычи в шахтах';
```
</details>

### Задание Б. Напишите запрос

Напишите запрос, который показывает для каждого оператора:
- ФИО и квалификацию
- На каком оборудовании он работал
- Общую добычу
- Количество простоев оборудования, на котором он работал

<details>
<summary>Подсказка</summary>

```sql
SELECT o.last_name || ' ' || o.first_name AS operator_name,
       o.qualification,
       e.equipment_name,
       ROUND(SUM(p.tonnage_extracted), 1) AS total_tonnage,
       (SELECT COUNT(*) FROM ruda_plus.downtime_events d
        WHERE d.equipment_id = e.equipment_id) AS downtime_events
FROM ruda_plus.operators o
JOIN ruda_plus.ore_production p ON o.operator_id = p.operator_id
JOIN ruda_plus.equipment e ON p.equipment_id = e.equipment_id
WHERE p.status = 'Завершена'
GROUP BY o.operator_id, o.last_name, o.first_name, o.qualification,
         e.equipment_id, e.equipment_name
ORDER BY total_tonnage DESC;
```
</details>

### Задание В. Модель для видеорегистраторов

Предложите концептуальную модель для подсистемы видеорегистраторов:
- Какие сущности нужны?
- Как связать видеозаписи с оборудованием, временем, событиями?
- Где хранить сами видеофайлы (в БД или в Object Storage)?

---

## Итоговые вопросы для обсуждения

1. Чем отличается модель с 4 таблицами (модуль 1) от модели с 7 таблицами (модуль 2)? Какие преимущества даёт нормализация?
2. В каких случаях денормализация (хранение mine_name в equipment) допустима?
3. Почему мы используем суррогатные ключи (EQ-001, MINE-01) вместо автоинкремента?
4. Какие ограничения вы бы добавили к модели для обеспечения целостности данных?
5. Как изменится модель, если предприятие «Руда+» откроет карьер (открытую добычу)?
