# Практическая работа 3. OLTP, OLAP и Data Vault для MES «Руда+»

## Общая информация

| Параметр | Значение |
|---|---|
| **Модуль** | 3. Моделирование транзакционных и аналитических систем |
| **Темы** | 3.1–3.6. OLTP, OLAP, Кимбалл, Инмон, Data Vault, сравнение |
| **Длительность** | 120–150 минут |
| **Формат** | Индивидуальная работа / работа в парах |
| **Среда** | Yandex Cloud (Managed PostgreSQL) / локальный PostgreSQL |

## Цель работы

Построить три модели данных для одного предприятия «Руда+»:
1. **OLTP (3НФ)** — проверить и довести модель из модулей 1–2 до полной третьей нормальной формы
2. **Star Schema (Кимбалл)** — спроектировать аналитическое хранилище со схемой «Звезда»
3. **Data Vault 2.0** — реализовать модель Hub/Link/Satellite

Сравнить подходы на практике: количество JOIN, сложность запросов, гибкость.

## Предварительные требования

- Выполнены практические работы модулей 1 и 2 (схема `ruda_plus` с 7+ таблицами)
- Доступ к PostgreSQL (Yandex Cloud или локальный)
- Текстовый редактор или DBeaver
- Браузер для dbdiagram.io (опционально)

## Подготовленные материалы

```
practice/
├── README.md                              ← вы читаете этот файл
├── data/
│   ├── dim_time_sample.csv               ← пример данных измерения Время
│   ├── dim_downtime_category.csv         ← категории простоев
│   └── star_schema_model.dbml            ← DBML-модель Star Schema для dbdiagram.io
└── scripts/
    ├── 01_normalize_oltp.sql             ← проверка и доведение до 3НФ
    ├── 02_star_schema.sql                ← создание Star Schema (схема star)
    ├── 03_data_vault.sql                 ← создание Data Vault (схема vault)
    └── 04_analytical_queries.sql         ← аналитические запросы по всем моделям
```

---

## Часть 1. Нормализация OLTP-модели (30 минут)

### Шаг 1.1. Подключение к БД

Подключитесь к базе данных `ruda_plus_db`:

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

### Шаг 1.2. Проверьте текущее состояние

```sql
SET search_path TO ruda_plus, public;

-- Список таблиц
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'ruda_plus' ORDER BY table_name;
```

Ожидаемый результат: 7 таблиц (`downtime_events`, `equipment`, `equipment_types`, `mines`, `operators`, `ore_production`, `sensor_readings`).

### Шаг 1.3. Анализ на соответствие 1НФ и 2НФ

Откройте скрипт `scripts/01_normalize_oltp.sql` и выполняйте шаги 1–4.

**Контрольные вопросы:**
- Есть ли в модели составные/многозначные поля? (Нет → 1НФ выполняется)
- Есть ли составные первичные ключи? (Нет → 2НФ выполняется автоматически)

### Шаг 1.4. Анализ на соответствие 3НФ

Выполните шаг 4 скрипта. Вы обнаружите **транзитивные зависимости**:

| Таблица | Поле | Дублирует | Через FK |
|---|---|---|---|
| `equipment` | `mine_name` | `mines.mine_name` | `mine_id` |
| `equipment` | `equipment_type` | `equipment_types.type_name` | `type_id` |
| `ore_production` | `mine_name` | `mines.mine_name` | `mine_id` |
| `ore_production` | `operator_name` | `operators.last_name + first_name` | `operator_id` |
| `downtime_events` | `reported_by` | `operators.last_name + first_name` | `reported_by_id` |

### Шаг 1.5. Устранение нарушений 3НФ

Выполните шаг 5 скрипта — удаление дублирующих текстовых полей.

> **Важно:** Перед удалением полей убедитесь, что FK-столбцы (`mine_id`, `type_id`, `operator_id`, `reported_by_id`) заполнены корректно! Если они содержат NULL — сначала заполните их.

### Шаг 1.6. Проверка результата

Выполните шаг 6 скрипта. Теперь все текстовые дубли удалены, данные извлекаются только через JOIN:

```sql
-- Пример: получение оборудования с именем шахты (через JOIN, не через текстовое поле)
SELECT e.equipment_id, e.equipment_name,
       et.type_name, m.mine_name
FROM equipment e
JOIN equipment_types et ON e.type_id = et.type_id
JOIN mines m ON e.mine_id = m.mine_id;
```

<details>
<summary>Ожидаемый результат</summary>

Модель полностью в 3НФ:
- Каждый неключевой атрибут зависит только от PK
- Нет транзитивных зависимостей
- Текстовые поля, дублирующие справочники, удалены
- Все связи — через FK

</details>

---

## Часть 2. Star Schema — Кимбалл (40 минут)

### Шаг 2.1. Визуализация модели в dbdiagram.io (опционально)

1. Откройте [dbdiagram.io](https://dbdiagram.io/)
2. Скопируйте содержимое файла `data/star_schema_model.dbml`
3. Вставьте в редактор — диаграмма сгенерируется автоматически
4. Обратите внимание:
   - Таблица фактов (`fact_production`) в центре
   - Измерения вокруг, соединены через FK
   - Измерения **денормализованы** (`dim_equipment` содержит `type_name`, `mine_name`)

### Шаг 2.2. Создание Star Schema в PostgreSQL

Выполните скрипт `scripts/02_star_schema.sql`. Он создаёт:

| Таблица | Тип | Описание | Строк |
|---|---|---|---|
| `star.dim_time` | Измерение | Календарь 2025 года | 365 |
| `star.dim_mine` | Измерение | Шахты (SCD Type 2) | 4 |
| `star.dim_equipment` | Измерение | Оборудование (денормализовано) | 12 |
| `star.dim_operator` | Измерение | Операторы (SCD Type 2) | 10 |
| `star.dim_downtime_category` | Измерение | Категории простоев (Junk) | ~8 |
| `star.fact_production` | Факт | Добыча руды | ~15 |
| `star.fact_downtime` | Факт | Простои | ~10 |

### Шаг 2.3. Изучите структуру измерений

Обратите внимание на ключевые особенности:

**Суррогатные ключи:**
```sql
-- dim_mine.mine_key (SERIAL) вместо mine_id (VARCHAR)
-- Зачем? Для SCD Type 2: один mine_id может иметь несколько строк
SELECT mine_key, mine_id, mine_name, effective_from, effective_to, is_current
FROM star.dim_mine;
```

**Денормализация:**
```sql
-- dim_equipment содержит type_name и mine_name из других OLTP-таблиц
SELECT equipment_key, equipment_name, type_name, type_code, mine_name, mine_region
FROM star.dim_equipment;
-- В OLTP это потребовало бы 2 JOIN!
```

**Junk Dimension:**
```sql
-- dim_downtime_category объединяет 3 малых атрибута в одну таблицу
SELECT * FROM star.dim_downtime_category;
```

### Шаг 2.4. Выполните аналитические запросы

Откройте `scripts/04_analytical_queries.sql`, часть Б. Выполните запросы Б.1–Б.5.

**Вопросы для размышления:**
1. Сколько JOIN потребовалось для отчёта «Добыча по шахтам»? (Сравните с OLTP)
2. Чем удобен `dim_time` для аналитики по месяцам?
3. Как кросс-витринный запрос Б.5 использует Conformed Dimensions?

---

## Часть 3. Data Vault 2.0 (30 минут)

### Шаг 3.1. Изучите структуру Data Vault

Перед выполнением скрипта изучите основные концепции:

| Компонент | Назначение | Пример «Руда+» |
|---|---|---|
| **Hub** | Бизнес-ключ + метаданные | `hub_equipment (equipment_id)` |
| **Satellite** | Описательные атрибуты + история | `sat_equipment_details (name, manufacturer...)` |
| **Link** | Связь между хабами | `link_equipment_mine (equipment ↔ mine)` |
| **Hash Key** | MD5 от бизнес-ключа (PK) | `MD5('EQ-001') → CHAR(32)` |

### Шаг 3.2. Создание Data Vault

Выполните скрипт `scripts/03_data_vault.sql`. Он создаёт:

**Hubs (3):**
- `vault.hub_mine` — бизнес-ключи шахт
- `vault.hub_equipment` — бизнес-ключи оборудования
- `vault.hub_operator` — бизнес-ключи операторов

**Satellites (5):**
- `vault.sat_mine_details` — атрибуты шахт
- `vault.sat_equipment_details` — характеристики оборудования
- `vault.sat_equipment_status` — статус и наработка (отдельный сателлит!)
- `vault.sat_operator_details` — данные операторов
- `vault.sat_production_metrics` — метрики добычи

**Links (3):**
- `vault.link_equipment_mine` — оборудование ↔ шахта
- `vault.link_operator_mine` — оператор ↔ шахта
- `vault.link_production` — транзакция добычи (mine + equipment + operator)

### Шаг 3.3. Изучите хеш-ключи

```sql
-- Hub содержит только бизнес-ключ и метаданные
SELECT hub_equipment_hk, equipment_id, load_dts, record_source
FROM vault.hub_equipment
LIMIT 5;

-- hash_diff в Satellite позволяет отслеживать изменения
SELECT hub_equipment_hk, load_dts, equipment_name, manufacturer, hash_diff
FROM vault.sat_equipment_details
LIMIT 5;
```

**Вопрос:** Зачем разделять `sat_equipment_details` и `sat_equipment_status`?

<details>
<summary>Ответ</summary>

Статус и наработка (`status`, `engine_hours`) меняются часто (каждый день).
Характеристики (`name`, `manufacturer`, `model`) — почти никогда.

Разделение позволяет:
- Не создавать новую версию в `sat_equipment_details` при каждом обновлении наработки
- Загружать сателлиты параллельно
- Уменьшить объём хранимой истории

</details>

### Шаг 3.4. Выполните запросы к Data Vault

Откройте `scripts/04_analytical_queries.sql`, часть В. Выполните запросы В.1–В.3.

Обратите внимание: для получения тех же данных, что в Star Schema, нужно значительно больше JOIN!

---

## Часть 4. Сравнение подходов (20 минут)

### Шаг 4.1. Сравните количество JOIN

Выполните часть Г скрипта `04_analytical_queries.sql`.

Заполните таблицу:

| Отчёт | OLTP (3НФ) | Star Schema | Data Vault |
|---|---|---|---|
| Добыча по шахтам | ? JOIN | ? JOIN | ? JOIN |
| Простои по оборудованию | ? JOIN | ? JOIN | — |
| Количество таблиц | ? | ? | ? |

### Шаг 4.2. Сравните сложность SQL

Один и тот же отчёт «Добыча по шахтам с именами операторов»:

- **OLTP:** `SELECT ... FROM ore_production JOIN mines JOIN equipment JOIN equipment_types JOIN operators` — 4 JOIN, но таблицы маленькие и нормализованные
- **Star:** `SELECT ... FROM fact_production JOIN dim_mine JOIN dim_operator` — 2 JOIN, измерения уже содержат все атрибуты
- **Data Vault:** `SELECT ... FROM link_production JOIN hub_mine JOIN sat_mine_details JOIN hub_equipment JOIN sat_equipment_details JOIN hub_operator JOIN sat_operator_details JOIN sat_production_metrics` — 7 JOIN

### Шаг 4.3. Заполните итоговую сравнительную таблицу

| Критерий | OLTP (3НФ) | Star Schema | Data Vault |
|---|---|---|---|
| Целевое назначение | ? | ? | ? |
| Количество таблиц | ? | ? | ? |
| Сложность запросов | ? | ? | ? |
| Избыточность данных | ? | ? | ? |
| Историчность | ? | ? | ? |
| Аудит/трассировка | ? | ? | ? |
| Подходит для BI? | ? | ? | ? |

<details>
<summary>Ожидаемые ответы</summary>

| Критерий | OLTP (3НФ) | Star Schema | Data Vault |
|---|---|---|---|
| Целевое назначение | Оперативная обработка | Аналитика и BI | Интеграция данных |
| Количество таблиц | 7–8 | 7 (5 dim + 2 fact) | 11 (3 hub + 5 sat + 3 link) |
| Сложность запросов | Средняя (3–4 JOIN) | Низкая (1–2 JOIN) | Высокая (5–7 JOIN) |
| Избыточность данных | Минимальная | Есть (денормализация) | Минимальная |
| Историчность | Нет (только текущие) | SCD Type 2 | Полная (по загрузкам) |
| Аудит/трассировка | Нет | Нет | Полная (record_source) |
| Подходит для BI? | Условно | Идеально | Через витрины |

</details>

---

## Самостоятельные задания

### Задание А. Добавьте таблицу фактов для телеметрии

Спроектируйте `star.fact_sensor_reading` для анализа показаний датчиков:
1. Какие измерения нужны? (время, оборудование, ...)
2. Какие метрики?
3. Какое зерно? (одно показание? агрегат за минуту? за час?)
4. Оцените объём: 1000 показаний/сек × 86400 сек = 86 млн строк/сутки

<details>
<summary>Подсказка</summary>

```sql
CREATE TABLE star.fact_sensor_agg_hourly (
    sensor_agg_key   SERIAL PRIMARY KEY,
    time_key         INTEGER REFERENCES star.dim_time(time_key),
    equipment_key    INTEGER REFERENCES star.dim_equipment(equipment_key),
    hour_of_day      INTEGER NOT NULL,      -- 0–23
    sensor_type      VARCHAR(30) NOT NULL,
    -- Метрики (агрегаты за час):
    avg_value        NUMERIC(10,2),
    min_value        NUMERIC(10,2),
    max_value        NUMERIC(10,2),
    reading_count    INTEGER,
    alarm_count      INTEGER DEFAULT 0
);
-- Зерно: 1 час × 1 оборудование × 1 тип датчика
-- ~12 единиц × 5 типов × 24 часа = 1440 строк/сутки (вместо 86 млн!)
```

</details>

### Задание Б. SCD Type 2: смоделируйте изменение

Оператор Волков Ж.Ж. (OP-007) повысил квалификацию с «4 разряд» до «5 разряд» с 01.07.2025. Обновите:
1. `star.dim_operator` — закройте старую версию, добавьте новую
2. `vault.sat_operator_details` — добавьте новый сателлит

<details>
<summary>Подсказка: Star Schema</summary>

```sql
-- 1. Закрываем текущую версию
UPDATE star.dim_operator
SET effective_to = '2025-06-30', is_current = FALSE
WHERE operator_id = 'OP-007' AND is_current = TRUE;

-- 2. Добавляем новую версию
INSERT INTO star.dim_operator (operator_id, full_name, last_name, first_name,
    position, qualification, mine_name, effective_from)
SELECT 'OP-007', full_name, last_name, first_name,
       position, '5 разряд', mine_name, '2025-07-01'
FROM star.dim_operator
WHERE operator_id = 'OP-007' AND effective_to = '2025-06-30';
```

</details>

### Задание В. Data Vault: добавьте новый источник

На предприятии «Руда+» появился новый источник данных — ERP-система, которая содержит стоимость ТО оборудования. Как добавить эту информацию в Data Vault?

<details>
<summary>Подсказка</summary>

Нужно создать новый сателлит `vault.sat_equipment_maintenance_cost` на `hub_equipment`:

```sql
CREATE TABLE vault.sat_equipment_maintenance_cost (
    hub_equipment_hk CHAR(32) REFERENCES vault.hub_equipment,
    load_dts         TIMESTAMP NOT NULL,
    load_end_dts     TIMESTAMP DEFAULT '9999-12-31',
    maintenance_cost NUMERIC(12,2),
    cost_currency    VARCHAR(3) DEFAULT 'RUB',
    cost_period_from DATE,
    cost_period_to   DATE,
    hash_diff        CHAR(32),
    record_source    VARCHAR(50) DEFAULT 'erp_system',  -- новый источник!
    PRIMARY KEY (hub_equipment_hk, load_dts)
);
-- Ни одна существующая таблица не изменилась!
```

</details>

---

## Итоговые вопросы для обсуждения

1. Какую модель вы бы выбрали для MES-системы «Руда+» на первом этапе? Почему?
2. Как организовать ETL-процесс для переноса данных из OLTP (ruda_plus) в Star Schema (star)?
3. Почему Data Vault разделяет описательные атрибуты (Satellite) от бизнес-ключей (Hub)?
4. Телеметрия: 1000 показаний/сек. Какая модель лучше подходит для хранения? Для анализа?
5. Если «Руда+» вырастет до 10 шахт и 2000 сотрудников, нужно ли менять архитектуру?
