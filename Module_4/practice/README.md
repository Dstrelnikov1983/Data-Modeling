# Практическая работа 4. Моделирование потоковых и пакетных данных для MES "Руда+"

## Общая информация

| Параметр | Значение |
|---|---|
| **Модуль** | 4. Моделирование потоковых и пакетных данных |
| **Темы** | ETL-пайплайны, инкрементальная загрузка, потоковая обработка, сравнение подходов |
| **Длительность** | 120 минут |
| **Формат** | Индивидуальная работа / работа в парах |
| **Среда** | Yandex Cloud (Managed PostgreSQL) / локальный PostgreSQL |

## Цель работы

Построить полный цикл обработки данных для предприятия "Руда+":

1. **ETL-пайплайн** — спроектировать и реализовать конвейер загрузки данных из OLTP в аналитическое хранилище (Star Schema)
2. **Инкрементальная загрузка** — реализовать механизм отслеживания изменений и частичной загрузки с поддержкой SCD Type 2
3. **Потоковая обработка** — смоделировать обработку потоковых данных телеметрии шахтного оборудования
4. **Сравнение подходов** — сопоставить пакетную и потоковую обработку на одних и тех же данных

## Предварительные требования

- Выполнены практические работы модулей 1, 2 и 3
- Существуют схемы:
  - `ruda_plus` — OLTP (таблицы: `equipment`, `equipment_types`, `mines`, `horizons`, `operators`, `ore_production`, `sensor_readings`, `downtime_events`)
  - `star` — Star Schema (таблицы: `dim_time`, `dim_mine`, `dim_equipment`, `dim_operator`, `dim_downtime_category`, `fact_production`, `fact_downtime`)
  - `vault` — Data Vault 2.0
- Доступ к PostgreSQL (Yandex Cloud или локальный)
- Текстовый редактор или DBeaver
- Базовое понимание ETL-процессов

## Подготовленные материалы

```
Module_4/practice/
├── README.md                                  ← вы читаете этот файл
├── data/
│   ├── sensor_events_sample.csv              ← тестовые потоковые события датчиков
│   └── etl_config.json                       ← пример конфигурации ETL
└── scripts/
    ├── 01_staging_schema.sql                 ← создание staging-схемы и таблиц управления ETL
    ├── 02_etl_full_load.sql                  ← полная загрузка (Full Load) из OLTP → Staging → Star
    ├── 03_etl_incremental.sql                ← инкрементальная загрузка с SCD Type 2
    └── 04_stream_processing.sql              ← моделирование потоковой обработки
```

---

## Часть 1. Проектирование ETL-пайплайна (40 минут)

### Концепция

ETL-пайплайн "Руда+" реализует классическую трёхступенчатую архитектуру:

```
┌─────────────┐     Extract      ┌─────────────┐     Transform     ┌─────────────┐
│   OLTP      │ ───────────────→ │   Staging    │ ───────────────→ │ Star Schema │
│ (ruda_plus) │                  │  (staging)   │                  │   (star)    │
└─────────────┘                  └─────────────┘                  └─────────────┘
   Источник              Промежуточная зона             Аналитическое хранилище
```

**Зачем нужна промежуточная зона (Staging)?**
- Разделение этапов: извлечение и трансформация не зависят друг от друга
- Возможность повторной трансформации без повторного извлечения
- Логирование и аудит: каждая загрузка получает уникальный идентификатор
- Проверки качества данных перед загрузкой в хранилище

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

### Шаг 1.2. Создание Staging-схемы и таблиц управления ETL

Выполните скрипт `scripts/01_staging_schema.sql`. Он создаст:

| Объект | Тип | Описание |
|---|---|---|
| `staging` | Схема | Промежуточная зона для данных |
| `staging.stg_mines` | Таблица | Копия шахт с метаданными ETL |
| `staging.stg_equipment` | Таблица | Копия оборудования с метаданными ETL |
| `staging.stg_equipment_types` | Таблица | Копия типов оборудования с метаданными ETL |
| `staging.stg_operators` | Таблица | Копия операторов с метаданными ETL |
| `staging.stg_ore_production` | Таблица | Копия добычи с метаданными ETL |
| `staging.stg_downtime_events` | Таблица | Копия простоев с метаданными ETL |
| `staging.stg_sensor_readings` | Таблица | Копия показаний датчиков с метаданными ETL |
| `staging.etl_load_log` | Таблица | Журнал загрузок |
| `staging.etl_watermark` | Таблица | Водяные знаки для инкрементальной загрузки |

```sql
-- Проверьте, что staging-схема создана
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'staging'
ORDER BY table_name;
```

<details>
<summary>Ожидаемый результат</summary>

```
      table_name
-----------------------
 etl_load_log
 etl_watermark
 stg_downtime_events
 stg_equipment
 stg_equipment_types
 stg_mines
 stg_operators
 stg_ore_production
 stg_sensor_readings
(9 rows)
```

</details>

**Обратите внимание** на дополнительные столбцы в staging-таблицах:

```sql
-- Метаданные ETL в каждой staging-таблице
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = 'stg_equipment'
ORDER BY ordinal_position;
```

Каждая staging-таблица содержит:
- `_load_id` — ссылка на запись в `etl_load_log`
- `_load_timestamp` — время загрузки
- `_source_system` — имя источника
- `_row_hash` — MD5-хеш строки для обнаружения изменений

### Шаг 1.3. Полная загрузка (Full Load) из OLTP в Staging

Выполните скрипт `scripts/02_etl_full_load.sql`. Скрипт выполняет:

1. **Регистрация загрузки** — создание записи в `etl_load_log`
2. **Очистка staging** — TRUNCATE старых данных (Truncate-and-Load паттерн)
3. **Извлечение** — копирование данных из `ruda_plus` в `staging`
4. **Хеширование** — вычисление MD5-хеша для каждой строки
5. **Проверки качества** — валидация данных в staging
6. **Загрузка в Star Schema** — трансформация и загрузка в измерения и факты
7. **Обновление журнала** — фиксация результатов загрузки

```sql
-- После выполнения скрипта проверьте журнал загрузок
SELECT load_id, table_name, load_type, status,
       rows_extracted, rows_loaded,
       started_at, finished_at,
       finished_at - started_at AS duration
FROM staging.etl_load_log
ORDER BY load_id;
```

<details>
<summary>Ожидаемый результат</summary>

```
 load_id |     table_name     | load_type | status    | rows_extracted | rows_loaded
---------+--------------------+-----------+-----------+----------------+-------------
       1 | mines              | full      | completed |              4 |           4
       2 | equipment_types    | full      | completed |              4 |           4
       3 | equipment          | full      | completed |             12 |          12
       4 | operators          | full      | completed |             10 |          10
       5 | ore_production     | full      | completed |             15 |          15
       6 | downtime_events    | full      | completed |             10 |          10
       7 | sensor_readings    | full      | completed |             50 |          50
```

</details>

### Шаг 1.4. Проверки качества данных в Staging

Скрипт `02_etl_full_load.sql` включает проверки. Выполните их вручную для понимания:

```sql
-- Проверка 1: NULL в обязательных полях
SELECT 'stg_equipment' AS table_name, COUNT(*) AS null_count
FROM staging.stg_equipment
WHERE equipment_id IS NULL OR equipment_name IS NULL

UNION ALL

SELECT 'stg_ore_production', COUNT(*)
FROM staging.stg_ore_production
WHERE production_id IS NULL OR mine_id IS NULL;

-- Проверка 2: Дубликаты по бизнес-ключам
SELECT equipment_id, COUNT(*) AS cnt
FROM staging.stg_equipment
GROUP BY equipment_id
HAVING COUNT(*) > 1;

-- Проверка 3: Ссылочная целостность (FK в staging)
SELECT p.production_id, p.equipment_id
FROM staging.stg_ore_production p
LEFT JOIN staging.stg_equipment e ON p.equipment_id = e.equipment_id
WHERE e.equipment_id IS NULL;
```

<details>
<summary>Ожидаемый результат</summary>

Все проверки должны вернуть 0 строк. Если есть нарушения, данные не должны загружаться в Star Schema до устранения проблем.

</details>

> **Вопрос для обсуждения:** Что произойдёт, если проверка качества выявит ошибку? Должен ли ETL остановиться целиком или продолжить загрузку "чистых" данных?

### Шаг 1.5. Трансформация и загрузка в Star Schema

Скрипт `02_etl_full_load.sql` также выполняет загрузку из staging в star. Проверьте результат:

```sql
-- Сравните количество записей
SELECT 'staging.stg_ore_production' AS source, COUNT(*) FROM staging.stg_ore_production
UNION ALL
SELECT 'star.fact_production', COUNT(*) FROM star.fact_production;

SELECT 'staging.stg_downtime_events' AS source, COUNT(*) FROM staging.stg_downtime_events
UNION ALL
SELECT 'star.fact_downtime', COUNT(*) FROM star.fact_downtime;
```

<details>
<summary>Ожидаемый результат</summary>

Количество записей в star-схеме может отличаться от staging, если:
- Не все записи в `ore_production` имеют статус "Завершена"
- Некоторые строки не прошли проверку ссылочной целостности

Это **нормальное поведение** — не все данные из OLTP подлежат аналитике.

</details>

---

## Часть 2. Инкрементальная загрузка (30 минут)

### Концепция

Полная загрузка (Full Load) неэффективна при больших объёмах данных. Для MES-системы "Руда+" ежедневно поступают:
- ~100 записей о добыче
- ~50 событий простоя
- ~86 400 000 показаний телеметрии (1000/сек)

**Инкрементальная загрузка** извлекает только данные, изменившиеся с момента последней загрузки.

```
┌─────────────┐  WHERE updated_at >  ┌─────────────┐     Merge      ┌─────────────┐
│   OLTP      │  last_watermark      │   Staging    │ ────────────→ │ Star Schema │
│ (ruda_plus) │ ───────────────────→ │  (staging)   │  SCD Type 2   │   (star)    │
└─────────────┘                      └─────────────┘               └─────────────┘
                   Δ (дельта)
```

### Шаг 2.1. Изучите таблицы метаданных ETL

Таблицы создаются скриптом `01_staging_schema.sql`. Проверьте их:

```sql
-- Журнал загрузок
SELECT * FROM staging.etl_load_log ORDER BY load_id DESC LIMIT 5;

-- Водяные знаки (watermarks) — хранят точку последней загрузки
SELECT * FROM staging.etl_watermark;
```

**Водяной знак (watermark)** — это метка, указывающая, до какого момента данные уже загружены:

| table_name | last_loaded_at | last_loaded_id |
|---|---|---|
| ore_production | 2025-03-15 18:00:00 | PRD-015 |
| downtime_events | 2025-03-15 18:00:00 | EVT-010 |

### Шаг 2.2. Инкрементальное извлечение

Выполните скрипт `scripts/03_etl_incremental.sql`. Он демонстрирует:

**Шаг А. Имитация новых данных в источнике:**

Скрипт добавляет в `ruda_plus.ore_production` 3 новые записи и обновляет квалификацию оператора OP-007 — это имитирует работу MES-системы между двумя загрузками.

```sql
-- Проверьте новые данные в источнике
SELECT production_id, production_date, mine_id, tonnage_extracted, created_at
FROM ruda_plus.ore_production
ORDER BY created_at DESC
LIMIT 5;
```

**Шаг Б. Извлечение только изменений (дельта):**

```sql
-- Принцип: берём только записи новее водяного знака
SELECT *
FROM ruda_plus.ore_production
WHERE created_at > (
    SELECT last_loaded_at
    FROM staging.etl_watermark
    WHERE table_name = 'ore_production'
);
```

### Шаг 2.3. SCD Type 2: обновление измерений

При изменении атрибутов оператора (например, повышение квалификации), Star Schema должна сохранить историю:

```sql
-- До обновления: 1 запись для OP-007
SELECT operator_key, operator_id, full_name, qualification,
       effective_from, effective_to, is_current
FROM star.dim_operator
WHERE operator_id = 'OP-007';
```

Скрипт `03_etl_incremental.sql` выполняет SCD Type 2 обновление:

1. **Закрывает** текущую версию (устанавливает `effective_to` и `is_current = FALSE`)
2. **Создаёт** новую версию с обновлёнными данными

```sql
-- После обновления: 2 записи для OP-007
SELECT operator_key, operator_id, full_name, qualification,
       effective_from, effective_to, is_current
FROM star.dim_operator
WHERE operator_id = 'OP-007'
ORDER BY effective_from;
```

<details>
<summary>Ожидаемый результат</summary>

```
 operator_key | operator_id |    full_name     | qualification | effective_from | effective_to | is_current
--------------+-------------+------------------+---------------+----------------+--------------+------------
           7  | OP-007      | Волков Ж.Ж.      | 4 разряд      | 2025-01-01     | 2025-07-01   | false
          11  | OP-007      | Волков Ж.Ж.      | 5 разряд      | 2025-07-01     | 9999-12-31   | true
```

Теперь аналитические запросы за период до июля 2025 будут использовать старую квалификацию, а после — новую. Это и есть **историчность измерений**.

</details>

> **Вопрос для обсуждения:** В каких случаях SCD Type 2 не подходит? Когда лучше использовать SCD Type 1 (перезапись) или SCD Type 3 (добавление столбца)?

### Шаг 2.4. Инкрементальная загрузка фактов

Скрипт `03_etl_incremental.sql` загружает только новые факты:

```sql
-- Проверяем: новые записи добычи добавлены в star.fact_production
SELECT fp.production_id, fp.tonnage_extracted, fp.fe_content_pct,
       dt.production_date, dm.mine_name, de.equipment_name
FROM star.fact_production fp
JOIN star.dim_time dt ON fp.time_key = dt.time_key
JOIN star.dim_mine dm ON fp.mine_key = dm.mine_key
JOIN star.dim_equipment de ON fp.equipment_key = de.equipment_key
ORDER BY dt.production_date DESC
LIMIT 5;
```

### Шаг 2.5. Проверка водяных знаков

```sql
-- Водяные знаки обновлены
SELECT table_name, last_loaded_at, last_loaded_id
FROM staging.etl_watermark
ORDER BY table_name;
```

<details>
<summary>Ожидаемый результат</summary>

Значения `last_loaded_at` и `last_loaded_id` должны соответствовать последним добавленным записям. При следующей инкрементальной загрузке будут извлечены только записи новее этих значений.

</details>

---

## Часть 3. Моделирование потоковой обработки (30 минут)

### Концепция

В шахтах "Руда+" оборудование оснащено датчиками, которые генерируют данные непрерывно:

```
┌──────────┐    ┌──────────┐    ┌──────────┐
│ Датчик   │    │ Датчик   │    │ Датчик   │
│ темпер.  │    │ вибрации │    │ давления │     ... × 12 единиц оборудования
└────┬─────┘    └────┬─────┘    └────┬─────┘         × 5 типов датчиков
     │               │               │               = ~60 потоков
     └───────────────┼───────────────┘
                     ▼
          ┌─────────────────────┐
          │  Landing Zone       │     ← "Посадочная зона" для сырых событий
          │  (raw_sensor_events)│
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  Оконные агрегации  │     ← Tumbling Windows (5 мин, 1 час)
          │  Moving Averages    │
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  Правила алертов    │     ← Пороговые значения
          │  Обнаружение        │        Аномалии
          │  аномалий           │
          └─────────────────────┘
```

> **Важно:** PostgreSQL не является потоковой системой (как Apache Kafka + Flink). Мы **моделируем** потоковую обработку средствами SQL, чтобы понять концепции: event-driven архитектура, оконные функции, агрегации в реальном времени.

### Шаг 3.1. Создание схемы потоковой обработки

Выполните скрипт `scripts/04_stream_processing.sql`. Он создаёт:

| Объект | Описание |
|---|---|
| `streaming` (схема) | Схема для потоковых данных |
| `streaming.raw_sensor_events` | Посадочная зона: события датчиков |
| `streaming.raw_equipment_events` | Посадочная зона: события оборудования |
| `streaming.raw_navigation_events` | Посадочная зона: события навигации |
| `streaming.sensor_alerts` | Таблица алертов |
| `streaming.window_aggregations` | Материализованные оконные агрегации |

### Шаг 3.2. Изучите схемы событий

```sql
-- Структура события датчика
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'streaming' AND table_name = 'raw_sensor_events'
ORDER BY ordinal_position;
```

Обратите внимание:
- `event_timestamp` с типом `TIMESTAMP(3)` — миллисекундная точность
- `payload` с типом `JSONB` — гибкая структура для расширения
- `quality_flag` — индикатор качества данных датчика

### Шаг 3.3. Имитация потоковых событий

Скрипт `04_stream_processing.sql` вставляет ~200 событий, имитируя 2-часовую работу датчиков:

```sql
-- Проверьте загруженные события
SELECT event_type, COUNT(*) AS event_count,
       MIN(event_timestamp) AS first_event,
       MAX(event_timestamp) AS last_event
FROM streaming.raw_sensor_events
GROUP BY event_type
ORDER BY event_count DESC;
```

<details>
<summary>Ожидаемый результат</summary>

```
 event_type  | event_count |       first_event        |        last_event
-------------+-------------+--------------------------+--------------------------
 temperature |          ~40 | 2025-03-15 08:00:01.123 | 2025-03-15 09:58:45.789
 vibration   |          ~40 | 2025-03-15 08:00:01.456 | 2025-03-15 09:59:12.345
 pressure    |          ~40 | 2025-03-15 08:00:02.789 | 2025-03-15 09:57:33.567
 speed       |          ~40 | 2025-03-15 08:00:03.012 | 2025-03-15 09:58:01.234
 fuel_level  |          ~40 | 2025-03-15 08:00:05.678 | 2025-03-15 09:56:22.890
```

</details>

Также загрузите тестовые данные из CSV:

```sql
-- Если вы используете psql, можно загрузить данные из CSV:
-- \COPY streaming.raw_sensor_events FROM 'data/sensor_events_sample.csv' WITH (FORMAT csv, HEADER true);
```

### Шаг 3.4. Оконные агрегации (Tumbling Windows)

**Tumbling Window (кувыркающееся окно)** — неперекрывающиеся окна фиксированной ширины.

Скрипт реализует 5-минутные и 1-часовые окна:

```sql
-- Пример: 5-минутное окно для средней температуры по каждому оборудованию
SELECT
    equipment_id,
    -- Начало 5-минутного окна
    date_trunc('hour', event_timestamp)
        + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5)
        AS window_start,
    -- Конец окна
    date_trunc('hour', event_timestamp)
        + INTERVAL '5 min' * (FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5) + 1)
        AS window_end,
    COUNT(*) AS reading_count,
    ROUND(AVG(sensor_value), 2) AS avg_value,
    ROUND(MIN(sensor_value), 2) AS min_value,
    ROUND(MAX(sensor_value), 2) AS max_value,
    ROUND(STDDEV(sensor_value), 2) AS stddev_value
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
GROUP BY equipment_id,
         date_trunc('hour', event_timestamp)
             + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_timestamp) / 5)
ORDER BY equipment_id, window_start;
```

<details>
<summary>Ожидаемый результат</summary>

```
 equipment_id |     window_start        |      window_end         | reading_count | avg_value | min_value | max_value | stddev_value
--------------+-------------------------+-------------------------+---------------+-----------+-----------+-----------+--------------
 EQ-001       | 2025-03-15 08:00:00     | 2025-03-15 08:05:00     |             3 |     72.50 |     71.20 |     73.80 |         1.30
 EQ-001       | 2025-03-15 08:05:00     | 2025-03-15 08:10:00     |             2 |     74.10 |     73.50 |     74.70 |         0.85
 ...
```

</details>

> **Вопрос для обсуждения:** Почему Tumbling Window предпочтительнее Sliding Window для мониторинга оборудования? В каких случаях нужен Sliding Window?

### Шаг 3.5. Скользящие средние и обнаружение аномалий

```sql
-- Скользящее среднее с окном 10 измерений
SELECT
    event_id,
    event_timestamp,
    equipment_id,
    sensor_value,
    ROUND(AVG(sensor_value) OVER (
        PARTITION BY equipment_id
        ORDER BY event_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_10,
    -- Отклонение от скользящего среднего
    ROUND(sensor_value - AVG(sensor_value) OVER (
        PARTITION BY equipment_id
        ORDER BY event_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 2) AS deviation
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
  AND equipment_id = 'EQ-001'
ORDER BY event_timestamp;
```

**Обнаружение аномалий** — значения, отклоняющиеся более чем на 2 стандартных отклонения:

```sql
-- Аномальные показания (Z-score > 2)
WITH stats AS (
    SELECT equipment_id, event_type,
           AVG(sensor_value) AS mean_val,
           STDDEV(sensor_value) AS stddev_val
    FROM streaming.raw_sensor_events
    WHERE quality_flag = 'good'
    GROUP BY equipment_id, event_type
)
SELECT e.event_id, e.event_timestamp, e.equipment_id, e.event_type,
       e.sensor_value,
       ROUND(s.mean_val, 2) AS mean_val,
       ROUND(ABS(e.sensor_value - s.mean_val) / NULLIF(s.stddev_val, 0), 2) AS z_score
FROM streaming.raw_sensor_events e
JOIN stats s ON e.equipment_id = s.equipment_id AND e.event_type = s.event_type
WHERE ABS(e.sensor_value - s.mean_val) / NULLIF(s.stddev_val, 0) > 2
ORDER BY e.event_timestamp;
```

### Шаг 3.6. Правила алертов в реальном времени

Скрипт `04_stream_processing.sql` создаёт правила для генерации алертов:

| Датчик | Предупреждение (warning) | Критический (critical) |
|---|---|---|
| Температура | > 85 C | > 95 C |
| Вибрация | > 4.5 мм/с | > 7.0 мм/с |
| Давление | < 2.0 бар или > 8.0 бар | < 1.0 бар или > 10.0 бар |
| Скорость | > 12.0 км/ч | > 15.0 км/ч (подземная) |
| Уровень топлива | < 15% | < 5% |

```sql
-- Проверьте сгенерированные алерты
SELECT alert_level, event_type, COUNT(*) AS alert_count
FROM streaming.sensor_alerts
GROUP BY alert_level, event_type
ORDER BY alert_level, event_type;
```

<details>
<summary>Ожидаемый результат</summary>

```
 alert_level | event_type  | alert_count
-------------+-------------+-------------
 critical    | temperature |           2
 critical    | vibration   |           1
 warning     | fuel_level  |           3
 warning     | pressure    |           2
 warning     | temperature |           5
 warning     | vibration   |           3
```

</details>

---

## Часть 4. Сравнение пакетной и потоковой обработки (20 минут)

### Шаг 4.1. Анализ одних и тех же данных двумя подходами

**Задача:** Вычислить среднюю температуру оборудования EQ-001 за период 08:00-09:00.

**Пакетный подход (Batch):**
```sql
-- Вся агрегация выполняется за один раз по полному набору данных
SELECT equipment_id,
       ROUND(AVG(sensor_value), 2) AS avg_temp,
       COUNT(*) AS readings
FROM streaming.raw_sensor_events
WHERE event_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND event_timestamp >= '2025-03-15 08:00:00'
  AND event_timestamp < '2025-03-15 09:00:00'
GROUP BY equipment_id;
```

**Потоковый подход (Stream) — через оконные агрегации:**
```sql
-- Данные обрабатываются окнами по 5 минут
SELECT window_start, window_end,
       avg_value, reading_count
FROM streaming.window_aggregations
WHERE equipment_id = 'EQ-001'
  AND event_type = 'temperature'
  AND window_start >= '2025-03-15 08:00:00'
  AND window_end <= '2025-03-15 09:00:00'
ORDER BY window_start;
```

> **Вопрос для обсуждения:** Будут ли результаты одинаковыми? Почему среднее агрегированных окон может отличаться от среднего по всему набору?

### Шаг 4.2. Заполните сравнительную таблицу

| Критерий | Пакетная обработка (Batch) | Потоковая обработка (Stream) |
|---|---|---|
| Задержка (Latency) | ? | ? |
| Полнота данных | ? | ? |
| Точность агрегации | ? | ? |
| Обработка опозданий | ? | ? |
| Сложность реализации | ? | ? |
| Ресурсоёмкость | ? | ? |
| Подходит для... | ? | ? |
| Пример для "Руда+" | ? | ? |

<details>
<summary>Ожидаемые ответы</summary>

| Критерий | Пакетная обработка (Batch) | Потоковая обработка (Stream) |
|---|---|---|
| Задержка (Latency) | Минуты — часы (расписание) | Секунды — миллисекунды |
| Полнота данных | Полный набор на момент запуска | Данные приходят непрерывно |
| Точность агрегации | Высокая (все данные доступны) | Приближённая (окна, watermarks) |
| Обработка опозданий | Повторный запуск ETL | Watermarks, Side outputs |
| Сложность реализации | Средняя (SQL, ETL-инструменты) | Высокая (Kafka, Flink, специализация) |
| Ресурсоёмкость | Пиковая нагрузка при запуске | Постоянная, но умеренная |
| Подходит для... | Отчёты, KPI, BI-дашборды | Алерты, мониторинг, real-time |
| Пример для "Руда+" | Суточный отчёт о добыче | Мониторинг температуры двигателя |

</details>

### Шаг 4.3. Архитектура Lambda/Kappa для "Руда+"

На реальном предприятии используется **гибридная архитектура**:

```
                        ┌─────────────────────────────────┐
                        │        Датчики оборудования      │
                        └───────────────┬─────────────────┘
                                        │
                        ┌───────────────▼─────────────────┐
                        │         Message Broker           │
                        │     (Kafka / Yandex Data Streams)│
                        └───┬───────────────────────┬─────┘
                            │                       │
                   ┌────────▼────────┐    ┌─────────▼────────┐
                   │  Speed Layer    │    │  Batch Layer     │
                   │  (потоковый)    │    │  (пакетный)      │
                   │                 │    │                  │
                   │  Flink / KsqlDB │    │  ETL (наш SQL)  │
                   │  Алерты,        │    │  Star Schema,   │
                   │  мониторинг     │    │  Data Vault     │
                   └────────┬────────┘    └─────────┬────────┘
                            │                       │
                   ┌────────▼───────────────────────▼────────┐
                   │           Serving Layer                  │
                   │        (BI, Dashboards, API)             │
                   └─────────────────────────────────────────┘
```

> **Задание для групповой работы:** Обсудите в группах (3-4 человека), какие данные "Руда+" нужно обрабатывать потоково, а какие — пакетно. Заполните:

| Данные | Batch | Stream | Обоснование |
|---|---|---|---|
| Добыча руды (ore_production) | ? | ? | ? |
| Телеметрия датчиков (sensor_readings) | ? | ? | ? |
| Простои оборудования (downtime_events) | ? | ? | ? |
| Навигация машин (GPS) | ? | ? | ? |
| Качество руды (Fe%, влажность) | ? | ? | ? |
| Видео с регистраторов | ? | ? | ? |

---

## Самостоятельные задания

### Задание А. Расширенный ETL: обработка ошибок

Доработайте скрипт полной загрузки (`02_etl_full_load.sql`):

1. Добавьте таблицу `staging.etl_errors` для хранения отклонённых записей:
   ```sql
   CREATE TABLE staging.etl_errors (
       error_id     SERIAL PRIMARY KEY,
       load_id      INTEGER,
       table_name   VARCHAR(50),
       record_key   VARCHAR(50),
       error_type   VARCHAR(50),
       error_message TEXT,
       raw_data     JSONB,
       created_at   TIMESTAMP DEFAULT NOW()
   );
   ```
2. При проверке качества вместо остановки ETL записывайте ошибки в эту таблицу
3. Продолжайте загрузку только "чистых" данных

<details>
<summary>Подсказка</summary>

```sql
-- Вставка ошибок: записи с NULL в обязательных полях
INSERT INTO staging.etl_errors (load_id, table_name, record_key, error_type, error_message, raw_data)
SELECT
    current_load_id,
    'ore_production',
    production_id,
    'null_required_field',
    'Поле mine_id содержит NULL',
    row_to_json(p)::jsonb
FROM staging.stg_ore_production p
WHERE mine_id IS NULL;

-- Загрузка в star: исключаем записи с ошибками
INSERT INTO star.fact_production (...)
SELECT ...
FROM staging.stg_ore_production p
WHERE p.production_id NOT IN (
    SELECT record_key FROM staging.etl_errors
    WHERE load_id = current_load_id AND table_name = 'ore_production'
);
```

</details>

### Задание Б. Потоковые алерты: эскалация

Реализуйте логику эскалации алертов:

1. Если за 15 минут поступило 3+ алертов `warning` по одному оборудованию, создайте алерт `critical`
2. Если алерт `critical` не подтверждён (acknowledged) в течение 10 минут, создайте алерт `emergency`

<details>
<summary>Подсказка</summary>

```sql
-- Эскалация: 3+ warning за 15 минут → critical
INSERT INTO streaming.sensor_alerts (equipment_id, event_type, alert_level, alert_message, event_timestamp)
SELECT equipment_id, event_type, 'critical',
       'ЭСКАЛАЦИЯ: ' || COUNT(*) || ' предупреждений за 15 минут',
       MAX(event_timestamp)
FROM streaming.sensor_alerts
WHERE alert_level = 'warning'
  AND event_timestamp > NOW() - INTERVAL '15 minutes'
GROUP BY equipment_id, event_type
HAVING COUNT(*) >= 3;
```

</details>

### Задание В. Оконные агрегации: Session Window

Реализуйте **Session Window** — окно, которое объединяет события, разделённые паузой менее N минут:

1. Найдите сессии работы оборудования (пауза между событиями > 10 минут = новая сессия)
2. Вычислите длительность каждой сессии и количество событий

<details>
<summary>Подсказка</summary>

```sql
-- Session Window: определяем сессии по паузам > 10 минут
WITH event_gaps AS (
    SELECT event_id, equipment_id, event_timestamp,
           event_timestamp - LAG(event_timestamp) OVER (
               PARTITION BY equipment_id ORDER BY event_timestamp
           ) AS gap
    FROM streaming.raw_sensor_events
),
session_starts AS (
    SELECT *,
           CASE WHEN gap IS NULL OR gap > INTERVAL '10 minutes' THEN 1 ELSE 0 END AS is_new_session
    FROM event_gaps
),
sessions AS (
    SELECT *,
           SUM(is_new_session) OVER (
               PARTITION BY equipment_id ORDER BY event_timestamp
           ) AS session_id
    FROM session_starts
)
SELECT equipment_id, session_id,
       MIN(event_timestamp) AS session_start,
       MAX(event_timestamp) AS session_end,
       MAX(event_timestamp) - MIN(event_timestamp) AS session_duration,
       COUNT(*) AS event_count
FROM sessions
GROUP BY equipment_id, session_id
ORDER BY equipment_id, session_start;
```

</details>

### Задание Г. Комбинированная архитектура

Спроектируйте таблицу `star.fact_sensor_agg_hourly`, которая объединяет потоковые и пакетные данные:

1. Потоковый слой записывает предварительные агрегации (streaming.window_aggregations)
2. Пакетный ETL переносит агрегации в star-схему, обогащая суррогатными ключами

```sql
-- Загрузка потоковых агрегаций в Star Schema
INSERT INTO star.fact_sensor_agg_hourly (
    time_key, equipment_key, hour_of_day, sensor_type,
    avg_value, min_value, max_value, reading_count, alarm_count
)
SELECT
    dt.time_key,
    de.equipment_key,
    EXTRACT(HOUR FROM wa.window_start)::int,
    wa.event_type,
    wa.avg_value, wa.min_value, wa.max_value,
    wa.reading_count,
    (SELECT COUNT(*) FROM streaming.sensor_alerts sa
     WHERE sa.equipment_id = wa.equipment_id
       AND sa.event_type = wa.event_type
       AND sa.event_timestamp >= wa.window_start
       AND sa.event_timestamp < wa.window_end)
FROM streaming.window_aggregations wa
JOIN star.dim_time dt ON wa.window_start::date = dt.production_date
JOIN star.dim_equipment de ON wa.equipment_id = de.equipment_id AND de.is_current = TRUE;
```

---

## Итоговые вопросы для обсуждения

1. **ETL vs ELT:** Чем отличается ETL (Extract-Transform-Load) от ELT (Extract-Load-Transform)? Какой подход лучше подходит для облачных хранилищ (Yandex Cloud)?

2. **Идемпотентность:** Что произойдёт, если ETL-загрузка запустится дважды? Как обеспечить идемпотентность (повторный запуск не создаёт дубликатов)?

3. **Потоковая телеметрия:** На предприятии "Руда+" 12 единиц оборудования, каждая генерирует 1000 событий/сек. Это 1 036 800 000 событий в сутки. Можно ли хранить все в PostgreSQL? Какие альтернативы?

4. **Late-arriving data:** Датчик отправил данные с задержкой 30 минут (из-за потери связи в шахте). Как это повлияет на оконные агрегации? Как обработать опоздавшие данные?

5. **Мониторинг ETL:** Как отслеживать здоровье ETL-пайплайна? Какие метрики важны (время загрузки, количество ошибок, свежесть данных)?

6. **Реальные инструменты:** Какие инструменты используются в промышленности для ETL (Apache Airflow, dbt) и потоковой обработки (Apache Kafka, Apache Flink, Yandex Data Streams)? Чем SQL-моделирование отличается от реальной реализации?

---

## Итоговая проверка

Перед завершением убедитесь, что:

- [ ] Создана и заполнена `staging`-схема с ETL-метаданными
- [ ] Выполнена полная загрузка (Full Load) из OLTP в Star Schema через staging
- [ ] Журнал загрузок (`etl_load_log`) содержит записи обо всех загрузках
- [ ] Инкрементальная загрузка работает корректно (только новые данные)
- [ ] SCD Type 2 обновление для dim_operator выполнено (2 версии для OP-007)
- [ ] Созданы таблицы потоковой обработки и загружены тестовые события
- [ ] Оконные агрегации вычисляются корректно
- [ ] Правила алертов генерируют предупреждения
- [ ] Заполнена сравнительная таблица Batch vs Stream
