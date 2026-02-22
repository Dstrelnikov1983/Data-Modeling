# Практическая работа 5. Специализированное моделирование данных

## Общая информация

| Параметр | Значение |
|---|---|
| **Модуль** | 5. Специализированное моделирование данных |
| **Темы** | 5.1–5.4. Временные ряды (TimescaleDB), графовые модели (Neo4j), полиглотная персистентность |
| **Длительность** | 110–120 минут |
| **Формат** | Индивидуальная работа / работа в парах |
| **Среда** | Yandex Cloud (Managed PostgreSQL) или локальный PostgreSQL + Neo4j Aura Free / Neo4j Desktop |

## Цель работы

Освоить два специализированных подхода к моделированию данных на примере MES-системы предприятия «Руда+»:

1. **Моделирование временных рядов** — хранение и анализ потоковых данных датчиков с использованием TimescaleDB (или стандартного PostgreSQL с секционированием)
2. **Графовое моделирование** — представление связей между объектами шахты (оборудование, операторы, зоны, обслуживание) в Neo4j
3. **Архитектурное решение** — понять, когда и зачем применять различные СУБД для одного предприятия (полиглотная персистентность)

## Предварительные требования

- Выполнены практические работы модулей 1–4 (схемы `ruda_plus`, `star`, `vault`, `staging`)
- Доступ к PostgreSQL (Yandex Cloud Managed PostgreSQL или локальный)
- Для графовой части: аккаунт Neo4j Aura Free **или** установленный Neo4j Desktop **или** Neo4j Sandbox
- Текстовый редактор, DBeaver или psql
- Браузер для Neo4j Browser

## Подготовленные материалы

```
Module_5/practice/
├── README.md                                ← вы читаете этот файл
├── data/
│   ├── sensor_timeseries_sample.csv        ← образец данных временных рядов (100 строк)
│   └── graph_model.json                    ← описание графовой модели (JSON)
└── scripts/
    ├── 01_timescaledb_setup.sql            ← создание схемы и гипертаблиц
    ├── 02_timeseries_data.sql              ← генерация реалистичных данных датчиков
    ├── 03_timeseries_queries.sql           ← аналитические запросы временных рядов
    └── 04_graph_cypher.cypher              ← Cypher-скрипт для Neo4j
```

---

## Часть 1. Моделирование временных рядов с TimescaleDB (50 минут)

### Введение

Временные ряды — последовательности измерений, упорядоченных по времени. На предприятии «Руда+» датчики оборудования генерируют показания каждую минуту: температура двигателя, вибрация подшипников, давление гидравлики, скорость движения, уровень топлива.

**Масштаб задачи:**
- 12 единиц оборудования x 5 датчиков x 1 показание/мин = 60 показаний/мин
- За сутки: 60 x 1440 = 86 400 записей
- За месяц: ~2 600 000 записей
- За год: ~31 000 000 записей

Стандартная реляционная таблица начнёт деградировать при таких объёмах. TimescaleDB решает эту проблему с помощью **гипертаблиц** — автоматического секционирования по времени.

### Шаг 1.1. Подключение к базе данных

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

### Шаг 1.2. Включение расширения TimescaleDB

```sql
-- Попытка включить TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

> **Примечание для Yandex Cloud:** В Managed PostgreSQL расширение TimescaleDB может потребовать активации через консоль управления:
> 1. Перейдите в настройки кластера
> 2. Раздел «Расширения СУБД»
> 3. Добавьте расширение `timescaledb`
> 4. Дождитесь перезапуска кластера

> **Если TimescaleDB недоступен** — не беспокойтесь! В скриптах предусмотрен запасной вариант (fallback) с использованием стандартного секционирования PostgreSQL. Перейдите к Шагу 1.7.

Проверьте, что расширение активно:
```sql
SELECT extname, extversion
FROM pg_extension
WHERE extname = 'timescaledb';
```

Ожидаемый результат:
| extname     | extversion |
|-------------|------------|
| timescaledb | 2.x.x      |

### Шаг 1.3. Создание схемы и гипертаблицы

Выполните скрипт `scripts/01_timescaledb_setup.sql`. Он создаёт:

| Таблица | Описание | Особенность |
|---|---|---|
| `timeseries.sensor_readings` | Показания датчиков | Гипертаблица (chunk по 7 дней) |
| `timeseries.equipment_metrics_hourly` | Часовые агрегаты | Предрассчитанные метрики |
| `timeseries.production_timeseries` | Добыча по часам | Гипертаблица |
| `timeseries.alerts` | Сработавшие алерты | Обычная таблица |

Ключевой момент — преобразование обычной таблицы в гипертаблицу:
```sql
-- Создаём обычную таблицу
CREATE TABLE timeseries.sensor_readings (
    reading_time    TIMESTAMPTZ NOT NULL,
    equipment_id    VARCHAR(20) NOT NULL,
    sensor_type     VARCHAR(30) NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    unit            VARCHAR(20) NOT NULL,
    quality         VARCHAR(10) DEFAULT 'good'
);

-- Превращаем в гипертаблицу с чанками по 7 дней
SELECT create_hypertable(
    'timeseries.sensor_readings',
    'reading_time',
    chunk_time_interval => INTERVAL '7 days'
);
```

После выполнения скрипта проверьте структуру:
```sql
-- Информация о гипертаблице
SELECT hypertable_name, num_dimensions, num_chunks
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'timeseries';
```

Ожидаемый результат: таблица `sensor_readings` отображается как гипертаблица с 1 измерением (время).

### Шаг 1.4. Загрузка данных временных рядов

Выполните скрипт `scripts/02_timeseries_data.sql`. Он генерирует реалистичные данные с помощью `generate_series`:

**Что генерируется:**

| Параметр | Значение |
|---|---|
| Период | 1–31 марта 2025 |
| Оборудование | EQ-001 — EQ-006 (6 единиц) |
| Типы датчиков | temperature, vibration, pressure, speed, fuel_level |
| Интервал | 1 показание в 10 минут (для сокращения объёма) |
| Итого строк | ~26 000+ |

**Реалистичные паттерны в данных:**
- **Температура:** базовая 65°C + суточный цикл (±10°C: холоднее ночью, теплее днём) + шум
- **Вибрация:** базовая 1.5 мм/с + постепенный рост за месяц (моделирует износ) + шум
- **Давление:** базовая 150 бар + случайные колебания
- **Скорость:** 5 км/ч в рабочие часы (6:00–22:00), 0 ночью
- **Топливо:** убывающий уровень в течение смены, пополнение в начале каждой смены

**Аномалии (вшиты в данные):**
- 15 марта: резкий скачок температуры EQ-001 до 105°C (перегрев)
- 20 марта: внезапный рост вибрации EQ-003 до 8 мм/с (дефект подшипника)
- 25 марта: падение давления EQ-002 до 80 бар (утечка)

Проверьте загрузку:
```sql
SELECT COUNT(*) AS total_rows,
       MIN(reading_time) AS first_reading,
       MAX(reading_time) AS last_reading,
       COUNT(DISTINCT equipment_id) AS equipment_count,
       COUNT(DISTINCT sensor_type) AS sensor_types
FROM timeseries.sensor_readings;
```

Ожидаемый результат:
| total_rows | first_reading | last_reading | equipment_count | sensor_types |
|---|---|---|---|---|
| ~26000+ | 2025-03-01 00:00:00 | 2025-03-31 23:50:00 | 6 | 5 |

Также проверьте распределение по чанкам:
```sql
SELECT chunk_name, range_start, range_end,
       pg_size_pretty(total_bytes) AS size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sensor_readings'
ORDER BY range_start;
```

### Шаг 1.5. Аналитические запросы временных рядов

Откройте скрипт `scripts/03_timeseries_queries.sql` и **последовательно** выполняйте запросы. После каждого запроса анализируйте результат.

#### 1.5.1. Агрегация по временным корзинам (time_bucket)

Функция `time_bucket()` — одна из самых мощных функций TimescaleDB. Она группирует данные по интервалам:

```sql
-- Средняя температура за каждые 5 минут
SELECT time_bucket('5 minutes', reading_time) AS bucket,
       equipment_id,
       AVG(value) AS avg_temp,
       MIN(value) AS min_temp,
       MAX(value) AS max_temp
FROM timeseries.sensor_readings
WHERE sensor_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-01'
  AND reading_time < '2025-03-02'
GROUP BY bucket, equipment_id
ORDER BY bucket;
```

Попробуйте разные интервалы:
- `'5 minutes'` — для детального анализа конкретного события
- `'1 hour'` — для суточных паттернов
- `'1 day'` — для трендов за месяц

**Вопрос для обсуждения:** Как выбор размера временной корзины влияет на обнаружение аномалий? Что будет, если использовать слишком большую корзину (1 день) для обнаружения кратковременных скачков температуры?

#### 1.5.2. Скользящие средние

Скользящее среднее сглаживает шумы и показывает тренд:

```sql
-- Скользящее среднее вибрации за 7 точек (обнаружение износа)
SELECT reading_time,
       value AS raw_value,
       AVG(value) OVER (
           PARTITION BY equipment_id
           ORDER BY reading_time
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS moving_avg_7,
       AVG(value) OVER (
           PARTITION BY equipment_id
           ORDER BY reading_time
           ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
       ) AS moving_avg_24
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
  AND equipment_id = 'EQ-003'
  AND reading_time >= '2025-03-15'
  AND reading_time < '2025-03-22'
ORDER BY reading_time;
```

Обратите внимание: `moving_avg_24` показывает более плавный тренд, а `moving_avg_7` реагирует на изменения быстрее.

#### 1.5.3. Обнаружение аномалий (Z-score)

Z-score показывает, на сколько стандартных отклонений значение отличается от среднего:

```sql
-- Аномалии: значения за пределами 2 стандартных отклонений
WITH stats AS (
    SELECT equipment_id,
           sensor_type,
           AVG(value) AS mean_val,
           STDDEV(value) AS std_val
    FROM timeseries.sensor_readings
    GROUP BY equipment_id, sensor_type
)
SELECT sr.reading_time,
       sr.equipment_id,
       sr.sensor_type,
       sr.value,
       ROUND((sr.value - s.mean_val) / NULLIF(s.std_val, 0), 2) AS z_score
FROM timeseries.sensor_readings sr
JOIN stats s ON sr.equipment_id = s.equipment_id
            AND sr.sensor_type = s.sensor_type
WHERE ABS(sr.value - s.mean_val) > 2 * s.std_val
ORDER BY sr.reading_time;
```

**Задание:** Найдите все аномалии в данных. Сколько их? Соответствуют ли они заложенным скачкам (15, 20, 25 марта)?

#### 1.5.4. Анализ трендов

```sql
-- Сравнение средней вибрации по неделям (обнаружение износа)
SELECT time_bucket('1 week', reading_time) AS week,
       equipment_id,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       ROUND(MAX(value)::numeric, 3) AS max_vibration,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY week, equipment_id
ORDER BY equipment_id, week;
```

**Вопрос:** У какого оборудования вибрация растёт от недели к неделе? Что это может означать?

### Шаг 1.6. Непрерывные агрегаты и политики хранения

#### Непрерывные агрегаты (Continuous Aggregates)

Это материализованные представления, которые автоматически обновляются при поступлении новых данных:

```sql
-- Часовой агрегат (обновляется автоматически)
CREATE MATERIALIZED VIEW timeseries.sensor_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', reading_time) AS hour,
       equipment_id,
       sensor_type,
       AVG(value) AS avg_value,
       MIN(value) AS min_value,
       MAX(value) AS max_value,
       COUNT(*) AS reading_count
FROM timeseries.sensor_readings
GROUP BY hour, equipment_id, sensor_type
WITH NO DATA;

-- Первоначальное заполнение
CALL refresh_continuous_aggregate('timeseries.sensor_hourly', NULL, NULL);
```

Запросы к агрегату работают мгновенно, потому что данные уже предрассчитаны:
```sql
-- Вместо сканирования 26000+ строк — читаем готовый агрегат
SELECT * FROM timeseries.sensor_hourly
WHERE equipment_id = 'EQ-001'
  AND sensor_type = 'temperature'
  AND hour >= '2025-03-15'
ORDER BY hour;
```

#### Политики хранения данных

```sql
-- Сжатие данных старше 7 дней (экономит 90%+ дискового пространства)
ALTER TABLE timeseries.sensor_readings
SET (timescaledb.compress,
     timescaledb.compress_segmentby = 'equipment_id, sensor_type');

SELECT add_compression_policy('timeseries.sensor_readings', INTERVAL '7 days');

-- Удаление данных старше 1 года
SELECT add_retention_policy('timeseries.sensor_readings', INTERVAL '1 year');
```

**Вопрос для обсуждения:** Какую стратегию хранения вы бы выбрали для «Руда+»?
- Сырые данные: хранить 3 месяца, затем сжимать
- Часовые агрегаты: хранить 1 год
- Суточные агрегаты: хранить бессрочно

### Шаг 1.7. Запасной вариант (без TimescaleDB)

Если TimescaleDB недоступен, используйте стандартное секционирование PostgreSQL:

```sql
-- Создание секционированной таблицы
CREATE TABLE timeseries.sensor_readings (
    reading_time    TIMESTAMPTZ NOT NULL,
    equipment_id    VARCHAR(20) NOT NULL,
    sensor_type     VARCHAR(30) NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    unit            VARCHAR(20) NOT NULL,
    quality         VARCHAR(10) DEFAULT 'good'
) PARTITION BY RANGE (reading_time);

-- Создание секций помесячно
CREATE TABLE timeseries.sensor_readings_2025_03
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE timeseries.sensor_readings_2025_04
    PARTITION OF timeseries.sensor_readings
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
```

Замены функций:
| TimescaleDB | Стандартный PostgreSQL |
|---|---|
| `time_bucket('1 hour', ts)` | `date_trunc('hour', ts)` |
| `create_hypertable(...)` | `PARTITION BY RANGE (...)` |
| Continuous Aggregate | `CREATE MATERIALIZED VIEW` + `REFRESH` вручную |
| `add_compression_policy(...)` | Ручное архивирование или pg_cron |
| `add_retention_policy(...)` | `DROP TABLE` секции или pg_cron |

Все запросы из скрипта `03_timeseries_queries.sql` имеют fallback-версии для стандартного PostgreSQL (см. комментарии `-- FALLBACK`).

---

## Часть 2. Графовое моделирование с Neo4j / Cypher (40 минут)

### Введение

Графовая база данных хранит данные в виде узлов (вершин) и связей (рёбер). Это идеально подходит для моделирования сложных взаимосвязей, таких как:

- Какой оператор работает на каком оборудовании?
- Где в шахте расположено оборудование и как оно связано маршрутами?
- Какие события обслуживания связаны с каким оборудованием и кем проведены?
- Какие паттерны поломок характерны для определённых комбинаций оператор-оборудование?

В реляционной модели такие запросы требуют множества JOIN и подзапросов. В графе — это естественные обходы (traversals).

### Шаг 2.1. Настройка среды Neo4j

Выберите один из вариантов:

#### Вариант А: Neo4j Aura Free (рекомендуется)

1. Перейдите на [neo4j.com/cloud/aura-free](https://neo4j.com/cloud/aura-free/)
2. Зарегистрируйтесь (бесплатно)
3. Создайте новый инстанс (Free tier)
4. **Важно:** Сохраните пароль, который отобразится при создании!
5. Дождитесь запуска (1–2 минуты)
6. Нажмите «Open with Neo4j Browser»

#### Вариант Б: Neo4j Desktop (локально)

1. Скачайте [Neo4j Desktop](https://neo4j.com/download/)
2. Установите и создайте новый проект
3. Создайте новый DBMS (версия 5.x)
4. Запустите и откройте Neo4j Browser

#### Вариант В: Neo4j Sandbox (временный)

1. Перейдите на [neo4j.com/sandbox](https://neo4j.com/sandbox/)
2. Выберите «Blank Sandbox»
3. Получите доступ через браузер (активен 3 дня)

После подключения убедитесь, что Neo4j Browser открыт и вы видите командную строку `neo4j$`.

### Шаг 2.2. Знакомство с графовой моделью «Руда+»

Прежде чем создавать данные, изучите модель. Откройте файл `data/graph_model.json` для полного описания.

**Узлы (Nodes):**

| Метка (Label) | Описание | Количество | Ключевые свойства |
|---|---|---|---|
| `Mine` | Шахта | 4 | mine_id, name, region, depth_m, status |
| `Horizon` | Горизонт (уровень шахты) | 10 | horizon_id, name, depth_m, mine_id |
| `Equipment` | Оборудование | 12 | equipment_id, name, type, manufacturer, status |
| `Operator` | Оператор | 10 | operator_id, name, qualification, position |
| `Sensor` | Датчик | 20 | sensor_id, type, unit, install_date |
| `MaintenanceEvent` | Событие обслуживания | 15 | event_id, date, type, duration_hours, cost |

**Связи (Relationships):**

| Тип связи | Из | В | Описание |
|---|---|---|---|
| `LOCATED_IN` | Equipment | Mine | Оборудование расположено в шахте |
| `ON_HORIZON` | Equipment | Horizon | Оборудование на конкретном горизонте |
| `WORKS_AT` | Operator | Mine | Оператор работает на шахте |
| `OPERATES` | Operator | Equipment | Оператор управляет оборудованием |
| `HAS_SENSOR` | Equipment | Sensor | У оборудования есть датчик |
| `CONNECTED_TO` | Equipment | Equipment | Маршрутная связь (цепочка транспортировки) |
| `MAINTAINED_BY` | MaintenanceEvent | Operator | Кто проводил обслуживание |
| `REQUIRED_MAINTENANCE` | Equipment | MaintenanceEvent | Оборудование требовало обслуживания |
| `PART_OF` | Horizon | Mine | Горизонт принадлежит шахте |

### Шаг 2.3. Создание данных в Neo4j

Откройте файл `scripts/04_graph_cypher.cypher` и **последовательно** выполняйте блоки команд в Neo4j Browser.

> **Важно:** В Neo4j Browser вставляйте по одному блоку команд (разделённых пустыми строками) и нажимайте Ctrl+Enter (или кнопку Play).

#### Шаг 2.3.1. Очистка и ограничения

```cypher
// Очистка базы (только для учебных целей!)
MATCH (n) DETACH DELETE n;

// Создание ограничений уникальности
CREATE CONSTRAINT mine_id_unique IF NOT EXISTS
FOR (m:Mine) REQUIRE m.mine_id IS UNIQUE;

CREATE CONSTRAINT equipment_id_unique IF NOT EXISTS
FOR (e:Equipment) REQUIRE e.equipment_id IS UNIQUE;

CREATE CONSTRAINT operator_id_unique IF NOT EXISTS
FOR (o:Operator) REQUIRE o.operator_id IS UNIQUE;
```

#### Шаг 2.3.2. Создание узлов

Выполните блоки создания шахт, горизонтов, оборудования и операторов из скрипта. Обратите внимание на синтаксис:

```cypher
// Создание шахты с свойствами
CREATE (m:Mine {
    mine_id: 'MINE-001',
    name: 'Северная',
    region: 'Кольский полуостров',
    depth_m: 850,
    status: 'Активная',
    commissioned_year: 1985
});
```

#### Шаг 2.3.3. Создание связей

```cypher
// Связь: оборудование расположено в шахте
MATCH (e:Equipment {equipment_id: 'EQ-001'}),
      (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2022-01-15')}]->(m);
```

После загрузки всех данных проверьте:
```cypher
// Статистика графа
MATCH (n)
RETURN labels(n)[0] AS label, COUNT(*) AS count
ORDER BY count DESC;
```

Ожидаемый результат:
| label | count |
|---|---|
| Sensor | 20 |
| MaintenanceEvent | 15 |
| Equipment | 12 |
| Horizon | 10 |
| Operator | 10 |
| Mine | 4 |

### Шаг 2.4. Запросы к графу

#### 2.4.1. Поиск по паттернам (MATCH)

```cypher
// Всё оборудование в шахте «Северная»
MATCH (e:Equipment)-[:LOCATED_IN]->(m:Mine {name: 'Северная'})
RETURN e.name AS equipment, e.type AS type, e.status AS status;
```

```cypher
// Операторы, которые управляют ПДМ-машинами
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)
WHERE e.type = 'ПДМ'
RETURN o.name AS operator, o.qualification AS qualification,
       e.name AS equipment;
```

```cypher
// Полная цепочка: Оператор → Оборудование → Шахта
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)-[:LOCATED_IN]->(m:Mine)
RETURN o.name AS operator, e.name AS equipment, m.name AS mine
ORDER BY m.name, e.name;
```

**Вопрос:** Сколько JOIN потребовалось бы в SQL для последнего запроса? В Cypher — это один MATCH.

#### 2.4.2. Агрегации в Cypher

```cypher
// Количество оборудования по шахтам
MATCH (e:Equipment)-[:LOCATED_IN]->(m:Mine)
RETURN m.name AS mine, COUNT(e) AS equipment_count
ORDER BY equipment_count DESC;
```

```cypher
// Средняя стоимость обслуживания по типам оборудования
MATCH (e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
RETURN e.type AS equipment_type,
       COUNT(me) AS events,
       ROUND(AVG(me.cost)) AS avg_cost,
       SUM(me.duration_hours) AS total_hours
ORDER BY avg_cost DESC;
```

### Шаг 2.5. Графовая аналитика

#### 2.5.1. Кратчайший путь

```cypher
// Кратчайший путь транспортировки руды: от забоя до скипового подъёмника
MATCH path = shortestPath(
    (start:Equipment {type: 'ПДМ', equipment_id: 'EQ-001'})
    -[:CONNECTED_TO*]->
    (finish:Equipment {type: 'Скиповый подъёмник'})
)
RETURN [n IN nodes(path) | n.name] AS route,
       length(path) AS hops;
```

#### 2.5.2. Изолированные узлы

```cypher
// Оборудование без назначенного оператора
MATCH (e:Equipment)
WHERE NOT (e)<-[:OPERATES]-(:Operator)
RETURN e.equipment_id AS id, e.name AS equipment,
       e.type AS type, e.status AS status;
```

#### 2.5.3. Степень связности (Degree Centrality)

```cypher
// Самые «связанные» узлы — потенциальные точки отказа
MATCH (e:Equipment)
OPTIONAL MATCH (e)-[r]-()
RETURN e.name AS equipment,
       e.type AS type,
       COUNT(r) AS total_connections
ORDER BY total_connections DESC
LIMIT 10;
```

**Вопрос:** Почему узлы с наибольшим числом связей — потенциальные точки отказа? Что произойдёт, если скиповый подъёмник выйдет из строя?

#### 2.5.4. Рекомендация оператора для оборудования

```cypher
// Найти оператора с наибольшим опытом работы на данном типе оборудования
MATCH (e:Equipment {equipment_id: 'EQ-005'})
WITH e, e.type AS target_type
MATCH (o:Operator)-[:OPERATES]->(similar:Equipment {type: target_type})
WHERE NOT (o)-[:OPERATES]->(e)
RETURN o.name AS recommended_operator,
       o.qualification AS qualification,
       COUNT(similar) AS experience_with_type
ORDER BY experience_with_type DESC
LIMIT 3;
```

### Шаг 2.6. Обнаружение аномальных паттернов обслуживания

#### Оборудование с частыми поломками

```cypher
// Оборудование с более чем 2 событиями обслуживания
MATCH (e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
WITH e, COUNT(me) AS maintenance_count,
     SUM(me.duration_hours) AS total_downtime,
     SUM(me.cost) AS total_cost
WHERE maintenance_count > 2
RETURN e.name AS equipment,
       e.type AS type,
       maintenance_count,
       total_downtime AS downtime_hours,
       total_cost
ORDER BY maintenance_count DESC;
```

#### Необычные комбинации оператор-поломка

```cypher
// Операторы, после работы которых оборудование чаще ломается
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
WHERE me.type = 'Аварийный'
RETURN o.name AS operator,
       COLLECT(DISTINCT e.name) AS broken_equipment,
       COUNT(me) AS emergency_events
ORDER BY emergency_events DESC;
```

**Вопрос для обсуждения:** Если вы обнаружили, что после одного оператора оборудование ломается чаще, какие гипотезы можно выдвинуть? (Нарушение эксплуатации? Сложные условия на его участке? Совпадение?)

---

## Часть 3. Сравнение подходов и архитектурное решение (20 минут)

### Шаг 3.1. Когда использовать каждый тип СУБД

Заполните таблицу на основе выполненной работы:

| Критерий | Реляционная (PostgreSQL) | Временные ряды (TimescaleDB) | Графовая (Neo4j) |
|---|---|---|---|
| **Тип данных** | ? | ? | ? |
| **Типичные запросы** | ? | ? | ? |
| **Сильная сторона** | ? | ? | ? |
| **Слабая сторона** | ? | ? | ? |
| **Пример для «Руда+»** | ? | ? | ? |

<details>
<summary>Ожидаемые ответы</summary>

| Критерий | Реляционная (PostgreSQL) | Временные ряды (TimescaleDB) | Графовая (Neo4j) |
|---|---|---|---|
| **Тип данных** | Структурированные бизнес-данные | Последовательности измерений во времени | Сильно связанные данные |
| **Типичные запросы** | CRUD, отчёты, транзакции | Агрегация по времени, тренды, аномалии | Обход связей, пути, рекомендации |
| **Сильная сторона** | ACID, целостность, зрелость | Сжатие, автосекционирование, time_bucket | Обход графа за O(1) по связи |
| **Слабая сторона** | Медленно при >100 млн строк TS-данных | Не для общего назначения (OLTP/OLAP) | Нет ACID в кластере, сложные агрегации |
| **Пример для «Руда+»** | Справочники, добыча, простои | Показания датчиков, мониторинг | Маршруты, зависимости, рекомендации |

</details>

### Шаг 3.2. Полиглотная персистентность для MES «Руда+»

Архитектура MES-системы «Руда+» с использованием нескольких СУБД:

```
┌─────────────────────────────────────────────────────────┐
│                    MES-система «Руда+»                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────────┐ │
│  │  PostgreSQL   │  │  TimescaleDB  │  │    Neo4j     │ │
│  │              │  │               │  │              │ │
│  │ Справочники  │  │ Телеметрия    │  │ Маршруты     │ │
│  │ Добыча       │  │ Мониторинг    │  │ Зависимости  │ │
│  │ Простои      │  │ Алерты        │  │ Рекомендации │ │
│  │ OLTP + OLAP  │  │ Тренды        │  │ Аналитика    │ │
│  │              │  │ Прогнозы      │  │ связей       │ │
│  └──────┬───────┘  └───────┬───────┘  └──────┬───────┘ │
│         │                  │                  │         │
│         └──────────┬───────┴──────────┬───────┘         │
│                    │  ETL / API       │                  │
│              ┌─────▼─────────────────▼─────┐            │
│              │     Слой интеграции (API)    │            │
│              └─────────────────────────────┘            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Вопрос для обсуждения:** Каковы плюсы и минусы полиглотной персистентности?

<details>
<summary>Ключевые тезисы</summary>

**Плюсы:**
- Каждая СУБД оптимальна для своего типа данных
- Лучшая производительность для специализированных задач
- Масштабирование каждого компонента независимо

**Минусы:**
- Сложность инфраструктуры (3 СУБД вместо 1)
- Необходимость синхронизации данных между системами
- Более высокие требования к квалификации команды
- Отсутствие единых ACID-транзакций между СУБД

**Компромисс для «Руда+»:**
На начальном этапе можно использовать PostgreSQL + TimescaleDB (одна СУБД с расширением) и добавить Neo4j позже, когда потребуется сложный анализ связей.

</details>

### Шаг 3.3. Групповое задание: архитектурное решение

Разделитесь на группы по 3–4 человека. Каждая группа получает сценарий:

**Сценарий А:** На предприятие «Руда+» устанавливается система видеонаблюдения (100 камер). Где хранить видеопоток и метаданные?

**Сценарий Б:** Предприятие расширяется: добавляется 5 новых шахт и 200 единиц оборудования. Как изменится архитектура?

**Сценарий В:** Регулятор требует хранить все сырые данные датчиков 5 лет без потери качества. Как организовать хранение?

Подготовьте 5-минутную презентацию с обоснованием архитектурного решения.

---

## Самостоятельные задания

### Задание А. Прогнозирование отказов (Time Series)

Используя данные вибрации оборудования EQ-003, определите:
1. Когда вибрация впервые превысила порог 5 мм/с?
2. Какова скорость роста средней вибрации (мм/с в неделю)?
3. Если тренд продолжится, через сколько дней вибрация достигнет критического значения 10 мм/с?

<details>
<summary>Подсказка</summary>

```sql
-- 1. Первое превышение порога
SELECT MIN(reading_time) AS first_exceedance
FROM timeseries.sensor_readings
WHERE equipment_id = 'EQ-003'
  AND sensor_type = 'vibration'
  AND value > 5.0;

-- 2. Средняя вибрация по неделям
SELECT time_bucket('1 week', reading_time) AS week,
       AVG(value) AS avg_vib
FROM timeseries.sensor_readings
WHERE equipment_id = 'EQ-003'
  AND sensor_type = 'vibration'
GROUP BY week
ORDER BY week;

-- 3. Линейная регрессия (оценка тренда)
SELECT regr_slope(value, EXTRACT(EPOCH FROM reading_time)) AS slope_per_second,
       regr_slope(value, EXTRACT(EPOCH FROM reading_time)) * 86400 * 7 AS slope_per_week,
       regr_intercept(value, EXTRACT(EPOCH FROM reading_time)) AS intercept
FROM timeseries.sensor_readings
WHERE equipment_id = 'EQ-003'
  AND sensor_type = 'vibration';
```

</details>

### Задание Б. Анализ маршрутов (Graph)

В Neo4j постройте запрос, который:
1. Найдёт все возможные маршруты от ПДМ-01 (EQ-001) до скипового подъёмника
2. Определит самый короткий маршрут
3. Определит маршрут с наименьшим числом перегрузок
4. Что произойдёт, если вагонетка EQ-007 выйдет из строя? Существует ли альтернативный маршрут?

<details>
<summary>Подсказка</summary>

```cypher
// 1. Все маршруты
MATCH path = (start:Equipment {equipment_id: 'EQ-001'})
    -[:CONNECTED_TO*1..5]->
    (finish:Equipment {type: 'Скиповый подъёмник'})
RETURN [n IN nodes(path) | n.name] AS route,
       length(path) AS hops;

// 4. Альтернативный маршрут без EQ-007
MATCH path = (start:Equipment {equipment_id: 'EQ-001'})
    -[:CONNECTED_TO*1..6]->
    (finish:Equipment {type: 'Скиповый подъёмник'})
WHERE NONE(n IN nodes(path) WHERE n.equipment_id = 'EQ-007')
RETURN [n IN nodes(path) | n.name] AS route,
       length(path) AS hops;
```

</details>

### Задание В. Интеграция данных

Предложите архитектуру ETL-процесса, который:
1. Берёт данные об оборудовании из PostgreSQL (схема `ruda_plus`)
2. Создаёт/обновляет узлы и связи в Neo4j
3. Агрегирует данные датчиков из TimescaleDB
4. Добавляет метрики надёжности в свойства узлов Equipment в Neo4j

Нарисуйте схему потоков данных и опишите, какие инструменты можно использовать (Apache Kafka, Apache Airflow, pg_cron, neo4j-etl и т.д.).

---

## Итоговые вопросы для обсуждения

1. **TimescaleDB vs InfluxDB:** Каковы преимущества TimescaleDB (SQL-совместимость) по сравнению со специализированными TSDB (InfluxDB, Prometheus)?
2. **Графовая модель vs JOIN:** При каком количестве JOIN реляционный запрос становится неэффективным и стоит рассмотреть графовую БД?
3. **Масштабирование:** Как бы вы масштабировали каждый компонент (PostgreSQL, TimescaleDB, Neo4j) при росте «Руда+» до 1000 единиц оборудования и 10 000 датчиков?
4. **Реальный мир:** Какие ещё специализированные СУБД могут понадобиться для MES? (Redis для кэширования? Elasticsearch для логов? MinIO/S3 для видео?)
5. **Стоимость владения:** Как оценить, стоит ли внедрять дополнительную СУБД или «дотянуть» PostgreSQL?
