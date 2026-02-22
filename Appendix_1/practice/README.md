# Приложение 1. Моделирование данных с помощью XML и JSON

## Практические упражнения

**Предприятие:** «Руда+» — добыча железной руды
**Контекст:** MES-система для анализа качества руды, простоев оборудования и эффективности процесса
**Среда:** PostgreSQL 15+ (Яндекс Облако)

---

## Структура файлов

```
Appendix_1/
├── xml_json_modeling.html          # Презентация
└── practice/
    ├── README.md                    # Этот файл
    ├── data/
    │   ├── equipment.xml            # XML-документ: паспорта оборудования
    │   ├── equipment.xsd            # XSD-схема оборудования
    │   ├── sensors.xsd              # XSD-схема датчиков
    │   ├── equipment.json           # JSON-документ: паспорта оборудования
    │   ├── sensor_telemetry.json    # JSON-документ: телеметрия датчиков
    │   ├── equipment_schema.json    # JSON Schema: паспорт оборудования
    │   └── sensor_telemetry_schema.json  # JSON Schema: телеметрия
    └── scripts/
        ├── 01_create_json_tables.sql    # Создание таблиц
        ├── 02_load_json_data.sql        # Загрузка данных
        ├── 03_json_queries.sql          # Запросы к JSON/XML
        └── 04_xpath_jsonpath.sql        # XPath и JSONPath: задания и ответы
```

---

## Предварительные требования

1. **PostgreSQL 15+** — установлен и доступен (Яндекс Облако или локально)
2. **Клиент PostgreSQL** — psql, DBeaver или pgAdmin
3. **Python 3.8+** — для валидации JSON Schema (pip install jsonschema)
4. **xmllint** — для валидации XML (входит в пакет libxml2)

### Установка зависимостей

```bash
# Python-библиотека для JSON Schema валидации
pip install jsonschema

# Проверка xmllint (Linux/macOS)
xmllint --version

# Проверка PostgreSQL
psql --version
```

---

## Упражнение 1: XML Schema (XSD)

**Время:** 30 минут
**Цель:** разработать XSD-схему для XML-документа и выполнить валидацию

### Шаг 1. Изучите XML-документ

Откройте файл `data/equipment.xml` и изучите его структуру:

```bash
cat data/equipment.xml
```

Обратите внимание на:
- Корневой элемент `<equipmentList>` с пространством имён
- Вложенные элементы `<equipment>` с паспортами машин
- Атрибуты (code, id, type, unit)
- Пространство имён для датчиков (`sensor:`)

### Шаг 2. Изучите готовую XSD-схему

Откройте файл `data/equipment.xsd`:

```bash
cat data/equipment.xsd
```

Проанализируйте:
- **Пользовательские типы**: `EquipmentIdType` (паттерн `[A-Z]{2,4}-\d{3}`), `EquipmentCategoryType` (перечисление)
- **Ограничения**: `PayloadValueType` (диапазон 0–100), `YearType` (2000–2030)
- **Сложные типы**: `MineType` (текст + атрибут), `SpecificationsType` (вложенные элементы)

### Шаг 3. Выполните валидацию

```bash
# Валидация XML по XSD-схеме
xmllint --schema data/equipment.xsd data/equipment.xml --noout
```

Ожидаемый результат: `data/equipment.xml validates`

### Шаг 4. Проверьте невалидный документ

Создайте копию XML-файла и внесите ошибки:

```bash
cp data/equipment.xml data/equipment_invalid.xml
```

Внесите следующие изменения в `equipment_invalid.xml`:

1. Измените `<id>PDM-001</id>` на `<id>pdm-1</id>` (нарушение паттерна)
2. Измените `<type>ПДМ</type>` на `<type>Бульдозер</type>` (нет в enumeration)
3. Измените `<payload unit="tonnes">17.0</payload>` на `<payload unit="tonnes">200.0</payload>` (превышение maxInclusive)

```bash
# Повторная валидация — должна вернуть ошибки
xmllint --schema data/equipment.xsd data/equipment_invalid.xml --noout
```

### Шаг 5. Самостоятельное задание

Расширьте XSD-схему:
- Добавьте элемент `<operator>` с полями: имя (строка), табельный номер (формат `T-\d{4}`), квалификация (перечисление: «стажёр», «оператор», «мастер»)
- Сделайте поле `<operator>` необязательным (`minOccurs="0"`)

---

## Упражнение 2: JSON Schema

**Время:** 25 минут
**Цель:** создать JSON Schema и валидировать JSON-документы с помощью Python

### Шаг 1. Изучите JSON-документ телеметрии

```bash
cat data/sensor_telemetry.json
```

Обратите внимание на:
- Массив показаний с разными типами датчиков
- Вложенные объекты (location, gps_data, alert)
- Значение `null` для нечисловых показаний
- Статусы: normal, warning, critical

### Шаг 2. Изучите JSON Schema

```bash
cat data/sensor_telemetry_schema.json
```

Проанализируйте:
- **$defs** — переиспользуемые определения (SensorReading, Location, GpsData, Alert)
- **$ref** — ссылки на определения
- **oneOf** — допуск нескольких типов (number или null для value)
- **pattern** — регулярные выражения для идентификаторов
- **enum** — ограничение допустимых значений
- **format** — проверка формата date-time

### Шаг 3. Валидация с Python

Создайте файл `validate_telemetry.py`:

```python
import json
import jsonschema
from jsonschema import validate, ValidationError

# Загрузка схемы
with open('data/sensor_telemetry_schema.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)

# Загрузка данных
with open('data/sensor_telemetry.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Валидация
try:
    validate(instance=data, schema=schema)
    print("✓ Документ валиден!")
    print(f"  Количество показаний: {len(data)}")
except ValidationError as e:
    print(f"✗ Ошибка валидации: {e.message}")
    print(f"  Путь: {' -> '.join(str(p) for p in e.absolute_path)}")
```

```bash
python validate_telemetry.py
```

### Шаг 4. Проверьте невалидный документ

Создайте файл `data/sensor_telemetry_invalid.json`:

```json
[
  {
    "reading_id": "R-000001",
    "sensor_id": "INVALID",
    "equipment_id": "PDM-001",
    "timestamp": "not-a-date",
    "type": "unknown_type",
    "status": "ok"
  }
]
```

```bash
# Запустите валидацию — должны появиться ошибки
python validate_telemetry.py
```

### Шаг 5. Валидация паспорта оборудования

```python
import json
from jsonschema import validate, ValidationError

with open('data/equipment_schema.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)

with open('data/equipment.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

try:
    validate(instance=data, schema=schema)
    print("✓ Паспорта оборудования валидны!")
    print(f"  Количество единиц: {len(data)}")
    for eq in data:
        print(f"  - {eq['equipment_id']}: {eq['model']}")
except ValidationError as e:
    print(f"✗ Ошибка: {e.message}")
```

### Шаг 6. Самостоятельное задание

Создайте JSON Schema для отчёта о качестве руды со следующими полями:
- `sample_id` — строка, формат `ORE-\d{4}`
- `date` — дата в формате ISO
- `iron_content_pct` — число от 20.0 до 70.0 (содержание железа в %)
- `impurities` — объект с полями: silicon (число), sulfur (число), phosphorus (число)
- `horizon` — строка
- `equipment_id` — ссылка на оборудование
- `laboratory` — строка
- `is_approved` — булево

---

## Упражнение 3: SQL/JSON в PostgreSQL

**Время:** 35 минут
**Цель:** создать таблицы с JSONB/XML данными, выполнить запросы и операции обновления

### Шаг 1. Подключитесь к PostgreSQL

```bash
psql -h <host> -p 5432 -U <user> -d <database>
```

### Шаг 2. Создайте таблицы

```bash
\i scripts/01_create_json_tables.sql
```

Скрипт создаст 4 таблицы:
| Таблица | Описание |
|---------|----------|
| `equipment_json` | JSONB-документы паспортов оборудования |
| `sensor_readings_json` | JSONB-телеметрия датчиков |
| `equipment_hybrid` | Гибридная модель (реляция + JSONB) |
| `equipment_xml` | XML-документы паспортов |

### Шаг 3. Загрузите данные

```bash
\i scripts/02_load_json_data.sql
```

Проверьте загрузку:

```sql
SELECT 'equipment_json' AS tbl, COUNT(*) FROM equipment_json
UNION ALL
SELECT 'sensor_readings_json', COUNT(*) FROM sensor_readings_json
UNION ALL
SELECT 'equipment_hybrid', COUNT(*) FROM equipment_hybrid
UNION ALL
SELECT 'equipment_xml', COUNT(*) FROM equipment_xml;
```

Ожидаемый результат:
```
       tbl              | count
------------------------+-------
 equipment_json         |     5
 sensor_readings_json   |    10
 equipment_hybrid       |     5
 equipment_xml          |     3
```

### Шаг 4. Выполните запросы

Откройте файл `scripts/03_json_queries.sql` и выполняйте запросы пошагово.

#### Часть 1: Базовые запросы к JSONB

```sql
-- Извлечение полей (оператор ->>)
SELECT
    data->>'equipment_id' AS equipment_id,
    data->>'type' AS eq_type,
    data->'mine'->>'code' AS mine_code
FROM equipment_json;
```

**Задание:** объясните разницу между операторами `->` и `->>`.

#### Часть 2: Работа с массивами

```sql
-- Разворачивание массива датчиков
SELECT
    data->>'equipment_id' AS equipment_id,
    sensor->>'sensor_id' AS sensor_id,
    sensor->>'type' AS sensor_type
FROM equipment_json,
     jsonb_array_elements(data->'sensors') AS sensor;
```

**Задание:** напишите запрос, который найдёт оборудование с более чем 3 датчиками.

#### Часть 3: JSONPath

```sql
-- Фильтрация через JSONPath
SELECT data->>'equipment_id' AS id
FROM equipment_json
WHERE data @@ '$.specifications.payload_tonnes > 15';
```

**Задание:** напишите JSONPath-запрос для поиска оборудования с датчиками GPS.

#### Часть 4: Агрегация

```sql
-- Средняя грузоподъёмность по типу
SELECT
    data->>'type' AS eq_type,
    ROUND(AVG((data->'specifications'->>'payload_tonnes')::decimal), 1) AS avg_payload
FROM equipment_json
GROUP BY data->>'type';
```

#### Часть 5: Обновление

```sql
-- Обновление поля в JSONB
UPDATE equipment_json
SET data = jsonb_set(data, '{is_active}', 'false')
WHERE data->>'equipment_id' = 'VG-001';
```

**Задание:** добавьте новое поле `firmware_version` со значением `"3.2.1"` для оборудования PDM-001.

### Шаг 5. Сравните планы выполнения

```sql
-- Без GIN-индекса
EXPLAIN ANALYZE
SELECT data->>'equipment_id'
FROM equipment_json
WHERE data->>'type' = 'ПДМ';

-- С GIN-индексом (оператор @>)
EXPLAIN ANALYZE
SELECT data->>'equipment_id'
FROM equipment_json
WHERE data @> '{"type": "ПДМ"}'::jsonb;
```

**Задание:** зафиксируйте разницу в планах выполнения. Какой оператор использует индекс?

### Шаг 6. Запросы к XML

```sql
-- Извлечение полей из XML с помощью xpath()
SELECT
    (xpath('//n:id/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS id,
    (xpath('//n:type/text()', data,
           ARRAY[ARRAY['n', 'http://ruda-plus.ru/mes/equipment']]))[1]::text AS type
FROM equipment_xml;
```

---

## Упражнение 4: XPath и JSONPath

**Время:** 20 минут
**Цель:** научиться писать запросы XPath и JSONPath для извлечения данных

### Задание

Заполните таблицу запросами XPath и JSONPath:

| # | Задача | XPath | JSONPath |
|---|--------|-------|----------|
| 1 | Все идентификаторы оборудования | ??? | ??? |
| 2 | Датчики вибрации | ??? | ??? |
| 3 | Оборудование шахты SH-01 | ??? | ??? |
| 4 | Грузоподъёмность > 15 тонн | ??? | ??? |
| 5 | Название модели первой машины | ??? | ??? |

### Проверка ответов

Ответы находятся в файле `scripts/04_xpath_jsonpath.sql`. Сначала попробуйте написать запросы самостоятельно, затем сверьтесь с ответами.

### Дополнительные задания

1. Найдите все уникальные типы датчиков на предприятии
2. Найдите оборудование без датчиков
3. Для каждого типа оборудования покажите все модели
4. Найдите оборудование с GPS и видео датчиками одновременно
5. Постройте сводку: оборудование × типы датчиков (перекрёстный запрос)

Ответы — в конце файла `scripts/04_xpath_jsonpath.sql` (раздел «Дополнительные задания»).

---

## Критерии оценки

| Критерий | Баллы |
|----------|-------|
| XSD-схема корректно валидирует XML | 15 |
| XSD содержит пользовательские типы и ограничения | 10 |
| JSON Schema корректно валидирует JSON | 15 |
| JSON Schema использует $ref и $defs | 10 |
| SQL-запросы к JSONB выполняются корректно | 20 |
| XPath и JSONPath запросы написаны правильно | 15 |
| Самостоятельные задания выполнены | 15 |
| **Итого** | **100** |

---

## Полезные ссылки

- [W3C XML Schema](https://www.w3.org/XML/Schema) — спецификация XSD
- [JSON Schema](https://json-schema.org/) — спецификация JSON Schema
- [PostgreSQL JSON Functions](https://www.postgresql.org/docs/current/functions-json.html) — документация PostgreSQL
- [JSONPath Specification (RFC 9535)](https://www.rfc-editor.org/rfc/rfc9535) — стандарт JSONPath
- [XPath Tutorial (MDN)](https://developer.mozilla.org/en-US/docs/Web/XPath) — руководство по XPath
