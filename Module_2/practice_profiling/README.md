# Практическая работа: Профилирование и валидация данных

## Информация

| Параметр | Значение |
|----------|----------|
| **Модуль** | 2. Основы моделирования данных (дополнительная работа) |
| **Тема** | Профилирование данных (Data Profiling) и валидация качества (Data Quality) |
| **Длительность** | 60–90 минут |
| **Формат** | Индивидуальная работа в Jupyter Notebook / Python |
| **Среда** | Локальная машина или Yandex Cloud (Managed Service for PostgreSQL + VM) |

## Цель работы

Научиться автоматически исследовать наборы данных и настраивать правила валидации качества с помощью двух инструментов:

1. **YData-Profiling** — автоматический EDA-отчёт (типы, распределения, корреляции, пропуски, дубликаты, выбросы)
2. **Great Expectations** — декларативная валидация данных через «ожидания» (expectations) с автодокументацией

## Предварительные требования

- Выполнена практическая работа Модуля 1 (таблицы и данные загружены)
- Установлен Python 3.9+
- Базовое знакомство с Pandas

## Подготовленные материалы

```
practice_profiling/
├── README.md                          # ← вы здесь
├── requirements.txt                   # Зависимости Python
├── scripts/
│   ├── 01_ydata_profiling.py          # Профилирование CSV-файлов
│   ├── 02_ydata_profiling_db.py       # Профилирование таблиц PostgreSQL
│   └── 03_great_expectations.py       # Валидация данных через GX
└── (результаты генерируются в reports/)
```

**Данные для работы** — файлы из Модуля 1:

```
Module_1/practice/data/
├── equipment.csv          # 12 единиц оборудования
├── sensor_readings.csv    # 50 показаний датчиков
├── ore_production.csv     # 15 записей добычи
└── downtime_events.csv    # 10 событий простоя
```

---

## Часть 1. Установка окружения (10 мин)

### Шаг 1.1. Создание виртуального окружения

```bash
# Перейдите в папку практической работы
cd Module_2/practice_profiling

# Создайте виртуальное окружение
python -m venv .venv

# Активируйте его
# Linux / macOS:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# Установите зависимости
pip install -r requirements.txt
```

### Шаг 1.2. Проверка установки

```python
import ydata_profiling
import great_expectations as gx
import pandas as pd

print(f"ydata-profiling: {ydata_profiling.__version__}")
print(f"great_expectations: {gx.__version__}")
print(f"pandas: {pd.__version__}")
```

> **Совет:** Если установка ydata-profiling занимает много времени, можно использовать `pip install ydata-profiling --no-deps` и доставить зависимости по ошибкам. Но обычно `pip install -r requirements.txt` работает корректно.

---

## Часть 2. YData-Profiling: автоматический EDA (25 мин)

### Шаг 2.1. Профилирование CSV-файлов

Запустите скрипт `01_ydata_profiling.py`:

```bash
python scripts/01_ydata_profiling.py
```

Скрипт создаст папку `reports/` с HTML-отчётами для каждого CSV-файла:

```
reports/
├── profile_equipment.html
├── profile_sensor_readings.html
├── profile_ore_production.html
└── profile_downtime_events.html
```

### Шаг 2.2. Изучение отчёта — equipment.csv

Откройте файл `reports/profile_equipment.html` в браузере. Изучите разделы:

| Раздел отчёта | На что обратить внимание |
|---|---|
| **Overview** | Количество строк, колонок, пропусков, дубликатов |
| **Variables** | Тип каждой колонки (числовая, категориальная, текстовая, дата) |
| **Interactions** | Корреляции между числовыми полями (engine_hours vs max_payload_tons) |
| **Missing values** | Матрица пропусков — есть ли паттерн? |
| **Duplicates** | Полные дубликаты строк |
| **Alerts** | Предупреждения: высокая кардинальность, константные колонки, перекосы |

> **Вопрос для размышления:** Какие поля YData-Profiling определил как категориальные? Совпадает ли это с вашими ожиданиями?

### Шаг 2.3. Изучение отчёта — sensor_readings.csv

Откройте `reports/profile_sensor_readings.html`:

1. Найдите раздел **Variables → reading_value**
2. Посмотрите на распределение (гистограмма)
3. Найдите раздел **Alerts**

> **Вопрос:** Есть ли выбросы в показаниях датчиков? Как YData-Profiling их определяет?

### Шаг 2.4. Изучение отчёта — ore_production.csv

Откройте `reports/profile_ore_production.html`:

1. Посмотрите на распределение `tonnage_extracted` и `fe_content_pct`
2. Найдите корреляции: связаны ли тоннаж и содержание Fe?
3. Проверьте раздел **Missing values**

> **Вопрос:** Обнаружил ли YData-Profiling поля с денормализацией (mine_name, operator_name)? Как это выглядит в отчёте?

### Шаг 2.5. Сравнительный отчёт (Comparison)

Скрипт `01_ydata_profiling.py` также создаёт **сравнительный отчёт** между двумя сменами (shift 1 vs shift 2):

```
reports/profile_production_comparison.html
```

Откройте его и найдите различия в распределениях тоннажа между сменами.

> **Вопрос:** Есть ли статистически значимые различия между сменами? Какая смена более продуктивна?

### Шаг 2.6. Профилирование из PostgreSQL (опционально)

Если у вас есть доступ к PostgreSQL с данными Модуля 1:

```bash
# Отредактируйте параметры подключения в скрипте
python scripts/02_ydata_profiling_db.py
```

Этот скрипт подключается к БД, загружает таблицу `ore_production` через SQL-запрос и формирует отчёт.

---

## Часть 3. Great Expectations: валидация данных (25 мин)

### Шаг 3.1. Концепция Great Expectations

Great Expectations (GX) — это фреймворк для описания **ожиданий** (expectations) к данным и автоматической проверки их выполнения.

Основные понятия:

| Понятие | Описание |
|---|---|
| **Expectation** | Одно правило валидации (напр. «колонка не содержит NULL») |
| **Expectation Suite** | Набор правил для одного датасета |
| **Validation Result** | Результат проверки: pass / fail для каждого правила |
| **Data Docs** | Автоматически генерируемая HTML-документация |

### Шаг 3.2. Запуск валидации

```bash
python scripts/03_great_expectations.py
```

Скрипт выполнит:

1. Загрузку CSV-файлов «Руда+»
2. Создание Expectation Suite для каждого датасета
3. Валидацию данных
4. Вывод результатов в консоль

### Шаг 3.3. Изучение правил валидации — equipment.csv

Скрипт проверяет для `equipment.csv`:

```
Структурные проверки:
  ✓ equipment_id — не NULL, уникальный
  ✓ equipment_name — не NULL
  ✓ mine_id — не NULL, значения из множества {MINE-1, MINE-2}
  ✓ status — значения из множества {В работе, На ТО, Простой}

Бизнес-правила:
  ✓ year_manufactured — в диапазоне [2010, 2026]
  ✓ engine_hours — ≥ 0
  ✓ max_payload_tons — в диапазоне [0, 100]

Полнота:
  ✓ Таблица содержит ≥ 10 строк
```

> **Вопрос:** Почему мы проверяем, что `mine_id` принимает значения из конкретного множества? Как это связано с понятием ссылочной целостности?

### Шаг 3.4. Изучение правил валидации — sensor_readings.csv

```
Структурные проверки:
  ✓ reading_id — не NULL, уникальный
  ✓ equipment_id — не NULL
  ✓ sensor_type — не NULL
  ✓ quality_flag — значения из множества {OK, WARN, ALARM}

Бизнес-правила:
  ✓ reading_value — ≥ 0 (показания датчиков не могут быть отрицательными)
  ✓ reading_timestamp — не NULL

Связность:
  ✓ equipment_id — все значения присутствуют в equipment.csv
```

### Шаг 3.5. Изучение правил валидации — ore_production.csv

```
Структурные проверки:
  ✓ production_id — не NULL, уникальный
  ✓ mine_id, equipment_id — не NULL
  ✓ shift — значения из множества {1, 2}

Бизнес-правила:
  ✓ tonnage_extracted — в диапазоне [0, 500]
  ✓ fe_content_pct — в диапазоне [0, 100]
  ✓ moisture_pct — в диапазоне [0, 100]
  ✓ status — значения из множества {Завершена, Прервана}

Временные проверки:
  ✓ production_date — не NULL, не в будущем
```

### Шаг 3.6. Изучение правил валидации — downtime_events.csv

```
Структурные проверки:
  ✓ event_id — не NULL, уникальный
  ✓ equipment_id, event_type — не NULL
  ✓ severity — значения из множества {Низкая, Средняя, Высокая, Критическая, Плановое}

Бизнес-правила:
  ✓ duration_minutes — в диапазоне [1, 1440] (от 1 мин до 24 часов)
  ✓ end_time > start_time (окончание после начала)
```

### Шаг 3.7. Анализ результатов

После запуска скрипта вы увидите сводку:

```
╔══════════════════════════════╗
║   Результаты валидации GX   ║
╠══════════════════════════════╣
║ equipment.csv:        15/15 PASS  ║
║ sensor_readings.csv:  12/12 PASS  ║
║ ore_production.csv:   14/14 PASS  ║
║ downtime_events.csv:  10/10 PASS  ║
╚══════════════════════════════╝
```

> **Вопрос:** Все проверки прошли успешно. Означает ли это, что данные «идеальные»? Какие проверки NOT было сделано?

---

## Часть 4. Работа с «грязными» данными (15 мин)

### Шаг 4.1. Порча данных

Скрипт `03_great_expectations.py` автоматически создаёт **«грязную» копию** файла `ore_production.csv` с типичными проблемами качества:

- NULL в обязательных полях
- Значения вне допустимого диапазона (fe_content_pct = 150%)
- Дубликаты production_id
- Некорректный статус («Неизвестно»)
- Тоннаж = -10 (отрицательное значение)

### Шаг 4.2. Валидация грязных данных

Скрипт прогоняет тот же набор ожиданий по грязному файлу и выводит:

```
╔════════════════════════════════════════╗
║  Валидация ГРЯЗНЫХ данных:  7/14 FAIL ║
╠════════════════════════════════════════╣
║ ✗ production_id: найдены дубликаты    ║
║ ✗ mine_id: найдены NULL               ║
║ ✗ tonnage_extracted: вне диапазона    ║
║ ✗ fe_content_pct: вне диапазона       ║
║ ✗ status: недопустимое значение       ║
║ ...                                    ║
╚════════════════════════════════════════╝
```

### Шаг 4.3. Анализ

> **Вопрос 1:** Какие проверки «поймали» ошибки? Какие пропустили?
>
> **Вопрос 2:** Как бы вы интегрировали Great Expectations в ETL-пайплайн «Руда+»? На каком этапе?

---

## Самостоятельные задания

### Задание A. Добавьте свои expectations

Добавьте в скрипт `03_great_expectations.py` дополнительные проверки:

1. Проверьте, что `ore_type` в `ore_production.csv` принимает только значения `{Магнетит, Гематит}`
2. Проверьте, что `manufacturer` в `equipment.csv` не содержит NULL
3. Проверьте, что `reading_timestamp` в `sensor_readings.csv` находится в диапазоне последних 365 дней

<details>
<summary>Подсказка</summary>

```python
# 1. Допустимые значения
batch.expect_column_values_to_be_in_set("ore_type", ["Магнетит", "Гематит"])

# 2. Не NULL
batch.expect_column_values_to_not_be_null("manufacturer")

# 3. Диапазон дат
from datetime import datetime, timedelta
batch.expect_column_values_to_be_between(
    "reading_timestamp",
    min_value=(datetime.now() - timedelta(days=365)).isoformat(),
    max_value=datetime.now().isoformat(),
    parse_strings_as_datetimes=True
)
```
</details>

### Задание B. Профилирование после нормализации

После выполнения практической работы Модуля 2 (нормализация) сравните профили:

1. Создайте отчёт для **денормализованной** таблицы `ore_production` (Модуль 1)
2. Создайте отчёт для **нормализованных** таблиц `ore_production` + `mines` + `operators` (Модуль 2)
3. Сравните: исчезли ли alert'ы о высокой корреляции текстовых полей?

<details>
<summary>Подсказка</summary>

В денормализованной таблице YData-Profiling обычно показывает:
- Высокую корреляцию между `mine_id` и `mine_name` (они дублируют друг друга)
- Высокую кардинальность `operator_name` (текстовое поле вместо FK)

После нормализации эти alert'ы исчезают.
</details>

### Задание C. Собственный Expectation Suite для новой таблицы

Напишите Expectation Suite для таблицы `mines` из Модуля 2 (`Module_2/practice/data/mines.csv`):

- `mine_id` — уникальный, не NULL, формат `MINE-\d+`
- `mine_name` — не NULL, уникальный
- `region` — значения из допустимого множества
- `max_depth_m` — в диапазоне [100, 2000]
- Таблица содержит ≥ 2 строк

<details>
<summary>Подсказка</summary>

```python
import great_expectations as gx
import pandas as pd

df = pd.read_csv("../../Module_2/practice/data/mines.csv")
context = gx.get_context()
ds = context.sources.add_pandas("mines")
asset = ds.add_dataframe_asset("mines_asset")
batch = asset.add_batch_definition_whole_dataframe("batch").get_batch(
    batch_parameters={"dataframe": df}
)

suite = context.add_expectation_suite("mines_suite")

batch.expect_column_values_to_not_be_null("mine_id")
batch.expect_column_values_to_be_unique("mine_id")
batch.expect_column_values_to_match_regex("mine_id", r"^MINE-\d+$")
batch.expect_column_values_to_not_be_null("mine_name")
batch.expect_column_values_to_be_between("max_depth_m", min_value=100, max_value=2000)
batch.expect_table_row_count_to_be_between(min_value=2)
```
</details>

---

## Обсуждение

1. **YData-Profiling vs ручной EDA:** В каких ситуациях автоматический отчёт достаточен, а когда нужен ручной анализ?

2. **Great Expectations в production:** Как бы вы настроили GX для автоматической проверки ежедневных данных с датчиков (86 400 показаний в день)?

3. **Data Profiling и моделирование:** Как результаты профилирования влияют на выбор типов данных, ограничений и индексов при проектировании физической модели?

4. **Качество vs полнота:** Что опаснее для MES-системы «Руда+» — пропуски в данных (NULL) или некорректные значения (тоннаж = -10)?

---

## Дополнительные ресурсы

- [YData-Profiling документация](https://docs.profiling.ydata.ai/)
- [Great Expectations документация](https://docs.greatexpectations.io/)
- [Data Quality — Habr](https://habr.com/ru/companies/unidata/articles/667636/)
