# Приложение 2. Модели данных: Key-Value и документоориентированные СУБД

## Практические упражнения

**Предприятие:** «Руда+» — добыча железной руды
**Контекст:** MES-система для анализа качества руды, простоев оборудования и эффективности процесса
**Среда:** Yandex StoreDoc (MongoDB-совместимая СУБД), JetBrains DataGrip

---

## Структура файлов

```
Appendix_2/
├── nosql_modeling.html               # Презентация (Приложение 2)
└── practice/
    ├── README.md                      # Этот файл — инструкции
    ├── data/
    │   ├── equipment.json             # Паспорта оборудования (12 документов)
    │   ├── mines.json                 # Шахты (3 документа)
    │   ├── incidents.json             # Инциденты (10 документов)
    │   └── telemetry_buckets.json     # Телеметрия в формате Bucket Pattern
    └── scripts/
        ├── 01_create_collections.js   # Создание коллекций с Schema Validation
        ├── 02_load_data.js            # Загрузка данных
        ├── 03_crud_queries.js         # CRUD-операции
        ├── 04_aggregation.js          # Aggregation Pipeline
        ├── 05_indexes.js              # Индексы и производительность
        └── 06_patterns.js             # Паттерны проектирования
```

---

## Предварительные требования

### 1. Yandex StoreDoc (кластер)

Yandex StoreDoc — это MongoDB-совместимая NoSQL СУБД в Яндекс Облаке. Поддерживает API MongoDB, включая CRUD, Aggregation Pipeline, индексы и Schema Validation.

**Создание кластера:**

1. Откройте [Консоль Яндекс Облака](https://console.yandex.cloud/)
2. Перейдите в раздел **Yandex StoreDoc**
3. Нажмите **Создать кластер**
4. Настройте параметры:

| Параметр | Значение |
|----------|----------|
| Имя кластера | `ruda-plus-nosql` |
| Окружение | `PRODUCTION` (или `PRESTABLE` для учебных целей) |
| Версия | `7.0` |
| Класс хоста | `s3-c2-m8` (2 vCPU, 8 ГБ RAM) — минимальный |
| Размер диска | 10 ГБ SSD |
| Имя БД | `ruda_plus` |
| Имя пользователя | `student` |
| Пароль | *(задайте надёжный пароль)* |
| Публичный доступ | **Включён** (для подключения из DataGrip) |

5. Нажмите **Создать кластер** и дождитесь статуса `RUNNING` (3-5 минут)

### 2. SSL-сертификат

Yandex StoreDoc требует SSL-соединение. Скачайте корневой сертификат:

**Linux / macOS:**
```bash
mkdir -p ~/.mongodb && \
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
     -O ~/.mongodb/root.crt
```

**Windows (PowerShell):**
```powershell
mkdir $env:USERPROFILE\.mongodb -Force
Invoke-WebRequest -Uri "https://storage.yandexcloud.net/cloud-certs/CA.pem" `
  -OutFile "$env:USERPROFILE\.mongodb\root.crt"
```

### 3. JetBrains DataGrip

DataGrip — универсальная IDE для баз данных, включая MongoDB.

**Установка:**
1. Скачайте DataGrip: [jetbrains.com/datagrip/download](https://www.jetbrains.com/datagrip/download/)
2. Установите и запустите
3. При первом запуске DataGrip предложит скачать драйвер MongoDB — согласитесь

### 4. Подключение DataGrip к Yandex StoreDoc

#### Шаг 1. Создание Data Source

1. В DataGrip: **File → New → Data Source → MongoDB**
2. Заполните поля:

| Поле | Значение |
|------|----------|
| Name | `Руда+ StoreDoc` |
| Host | FQDN хоста из консоли Яндекс Облака (например, `rc1a-xxx.mdb.yandexcloud.net`) |
| Port | `27018` |
| Authentication | `User & Password` |
| User | `student` |
| Password | *(пароль, заданный при создании кластера)* |
| Database | `ruda_plus` |
| Auth database | `ruda_plus` |

> **Где найти FQDN хоста:** Консоль Яндекс Облака → Yandex StoreDoc → ваш кластер → вкладка **Хосты** → столбец **Имя хоста**

#### Шаг 2. Настройка SSL

1. Перейдите на вкладку **SSH/SSL** в настройках Data Source
2. Поставьте галочку **Use SSL**
3. В поле **CA file** укажите путь к скачанному сертификату:
   - Linux/macOS: `~/.mongodb/root.crt`
   - Windows: `C:\Users\<username>\.mongodb\root.crt`

#### Шаг 3. Проверка подключения

1. Нажмите **Test Connection**
2. Если DataGrip предложит скачать драйвер — нажмите **Download**
3. Ожидаемый результат: зелёная галочка `Successful`

#### Шаг 4. Работа с MongoDB в DataGrip

- **Database Explorer** (слева) — навигация по коллекциям, документам, индексам
- **Console** — откройте: ПКМ на базе данных → **New → Console** (или `F4`)
- **Выполнение запросов** — введите JavaScript-команду MongoDB и нажмите `Ctrl+Enter`

> **Важно:** DataGrip поддерживает выполнение JavaScript-команд MongoDB (db.collection.find(), db.collection.aggregate() и т.д.) непосредственно в консоли.

---

## Упражнение 1: Создание коллекций с Schema Validation

**Время:** 20 минут
**Цель:** создать коллекции с правилами валидации документов

### Шаг 1. Откройте консоль MongoDB в DataGrip

1. В Database Explorer раскройте подключение `Руда+ StoreDoc`
2. ПКМ на базе данных `ruda_plus` → **New → Console**

### Шаг 2. Создайте коллекции

Скопируйте и выполните скрипт `scripts/01_create_collections.js` в консоли DataGrip.

Скрипт создаст 4 коллекции:

| Коллекция | Schema Validation | Описание |
|-----------|-------------------|----------|
| `equipment` | Строгая ($jsonSchema) | Паспорта оборудования (Polymorphic Pattern) |
| `mines` | Строгая | Шахты с горизонтами (Embedding) |
| `incidents` | Moderate / warn | Журнал инцидентов (свободная форма) |
| `telemetry_buckets` | Строгая | Телеметрия (Bucket Pattern) |

### Шаг 3. Проверьте валидацию

Попробуйте вставить невалидный документ:

```javascript
// Этот INSERT должен завершиться ошибкой!
db.equipment.insertOne({
  _id: "INVALID",           // нарушает паттерн ^EQ\d{3}$
  type: "Бульдозер",        // нет в enum
  status: "ok"              // нет в enum
});
```

Ожидаемый результат: ошибка валидации `Document failed validation`.

### Шаг 4. Вставьте валидный документ

```javascript
db.equipment.insertOne({
  _id: "EQ099",
  type: "ПДМ",
  model: "ST18",
  manufacturer: "Atlas Copco",
  status: "working",
  mine: { _id: "M001", name: "Северная" },
  engine_hours: 0
});

// Проверка
db.equipment.findOne({ _id: "EQ099" });
```

### Шаг 5. Самостоятельное задание

Добавьте в Schema Validation коллекции `equipment`:
- Поле `year_manufactured` — целое число, диапазон 2000–2030
- Поле `max_payload_tons` — число > 0

---

## Упражнение 2: Загрузка данных и CRUD

**Время:** 25 минут
**Цель:** загрузить тестовые данные «Руда+» и выполнить CRUD-операции

### Шаг 1. Загрузите данные

Выполните скрипт `scripts/02_load_data.js` в консоли DataGrip.

Проверьте загрузку:

```javascript
print("equipment:", db.equipment.countDocuments());
print("mines:", db.mines.countDocuments());
print("incidents:", db.incidents.countDocuments());
print("telemetry_buckets:", db.telemetry_buckets.countDocuments());
```

Ожидаемый результат:
```
equipment: 12
mines: 3
incidents: 10
telemetry_buckets: 6
```

### Шаг 2. Выполните CRUD-запросы

Откройте скрипт `scripts/03_crud_queries.js` и выполняйте запросы пошагово.

#### Часть 1: Чтение (find)

```javascript
// Все ПДМ на шахте "Северная"
db.equipment.find({
  type: "ПДМ",
  "mine.name": "Северная"
});

// Оборудование с наработкой > 10 000 моточасов
db.equipment.find({
  engine_hours: { $gt: 10000 }
}).sort({ engine_hours: -1 });

// Только имя и статус (проекция)
db.equipment.find(
  { status: "working" },
  { _id: 1, model: 1, status: 1, engine_hours: 1 }
);
```

**Задание:** Найдите все оборудование типа «Самосвал» с наработкой от 5 000 до 15 000 моточасов.

#### Часть 2: Обновление (update)

```javascript
// Перевести оборудование в ремонт
db.equipment.updateOne(
  { _id: "EQ003" },
  { $set: { status: "maintenance" } }
);

// Увеличить наработку атомарно
db.equipment.updateOne(
  { _id: "EQ001" },
  { $inc: { engine_hours: 8.5 } }
);

// Добавить запись ТО в массив maintenance
db.equipment.updateOne(
  { _id: "EQ001" },
  { $push: {
      "maintenance.history": {
        date: new Date("2025-02-15"),
        type: "ТО-3",
        cost: 65000,
        description: "Замена гидронасоса"
      }
  }}
);
```

**Задание:** Обновите координаты оборудования EQ005: latitude=56.84, longitude=60.61, level=-200.

#### Часть 3: Удаление (delete)

```javascript
// Удалить тестовый документ
db.equipment.deleteOne({ _id: "EQ099" });

// Подсчитать оставшиеся
db.equipment.countDocuments();
```

---

## Упражнение 3: Aggregation Pipeline

**Время:** 30 минут
**Цель:** написать аналитические запросы с использованием Aggregation Pipeline

### Шаг 1. Базовые агрегации

Откройте скрипт `scripts/04_aggregation.js` и выполняйте пошагово.

```javascript
// Количество оборудования по типам
db.equipment.aggregate([
  { $group: {
      _id: "$type",
      count: { $sum: 1 },
      avg_hours: { $avg: "$engine_hours" }
  }},
  { $sort: { count: -1 } }
]);
```

### Шаг 2. Агрегация с $lookup (JOIN)

```javascript
// Инциденты с информацией об оборудовании
db.incidents.aggregate([
  { $match: { severity: "critical" } },
  { $lookup: {
      from: "equipment",
      localField: "equipment_id",
      foreignField: "_id",
      as: "equip"
  }},
  { $unwind: { path: "$equip", preserveNullAndEmptyArrays: true } },
  { $project: {
      incident_date: 1,
      description: 1,
      severity: 1,
      "equip.model": 1,
      "equip.type": 1
  }}
]);
```

### Шаг 3. Аналитика по телеметрии (Bucket Pattern)

```javascript
// Средняя температура по датчикам за период
db.telemetry_buckets.aggregate([
  { $match: {
      bucket_start: { $gte: ISODate("2025-01-15T00:00:00Z") }
  }},
  { $group: {
      _id: "$sensor_id",
      avg_temp: { $avg: "$stats.avg_temp" },
      max_temp: { $max: "$stats.max_temp" },
      total_readings: { $sum: "$count" }
  }},
  { $sort: { max_temp: -1 } }
]);
```

### Шаг 4. Самостоятельные задания

1. Напишите Pipeline: **Количество инцидентов по шахтам и месяцам** ($match → $group по mine_name и месяцу → $sort)
2. Напишите Pipeline: **Топ-3 оборудования по средней наработке** ($group → $sort → $limit)
3. Напишите Pipeline: **Распределение инцидентов по severity** (critical / warning / info) с процентным соотношением

---

## Упражнение 4: Индексы и производительность

**Время:** 15 минут
**Цель:** создать индексы и сравнить планы выполнения

### Шаг 1. Сравните запрос до и после индекса

```javascript
// Без индекса — explain покажет COLLSCAN
db.equipment.find({ status: "working" }).explain("executionStats");

// Создаём индекс
db.equipment.createIndex({ status: 1 });

// С индексом — explain покажет IXSCAN
db.equipment.find({ status: "working" }).explain("executionStats");
```

**Задание:** зафиксируйте значения `totalDocsExamined` и `executionTimeMillis` до и после создания индекса.

### Шаг 2. Составной индекс

```javascript
// Составной индекс для частого запроса
db.equipment.createIndex({ "mine._id": 1, status: 1 });

// Проверка
db.equipment.find({
  "mine._id": "M001",
  status: "working"
}).explain("executionStats");
```

### Шаг 3. TTL-индекс для телеметрии

```javascript
// Автоудаление документов телеметрии старше 30 дней
db.telemetry_buckets.createIndex(
  { bucket_end: 1 },
  { expireAfterSeconds: 2592000 }  // 30 дней
);
```

### Шаг 4. Текстовый индекс для инцидентов

```javascript
// Полнотекстовый поиск по описанию инцидентов
db.incidents.createIndex({ description: "text" });

// Поиск
db.incidents.find({ $text: { $search: "перегрев двигатель" } });
```

---

## Упражнение 5: Паттерны проектирования

**Время:** 25 минут
**Цель:** применить паттерны Polymorphic, Bucket, Computed, Extended Reference

### Шаг 1. Polymorphic Pattern

Изучите документы в коллекции `equipment` — разные типы оборудования имеют разные поля:

```javascript
// ПДМ имеет bucket_volume_m3
db.equipment.findOne({ type: "ПДМ" });

// Самосвал имеет bed_capacity_tons
db.equipment.findOne({ type: "Самосвал" });

// Скиповый подъёмник имеет lift_height_m
db.equipment.findOne({ type: "Скип" });
```

**Задание:** Добавьте новый тип оборудования — «Буровая установка» со специфичными полями: `drill_depth_m` (глубина бурения), `drill_diameter_mm` (диаметр).

### Шаг 2. Bucket Pattern — работа с телеметрией

```javascript
// Изучите структуру bucket-документа
db.telemetry_buckets.findOne();

// Добавьте новое показание в bucket
db.telemetry_buckets.updateOne(
  {
    sensor_id: "SNS001",
    count: { $lt: 1000 }  // bucket не полон
  },
  {
    $push: { readings: { ts: new Date(), temp: 44.2, vibr: 3.5 } },
    $inc: { count: 1 },
    $max: { "stats.max_temp": 44.2 },
    $min: { "stats.min_temp": 44.2 },
    $set: { bucket_end: new Date() }
  },
  { upsert: true }
);
```

### Шаг 3. Computed Pattern — предвычисленные агрегаты

```javascript
// Создание коллекции shift_stats
db.createCollection("shift_stats");

// Запись тоннажа за смену (атомарное обновление)
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 42.5, trip_count: 1 },
    $max: { max_tonnage_per_trip: 42.5 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

// Ещё один рейс
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 38.0, trip_count: 1 },
    $max: { max_tonnage_per_trip: 42.5 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

// Мгновенное чтение — без Aggregation Pipeline
db.shift_stats.findOne({ mine_id: "M001", date: "2025-01-15", shift: 2 });
```

### Шаг 4. Самостоятельное задание

Создайте коллекцию `daily_stats` с предвычисленными агрегатами по шахтам:
- Поля: `mine_id`, `date`, `total_tonnage`, `avg_fe_pct`, `incident_count`, `equipment_active`
- Напишите updateOne с $inc и $set для обновления при поступлении новых данных
- Продемонстрируйте, что чтение — один запрос без Pipeline

---

## Критерии оценки

| Критерий | Баллы |
|----------|-------|
| Коллекции с Schema Validation созданы корректно | 15 |
| Тестовые данные загружены, CRUD работает | 15 |
| Aggregation Pipeline запросы написаны правильно | 20 |
| Индексы созданы, explain показывает IXSCAN | 10 |
| Паттерны (Polymorphic, Bucket, Computed) применены | 25 |
| Самостоятельные задания выполнены | 15 |
| **Итого** | **100** |

---

## Полезные ссылки

- [Yandex StoreDoc — документация](https://yandex.cloud/ru/docs/storedoc/) — управление кластером, подключение
- [MongoDB Manual — Data Modeling](https://www.mongodb.com/docs/manual/core/data-modeling-introduction/) — официальная документация
- [Building with Patterns (MongoDB Blog)](https://www.mongodb.com/blog/post/building-with-patterns-a-summary) — 12 паттернов проектирования
- [MongoDB Aggregation Pipeline](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/) — агрегации
- [MongoDB Schema Validation](https://www.mongodb.com/docs/manual/core/schema-validation/) — валидация схемы
- [DataGrip: MongoDB](https://www.jetbrains.com/help/datagrip/mongodb.html) — работа с MongoDB в DataGrip
