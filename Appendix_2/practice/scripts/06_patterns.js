// ============================================================
// Скрипт 06: Паттерны проектирования документной модели
// Предприятие «Руда+» — MES-система
// Среда: Yandex StoreDoc (MongoDB-совместимая СУБД)
// IDE: JetBrains DataGrip
// ============================================================
// Этот скрипт демонстрирует применение паттернов из серии
// "Building with Patterns" (MongoDB Blog) на примере «Руда+»
// ============================================================


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 1: Polymorphic Pattern                           ║
// ║  Разные типы оборудования в одной коллекции              ║
// ╚════════════════════════════════════════════════════════════╝

print("=== Polymorphic Pattern ===");

// Все документы в equipment имеют общие поля (type, model, status, mine, engine_hours),
// но специфичные поля зависят от типа:

// --- ПДМ: bucket_volume_m3 ---
print("\nПДМ (специфичные поля):");
db.equipment.find(
  { type: "ПДМ" },
  { _id: 1, type: 1, model: 1, "specifications.bucket_volume_m3": 1 }
);

// --- Самосвал: bed_capacity_m3, turning_radius_m ---
print("\nСамосвал (специфичные поля):");
db.equipment.find(
  { type: "Самосвал" },
  { _id: 1, type: 1, model: 1, "specifications.bed_capacity_m3": 1, "specifications.turning_radius_m": 1 }
);

// --- Скип: lift_height_m, rope_diameter_mm, motor_power_kw ---
print("\nСкип (специфичные поля):");
db.equipment.find(
  { type: "Скип" },
  { _id: 1, type: 1, model: 1, "specifications.lift_height_m": 1, "specifications.rope_diameter_mm": 1 }
);

// --- Вагонетка: track_gauge_mm, assigned_route ---
print("\nВагонетка (специфичные поля):");
db.equipment.find(
  { type: "Вагонетка" },
  { _id: 1, type: 1, model: 1, "specifications.track_gauge_mm": 1, assigned_route: 1 }
);

// Универсальный запрос по общим полям — работает для ВСЕХ типов:
print("\nУниверсальный запрос (все типы, общие поля):");
db.equipment.find(
  {},
  { _id: 1, type: 1, model: 1, status: 1, "mine.name": 1, engine_hours: 1 }
);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЗАДАНИЕ 1 (самостоятельно):                              ║
// ║  Добавьте новый тип оборудования — «Буровая установка»   ║
// ║  со специфичными полями:                                  ║
// ║    drill_depth_m (глубина бурения)                        ║
// ║    drill_diameter_mm (диаметр сверла)                     ║
// ║    rotation_speed_rpm (скорость вращения)                 ║
// ╚════════════════════════════════════════════════════════════╝

// Подсказка: сначала обновите validator коллекции, добавив
// "Буровая" в enum типов, затем вставьте документ.

// Шаг 1: Обновление валидатора (добавить "Буровая" в enum)
// db.runCommand({
//   collMod: "equipment",
//   validator: {
//     $jsonSchema: {
//       bsonType: "object",
//       required: ["_id", "type", "model", "manufacturer", "status", "mine"],
//       properties: {
//         _id: { bsonType: "string", pattern: "^EQ\\d{3}$" },
//         type: {
//           bsonType: "string",
//           enum: ["ПДМ", "Самосвал", "Вагонетка", "Скип", "Буровая"]
//         },
//         // ... остальные поля как в 01_create_collections.js
//       }
//     }
//   }
// });

// Шаг 2: Вставка документа
// db.equipment.insertOne({
//   _id: "EQ013",
//   type: "Буровая",
//   model: "???",
//   manufacturer: "???",
//   status: "working",
//   mine: { _id: "M001", name: "Северная" },
//   engine_hours: 0,
//   specifications: {
//     drill_depth_m: ???,
//     drill_diameter_mm: ???,
//     rotation_speed_rpm: ???,
//     engine_power_kw: ???
//   }
// });


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 2: Bucket Pattern                                ║
// ║  Группировка показаний телеметрии во временные окна       ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Bucket Pattern ===");

// Вместо 1 документ = 1 показание (43 200 документов/день при 60 датчиках × 1/мин),
// мы группируем: 1 документ = 1 час показаний (720 документов/день)

// --- Структура бакета ---
print("\nСтруктура bucket-документа:");
db.telemetry_buckets.findOne({ _id: "TB001" });

// --- Добавление нового показания в бакет ---
// Атомарная операция: push + обновление агрегатов одним запросом
print("\nДобавление нового показания:");
db.telemetry_buckets.updateOne(
  {
    sensor_id: "SNS001",
    equipment_id: "EQ001",
    count: { $lt: 1000 }    // бакет не переполнен
  },
  {
    $push: {
      readings: {
        ts: new Date().toISOString(),
        temp: 44.2
      }
    },
    $inc: { count: 1 },
    $max: { "stats.max_temp": 44.2 },
    $min: { "stats.min_temp": 44.2 },
    $set: { bucket_end: new Date().toISOString() }
  },
  { upsert: true }
);

// Проверка
print("Обновлённый бакет:");
db.telemetry_buckets.findOne({ sensor_id: "SNS001", equipment_id: "EQ001" },
  { count: 1, "stats": 1, bucket_end: 1 });

// --- Аналитика по бакетам ---
print("\nСводка по датчикам (из предвычисленных stats):");
db.telemetry_buckets.aggregate([
  { $group: {
      _id: { sensor: "$sensor_id", type: "$sensor_type" },
      buckets: { $sum: 1 },
      total_readings: { $sum: "$count" },
      overall_max: { $max: { $ifNull: ["$stats.max_temp", "$stats.max_vibr"] } },
      overall_min: { $min: { $ifNull: ["$stats.min_temp", "$stats.min_vibr"] } }
  }}
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 3: Computed Pattern                              ║
// ║  Предвычисленные агрегаты для мгновенного чтения         ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Computed Pattern ===");

// Создаём коллекцию для предвычисленных сменных показателей
db.shift_stats.drop();
db.createCollection("shift_stats");

// --- Запись тоннажа за смену (атомарные обновления) ---
// Каждый рейс ПДМ/самосвала атомарно обновляет агрегаты:

print("\nСмена 2, Шахта Северная — рейс 1:");
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 42.5, trip_count: 1 },
    $max: { max_tonnage_per_trip: 42.5 },
    $set: {
      last_updated: new Date(),
      mine_name: "Северная"
    }
  },
  { upsert: true }
);

print("Рейс 2:");
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 38.0, trip_count: 1 },
    $max: { max_tonnage_per_trip: 42.5 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

print("Рейс 3:");
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 45.2, trip_count: 1 },
    $max: { max_tonnage_per_trip: 45.2 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

print("Рейс 4:");
db.shift_stats.updateOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 39.8, trip_count: 1 },
    $max: { max_tonnage_per_trip: 45.2 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

// --- Мгновенное чтение — один запрос, без Aggregation Pipeline ---
print("\nРезультат (мгновенное чтение без Pipeline):");
db.shift_stats.findOne(
  { mine_id: "M001", date: "2025-01-15", shift: 2 }
);
// Результат: total_tonnage = 165.5, trip_count = 4, max = 45.2

// --- Добавим данные по другой шахте ---
print("\nСмена 2, Шахта Центральная:");
db.shift_stats.updateOne(
  { mine_id: "M002", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 51.3, trip_count: 1 },
    $max: { max_tonnage_per_trip: 51.3 },
    $set: { last_updated: new Date(), mine_name: "Центральная" }
  },
  { upsert: true }
);

db.shift_stats.updateOne(
  { mine_id: "M002", date: "2025-01-15", shift: 2 },
  {
    $inc: { total_tonnage: 48.7, trip_count: 1 },
    $max: { max_tonnage_per_trip: 51.3 },
    $set: { last_updated: new Date() }
  },
  { upsert: true }
);

// --- Сводка по всем шахтам ---
print("\nСводка shift_stats:");
db.shift_stats.find();


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 4: Extended Reference Pattern                    ║
// ║  Встроенная ссылка с дублированием часто читаемых полей  ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Extended Reference Pattern ===");

// В каждом документе equipment поле mine содержит не только _id шахты,
// но и её название — это Extended Reference:
//   mine: { _id: "M001", name: "Северная" }
// Это позволяет отображать название шахты без $lookup.

print("\nОборудование с Extended Reference на шахту:");
db.equipment.find(
  {},
  { _id: 1, type: 1, "mine._id": 1, "mine.name": 1 }
);

// Аналогично в incidents: mine_name + mine_id
print("\nИнциденты с Extended Reference:");
db.incidents.find(
  {},
  { _id: 1, type: 1, mine_id: 1, mine_name: 1 }
);

// ВАЖНО: при переименовании шахты нужно обновить все ссылки!
// Это компромисс Extended Reference: быстрое чтение ↔ консистентность при обновлении

// Пример обновления (НЕ выполняйте — для демонстрации):
// Если шахту M001 переименовали из "Северная" в "Северная-1":
// db.equipment.updateMany(
//   { "mine._id": "M001" },
//   { $set: { "mine.name": "Северная-1" } }
// );
// db.incidents.updateMany(
//   { mine_id: "M001" },
//   { $set: { mine_name: "Северная-1" } }
// );


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 5: Attribute Pattern                             ║
// ║  Однородные пары ключ-значение в массиве                 ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Attribute Pattern ===");

// Допустим, мы хотим хранить произвольные метрики оборудования,
// которые различаются между моделями. Вместо разных полей —
// массив {key, value, unit}:

print("\nДобавляем metrics к EQ001 (Attribute Pattern):");
db.equipment.updateOne(
  { _id: "EQ001" },
  { $set: {
      metrics: [
        { key: "fuel_consumption", value: 28.5, unit: "л/час" },
        { key: "hydraulic_pressure", value: 250, unit: "бар" },
        { key: "cabin_noise", value: 82, unit: "дБ" },
        { key: "tire_pressure_front", value: 4.5, unit: "бар" },
        { key: "tire_pressure_rear", value: 5.0, unit: "бар" }
      ]
  }}
);

// Поиск по конкретной метрике
print("\nОборудование с расходом топлива > 25 л/час:");
db.equipment.find(
  {
    metrics: {
      $elemMatch: { key: "fuel_consumption", value: { $gt: 25 } }
    }
  },
  { _id: 1, model: 1, metrics: 1 }
);

// Индекс для Attribute Pattern
db.equipment.createIndex({ "metrics.key": 1, "metrics.value": 1 });
print("✓ Индекс {metrics.key: 1, metrics.value: 1} создан.");


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 6: Subset Pattern                                ║
// ║  Хранение подмножества данных в основном документе        ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Subset Pattern ===");

// В equipment.maintenance.history хранятся только последние записи ТО.
// Полная история обслуживания — в отдельной коллекции maintenance_log.

// Создаём коллекцию с полной историей
db.maintenance_log.drop();
db.createCollection("maintenance_log");

db.maintenance_log.insertMany([
  {
    _id: "ML001", equipment_id: "EQ001",
    date: "2021-06-15", type: "Ввод в эксплуатацию", cost: 0,
    description: "Первый запуск, обкатка", technician: "Иванов А.П."
  },
  {
    _id: "ML002", equipment_id: "EQ001",
    date: "2021-12-10", type: "ТО-1", cost: 18000,
    description: "Первое плановое ТО", technician: "Петров Б.В."
  },
  {
    _id: "ML003", equipment_id: "EQ001",
    date: "2022-06-20", type: "ТО-1", cost: 20000,
    description: "Плановое ТО, замена воздушных фильтров", technician: "Петров Б.В."
  },
  {
    _id: "ML004", equipment_id: "EQ001",
    date: "2023-01-15", type: "ТО-2", cost: 48000,
    description: "Замена гидравлического масла и фильтров", technician: "Козлов И.С."
  },
  {
    _id: "ML005", equipment_id: "EQ001",
    date: "2024-07-20", type: "ТО-3", cost: 120000,
    description: "Капитальный ремонт двигателя", technician: "Сидоров В.Г."
  },
  {
    _id: "ML006", equipment_id: "EQ001",
    date: "2025-01-10", type: "ТО-2", cost: 45000,
    description: "Замена фильтров, проверка гидравлики", technician: "Козлов И.С."
  }
]);

print("✓ maintenance_log: " + db.maintenance_log.countDocuments() + " записей загружено.");

// В основном документе (equipment) — только 2 последние записи (Subset)
// В maintenance_log — полная история

// Быстрое чтение последних ТО — из equipment (без JOIN)
print("\nПоследние ТО (из основного документа — Subset):");
db.equipment.findOne(
  { _id: "EQ001" },
  { "maintenance.history": 1 }
);

// Полная история — из maintenance_log ($lookup)
print("\nПолная история ТО (из maintenance_log):");
db.maintenance_log.find({ equipment_id: "EQ001" }).sort({ date: 1 });


// ╔════════════════════════════════════════════════════════════╗
// ║  ПАТТЕРН 7: Schema Versioning Pattern                     ║
// ║  Управление версиями схемы документов                     ║
// ╚════════════════════════════════════════════════════════════╝

print("\n\n=== Schema Versioning Pattern ===");

// При эволюции схемы вместо миграции всех документов добавляем
// поле schema_version и обрабатываем разные версии в коде.

// Версия 1: координаты как отдельные поля
var v1 = {
  _id: "EQ_V1_DEMO",
  schema_version: 1,
  type: "ПДМ",
  model: "Demo",
  manufacturer: "Demo",
  status: "working",
  mine: { _id: "M001", name: "Северная" },
  latitude: 56.8431,
  longitude: 60.6454,
  level: -320
};

// Версия 2: координаты как GeoJSON (текущая схема)
var v2 = {
  _id: "EQ_V2_DEMO",
  schema_version: 2,
  type: "ПДМ",
  model: "Demo",
  manufacturer: "Demo",
  status: "working",
  mine: { _id: "M001", name: "Северная" },
  location: {
    type: "Point",
    coordinates: [60.6454, 56.8431],
    level: -320
  }
};

print("Версия 1 (старая схема):");
printjson(v1);
print("\nВерсия 2 (новая схема GeoJSON):");
printjson(v2);

// Функция, обрабатывающая обе версии:
print("\nФункция getLocation() для обеих версий:");
print("  function getLocation(doc) {");
print("    if (doc.schema_version === 1) {");
print("      return { lat: doc.latitude, lng: doc.longitude, lvl: doc.level };");
print("    } else {");
print("      return {");
print("        lat: doc.location.coordinates[1],");
print("        lng: doc.location.coordinates[0],");
print("        lvl: doc.location.level");
print("      };");
print("    }");
print("  }");


// ╔════════════════════════════════════════════════════════════╗
// ║  САМОСТОЯТЕЛЬНОЕ ЗАДАНИЕ                                  ║
// ╚════════════════════════════════════════════════════════════╝

// ЗАДАНИЕ 2:
// Создайте коллекцию daily_stats (Computed Pattern)
// с предвычисленными агрегатами по шахтам:
//
//   Поля:
//   - mine_id (string)
//   - date (string, ISO)
//   - total_tonnage (число, $inc)
//   - avg_fe_pct (число, последнее значение $set)
//   - incident_count (число, $inc)
//   - equipment_active (число, $set)
//
// 1. Создайте коллекцию
// 2. Напишите 3 updateOne с $inc/$set для имитации поступления данных
// 3. Покажите, что итоговый документ читается одним findOne

// Ваш код:



// ЗАДАНИЕ 3:
// Примените Subset Pattern к коллекции incidents:
// - Добавьте в документ шахты (mines) поле recent_incidents[]
//   с последними 3 инцидентами (только _id, type, severity, incident_date)
// - Убедитесь, что при добавлении нового инцидента в incidents
//   старые вытесняются из recent_incidents (используйте $push + $slice)

// Подсказка:
// db.mines.updateOne(
//   { _id: "M001" },
//   { $push: {
//       recent_incidents: {
//         $each: [{ _id: "INC_NEW", type: "sensor_alert", severity: "info", incident_date: "..." }],
//         $sort: { incident_date: -1 },
//         $slice: 3
//       }
//   }}
// );

// Ваш код:



print("\n✓ Скрипт 06 выполнен. Паттерны проектирования продемонстрированы.");
