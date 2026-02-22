// ============================================================
// Скрипт 04: Aggregation Pipeline
// Предприятие «Руда+» — MES-система
// Среда: Yandex StoreDoc (MongoDB-совместимая СУБД)
// IDE: JetBrains DataGrip
// ============================================================
// Выполняйте pipeline'ы по одному (выделите запрос → Ctrl+Enter)
// ============================================================


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 1: Базовые агрегации                               ║
// ╚════════════════════════════════════════════════════════════╝

// --- 1.1. Количество оборудования по типам ---
db.equipment.aggregate([
  { $group: {
      _id: "$type",
      count: { $sum: 1 },
      avg_hours: { $avg: "$engine_hours" }
  }},
  { $sort: { count: -1 } }
]);

// --- 1.2. Количество оборудования по шахтам ---
db.equipment.aggregate([
  { $group: {
      _id: "$mine.name",
      count: { $sum: 1 },
      total_hours: { $sum: "$engine_hours" }
  }},
  { $sort: { count: -1 } }
]);

// --- 1.3. Средняя наработка по типу и шахте ---
db.equipment.aggregate([
  { $group: {
      _id: { type: "$type", mine: "$mine.name" },
      count: { $sum: 1 },
      avg_hours: { $avg: "$engine_hours" },
      max_hours: { $max: "$engine_hours" }
  }},
  { $sort: { "_id.type": 1, "_id.mine": 1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 2: $match + $group — фильтрация перед агрегацией   ║
// ╚════════════════════════════════════════════════════════════╝

// --- 2.1. Только работающее оборудование ---
db.equipment.aggregate([
  { $match: { status: "working" } },
  { $group: {
      _id: "$type",
      count: { $sum: 1 },
      avg_hours: { $avg: "$engine_hours" }
  }},
  { $sort: { avg_hours: -1 } }
]);

// --- 2.2. Оборудование с наработкой > 10 000 ---
db.equipment.aggregate([
  { $match: { engine_hours: { $gt: 10000 } } },
  { $project: {
      _id: 1,
      type: 1,
      model: 1,
      engine_hours: 1,
      mine: "$mine.name"
  }},
  { $sort: { engine_hours: -1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 3: $unwind — разворачивание массивов               ║
// ╚════════════════════════════════════════════════════════════╝

// --- 3.1. Все датчики всего оборудования ---
db.equipment.aggregate([
  { $unwind: "$sensors" },
  { $project: {
      equipment_id: "$_id",
      equipment_type: "$type",
      sensor_id: "$sensors.sensor_id",
      sensor_type: "$sensors.type",
      sensor_location: "$sensors.location"
  }}
]);

// --- 3.2. Количество датчиков по типу ---
db.equipment.aggregate([
  { $unwind: "$sensors" },
  { $group: {
      _id: "$sensors.type",
      count: { $sum: 1 }
  }},
  { $sort: { count: -1 } }
]);

// --- 3.3. Среднее количество датчиков на единицу оборудования ---
db.equipment.aggregate([
  { $project: {
      type: 1,
      sensor_count: { $size: { $ifNull: ["$sensors", []] } }
  }},
  { $group: {
      _id: "$type",
      avg_sensors: { $avg: "$sensor_count" },
      max_sensors: { $max: "$sensor_count" }
  }},
  { $sort: { avg_sensors: -1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 4: $lookup — соединение коллекций (JOIN)           ║
// ╚════════════════════════════════════════════════════════════╝

// --- 4.1. Инциденты с информацией об оборудовании ---
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
      equipment_id: 1,
      "equip.model": 1,
      "equip.type": 1,
      downtime_hours: 1,
      repair_cost: 1
  }}
]);

// --- 4.2. Инциденты с полной информацией о шахте ---
db.incidents.aggregate([
  { $lookup: {
      from: "mines",
      localField: "mine_id",
      foreignField: "_id",
      as: "mine_info"
  }},
  { $unwind: "$mine_info" },
  { $project: {
      type: 1,
      severity: 1,
      description: 1,
      "mine_info.name": 1,
      "mine_info.depth_m": 1,
      "mine_info.daily_target_tons": 1
  }}
]);

// --- 4.3. Оборудование + количество связанных инцидентов ---
db.equipment.aggregate([
  { $lookup: {
      from: "incidents",
      localField: "_id",
      foreignField: "equipment_id",
      as: "related_incidents"
  }},
  { $project: {
      type: 1,
      model: 1,
      status: 1,
      incident_count: { $size: "$related_incidents" },
      critical_incidents: {
        $size: {
          $filter: {
            input: "$related_incidents",
            as: "inc",
            cond: { $eq: ["$$inc.severity", "critical"] }
          }
        }
      }
  }},
  { $sort: { incident_count: -1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 5: Аналитика инцидентов                            ║
// ╚════════════════════════════════════════════════════════════╝

// --- 5.1. Распределение инцидентов по severity ---
db.incidents.aggregate([
  { $group: {
      _id: "$severity",
      count: { $sum: 1 },
      avg_downtime: { $avg: { $ifNull: ["$downtime_hours", 0] } },
      total_cost: { $sum: { $ifNull: ["$repair_cost", 0] } }
  }},
  { $sort: { count: -1 } }
]);

// --- 5.2. Инциденты по типам ---
db.incidents.aggregate([
  { $group: {
      _id: "$type",
      count: { $sum: 1 },
      avg_response_min: { $avg: { $ifNull: ["$response_time_min", 0] } }
  }},
  { $sort: { count: -1 } }
]);

// --- 5.3. Общая стоимость ремонтов по шахтам ---
db.incidents.aggregate([
  { $group: {
      _id: "$mine_name",
      total_cost: { $sum: { $ifNull: ["$repair_cost", 0] } },
      total_downtime: { $sum: { $ifNull: ["$downtime_hours", 0] } },
      incident_count: { $sum: 1 }
  }},
  { $addFields: {
      avg_cost_per_incident: {
        $cond: [
          { $gt: ["$incident_count", 0] },
          { $divide: ["$total_cost", "$incident_count"] },
          0
        ]
      }
  }},
  { $sort: { total_cost: -1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 6: Аналитика телеметрии (Bucket Pattern)           ║
// ╚════════════════════════════════════════════════════════════╝

// --- 6.1. Средняя температура по датчикам ---
db.telemetry_buckets.aggregate([
  { $match: { sensor_type: "temperature" } },
  { $group: {
      _id: {
        sensor: "$sensor_id",
        equipment: "$equipment_id"
      },
      avg_temp: { $avg: "$stats.avg_temp" },
      max_temp: { $max: "$stats.max_temp" },
      min_temp: { $min: "$stats.min_temp" },
      total_readings: { $sum: "$count" }
  }},
  { $sort: { max_temp: -1 } }
]);

// --- 6.2. Датчики с превышением пороговых значений ---
db.telemetry_buckets.aggregate([
  { $match: {
      sensor_type: "temperature",
      "stats.max_temp": { $gt: 90 }
  }},
  { $project: {
      sensor_id: 1,
      equipment_id: 1,
      bucket_start: 1,
      max_temp: "$stats.max_temp",
      avg_temp: "$stats.avg_temp"
  }}
]);

// --- 6.3. Развёртка показаний из бакета ---
db.telemetry_buckets.aggregate([
  { $match: { _id: "TB001" } },
  { $unwind: "$readings" },
  { $project: {
      sensor_id: 1,
      timestamp: "$readings.ts",
      temperature: "$readings.temp"
  }},
  { $sort: { timestamp: 1 } }
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 7: Расширенные возможности                         ║
// ╚════════════════════════════════════════════════════════════╝

// --- 7.1. $facet — несколько агрегаций в одном запросе ---
db.equipment.aggregate([
  { $facet: {
      "by_type": [
        { $group: { _id: "$type", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ],
      "by_status": [
        { $group: { _id: "$status", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ],
      "by_mine": [
        { $group: { _id: "$mine.name", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ]
  }}
]);

// --- 7.2. $bucket — группировка по диапазонам наработки ---
db.equipment.aggregate([
  { $bucket: {
      groupBy: "$engine_hours",
      boundaries: [0, 5000, 10000, 15000, 20000, 30000],
      default: "30000+",
      output: {
        count: { $sum: 1 },
        equipment: { $push: { id: "$_id", model: "$model", hours: "$engine_hours" } }
      }
  }}
]);

// --- 7.3. Стоимость обслуживания — развёртка истории ТО ---
db.equipment.aggregate([
  { $unwind: "$maintenance.history" },
  { $group: {
      _id: { id: "$_id", type: "$type", model: "$model" },
      total_cost: { $sum: "$maintenance.history.cost" },
      service_count: { $sum: 1 },
      last_service_type: { $last: "$maintenance.history.type" }
  }},
  { $sort: { total_cost: -1 } },
  { $project: {
      equipment: "$_id.id",
      type: "$_id.type",
      model: "$_id.model",
      total_cost: 1,
      service_count: 1,
      last_service_type: 1,
      _id: 0
  }}
]);


// ╔════════════════════════════════════════════════════════════╗
// ║  САМОСТОЯТЕЛЬНЫЕ ЗАДАНИЯ                                  ║
// ╚════════════════════════════════════════════════════════════╝

// ЗАДАНИЕ 1:
// Напишите Pipeline: количество инцидентов по шахтам и месяцам
// Подсказка: $match → $group (по mine_name и подстроке месяца из incident_date) → $sort

// Ваш Pipeline:



// ЗАДАНИЕ 2:
// Напишите Pipeline: Топ-3 оборудования по средней наработке на тип
// Подсказка: $group (по type) → $sort → $limit

// Ваш Pipeline:



// ЗАДАНИЕ 3:
// Напишите Pipeline: распределение инцидентов по severity
// с процентным соотношением
// Подсказка: используйте $facet или два $group

// Ваш Pipeline:



print("\n✓ Скрипт 04 выполнен. Aggregation Pipeline продемонстрирован.");
