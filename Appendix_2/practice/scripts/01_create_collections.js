// ============================================================
// Скрипт 01: Создание коллекций с Schema Validation
// Предприятие «Руда+» — MES-система
// Среда: Yandex StoreDoc (MongoDB-совместимая СУБД)
// IDE: JetBrains DataGrip
// ============================================================
// Выполняйте этот скрипт в консоли MongoDB (DataGrip: ПКМ → New → Console)
// ============================================================

// --- 1. Удаление старых коллекций (если существуют) ---

db.equipment.drop();
db.mines.drop();
db.incidents.drop();
db.telemetry_buckets.drop();

print("Старые коллекции удалены (если были).");

// --- 2. Коллекция equipment (Polymorphic Pattern) ---
// Строгая валидация ($jsonSchema)
// Разные типы оборудования имеют разные специфичные поля,
// но общие поля валидируются для всех документов.

db.createCollection("equipment", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "type", "model", "manufacturer", "status", "mine"],
      properties: {
        _id: {
          bsonType: "string",
          pattern: "^EQ\\d{3}$",
          description: "Идентификатор оборудования в формате EQxxx"
        },
        type: {
          bsonType: "string",
          enum: ["ПДМ", "Самосвал", "Вагонетка", "Скип"],
          description: "Тип оборудования"
        },
        model: {
          bsonType: "string",
          description: "Модель оборудования"
        },
        manufacturer: {
          bsonType: "string",
          description: "Производитель"
        },
        year_manufactured: {
          bsonType: "int",
          minimum: 2000,
          maximum: 2030,
          description: "Год выпуска (2000–2030)"
        },
        serial_number: {
          bsonType: "string",
          description: "Серийный номер"
        },
        status: {
          bsonType: "string",
          enum: ["working", "maintenance", "idle", "decommissioned"],
          description: "Текущий статус оборудования"
        },
        mine: {
          bsonType: "object",
          required: ["_id", "name"],
          properties: {
            _id: {
              bsonType: "string",
              pattern: "^M\\d{3}$",
              description: "ID шахты"
            },
            name: {
              bsonType: "string",
              description: "Название шахты (Extended Reference)"
            }
          },
          description: "Шахта приписки (Extended Reference Pattern)"
        },
        engine_hours: {
          bsonType: "number",
          minimum: 0,
          description: "Наработка в моточасах"
        },
        commissioned_date: {
          bsonType: "string",
          description: "Дата ввода в эксплуатацию"
        },
        sensors: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["sensor_id", "type"],
            properties: {
              sensor_id: {
                bsonType: "string",
                pattern: "^SNS\\d{3}$"
              },
              type: {
                bsonType: "string",
                enum: ["temperature", "vibration", "gps", "video",
                       "payload_scale", "rfid", "rope_tension", "position"]
              },
              location: {
                bsonType: "string"
              }
            }
          },
          description: "Массив установленных датчиков"
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

print("✓ Коллекция equipment создана (strict / error).");

// --- 3. Коллекция mines ---
// Строгая валидация для справочника шахт

db.createCollection("mines", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "name", "code", "status", "type"],
      properties: {
        _id: {
          bsonType: "string",
          pattern: "^M\\d{3}$",
          description: "Идентификатор шахты"
        },
        name: {
          bsonType: "string",
          description: "Название шахты"
        },
        code: {
          bsonType: "string",
          pattern: "^SH-\\d{2}$",
          description: "Код шахты в формате SH-XX"
        },
        type: {
          bsonType: "string",
          enum: ["подземная", "открытая", "комбинированная"],
          description: "Тип шахты"
        },
        status: {
          bsonType: "string",
          enum: ["active", "suspended", "closed"],
          description: "Статус шахты"
        },
        depth_m: {
          bsonType: "number",
          minimum: 0,
          description: "Глубина шахты в метрах"
        },
        horizons: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["level", "name", "status"],
            properties: {
              level: {
                bsonType: "number",
                description: "Отметка горизонта (отрицательное число)"
              },
              name: {
                bsonType: "string"
              },
              status: {
                bsonType: "string",
                enum: ["active", "development", "closed"]
              },
              ore_type: {
                bsonType: "string"
              },
              avg_fe_pct: {
                bsonType: ["number", "null"],
                description: "Среднее содержание Fe (%)"
              }
            }
          },
          description: "Горизонты шахты (Embedding Pattern)"
        },
        daily_target_tons: {
          bsonType: "number",
          minimum: 0,
          description: "Плановая суточная добыча (тонн)"
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

print("✓ Коллекция mines создана (strict / error).");

// --- 4. Коллекция incidents ---
// Умеренная валидация (moderate / warn)
// Инциденты имеют очень разную структуру — Polymorphic Pattern

db.createCollection("incidents", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "type", "severity", "mine_id", "incident_date", "description", "status"],
      properties: {
        _id: {
          bsonType: "string",
          pattern: "^INC\\d{3}$",
          description: "Идентификатор инцидента"
        },
        type: {
          bsonType: "string",
          enum: ["equipment_failure", "sensor_alert", "safety_violation",
                 "ore_quality", "ventilation", "geological"],
          description: "Тип инцидента"
        },
        severity: {
          bsonType: "string",
          enum: ["critical", "warning", "info"],
          description: "Уровень серьёзности"
        },
        mine_id: {
          bsonType: "string",
          description: "ID шахты"
        },
        incident_date: {
          bsonType: "string",
          description: "Дата и время инцидента (ISO 8601)"
        },
        description: {
          bsonType: "string",
          description: "Описание инцидента"
        },
        status: {
          bsonType: "string",
          enum: ["open", "in_progress", "resolved", "monitoring"],
          description: "Статус инцидента"
        }
      }
    }
  },
  validationLevel: "moderate",
  validationAction: "warn"
});

print("✓ Коллекция incidents создана (moderate / warn).");

// --- 5. Коллекция telemetry_buckets (Bucket Pattern) ---
// Строгая валидация для структуры бакетов

db.createCollection("telemetry_buckets", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "sensor_id", "equipment_id", "sensor_type",
                 "bucket_start", "bucket_end", "count", "stats", "readings"],
      properties: {
        _id: {
          bsonType: "string",
          description: "Идентификатор бакета"
        },
        sensor_id: {
          bsonType: "string",
          pattern: "^SNS\\d{3}$",
          description: "ID датчика"
        },
        equipment_id: {
          bsonType: "string",
          pattern: "^EQ\\d{3}$",
          description: "ID оборудования"
        },
        sensor_type: {
          bsonType: "string",
          enum: ["temperature", "vibration", "rope_tension", "position"],
          description: "Тип датчика"
        },
        bucket_start: {
          bsonType: "string",
          description: "Начало временного окна (ISO 8601)"
        },
        bucket_end: {
          bsonType: "string",
          description: "Конец временного окна (ISO 8601)"
        },
        count: {
          bsonType: "int",
          minimum: 0,
          maximum: 1000,
          description: "Количество показаний в бакете"
        },
        stats: {
          bsonType: "object",
          description: "Предвычисленные агрегаты (Computed Pattern)"
        },
        readings: {
          bsonType: "array",
          description: "Массив показаний"
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

print("✓ Коллекция telemetry_buckets создана (strict / error).");

// --- 6. Проверка ---

print("\n=== Созданные коллекции ===");
db.getCollectionNames().forEach(function(name) {
  print("  • " + name);
});

print("\n=== Правила валидации ===");
var collections = ["equipment", "mines", "incidents", "telemetry_buckets"];
collections.forEach(function(name) {
  var info = db.getCollectionInfos({ name: name })[0];
  if (info && info.options && info.options.validator) {
    var level = info.options.validationLevel || "strict";
    var action = info.options.validationAction || "error";
    print("  " + name + ": validationLevel=" + level + ", validationAction=" + action);
  }
});

print("\n✓ Скрипт 01 выполнен. Коллекции готовы к загрузке данных.");
