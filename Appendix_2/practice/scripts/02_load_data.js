// ============================================================
// Скрипт 02: Загрузка данных в коллекции
// Предприятие «Руда+» — MES-система
// Среда: Yandex StoreDoc (MongoDB-совместимая СУБД)
// IDE: JetBrains DataGrip
// ============================================================
// ВАЖНО: Выполните скрипт 01_create_collections.js перед этим!
// ============================================================

// --- 1. Загрузка оборудования (12 документов, Polymorphic Pattern) ---

print("Загрузка оборудования...");

db.equipment.insertMany([
  {
    _id: "EQ001", type: "ПДМ", model: "ST14", manufacturer: "Epiroc",
    year_manufactured: 2021, serial_number: "EP-ST14-20210342",
    status: "working", mine: { _id: "M001", name: "Северная" },
    engine_hours: 12450, commissioned_date: "2021-06-15",
    specifications: {
      engine_power_kw: 200, max_payload_tons: 14,
      bucket_volume_m3: 7.0, tram_speed_kmh: 25, operating_weight_kg: 28000
    },
    sensors: [
      { sensor_id: "SNS001", type: "temperature", location: "engine" },
      { sensor_id: "SNS002", type: "vibration", location: "transmission" },
      { sensor_id: "SNS003", type: "gps", location: "cabin" },
      { sensor_id: "SNS004", type: "video", location: "cabin_front" }
    ],
    maintenance: {
      last_service: "2025-01-10", next_service: "2025-04-10",
      history: [
        { date: "2025-01-10", type: "ТО-2", cost: 45000, description: "Замена фильтров, проверка гидравлики" },
        { date: "2024-07-20", type: "ТО-3", cost: 120000, description: "Капитальный ремонт двигателя" }
      ]
    },
    location: { latitude: 56.8431, longitude: 60.6454, level: -320 }
  },
  {
    _id: "EQ002", type: "ПДМ", model: "ST18", manufacturer: "Epiroc",
    year_manufactured: 2022, serial_number: "EP-ST18-20220187",
    status: "working", mine: { _id: "M001", name: "Северная" },
    engine_hours: 8320, commissioned_date: "2022-03-01",
    specifications: {
      engine_power_kw: 275, max_payload_tons: 18,
      bucket_volume_m3: 10.0, tram_speed_kmh: 28, operating_weight_kg: 36000
    },
    sensors: [
      { sensor_id: "SNS005", type: "temperature", location: "engine" },
      { sensor_id: "SNS006", type: "vibration", location: "transmission" },
      { sensor_id: "SNS007", type: "gps", location: "cabin" }
    ],
    maintenance: {
      last_service: "2025-02-01", next_service: "2025-05-01",
      history: [
        { date: "2025-02-01", type: "ТО-1", cost: 25000, description: "Плановое ТО" }
      ]
    },
    location: { latitude: 56.8428, longitude: 60.6461, level: -280 }
  },
  {
    _id: "EQ003", type: "ПДМ", model: "LH517i", manufacturer: "Sandvik",
    year_manufactured: 2020, serial_number: "SV-LH517-20200055",
    status: "maintenance", mine: { _id: "M002", name: "Центральная" },
    engine_hours: 15680, commissioned_date: "2020-09-10",
    specifications: {
      engine_power_kw: 250, max_payload_tons: 17,
      bucket_volume_m3: 9.2, tram_speed_kmh: 30, operating_weight_kg: 34500
    },
    sensors: [
      { sensor_id: "SNS008", type: "temperature", location: "engine" },
      { sensor_id: "SNS009", type: "vibration", location: "chassis" },
      { sensor_id: "SNS010", type: "gps", location: "cabin" },
      { sensor_id: "SNS011", type: "video", location: "cabin_front" },
      { sensor_id: "SNS012", type: "video", location: "cabin_rear" }
    ],
    maintenance: {
      last_service: "2025-02-10", next_service: "2025-03-10",
      history: [
        { date: "2025-02-10", type: "Аварийный ремонт", cost: 280000, description: "Замена трансмиссии" },
        { date: "2024-11-05", type: "ТО-2", cost: 52000, description: "Замена фильтров и масла" }
      ]
    },
    location: { latitude: 56.8510, longitude: 60.6520, level: -400 }
  },
  {
    _id: "EQ004", type: "ПДМ", model: "ST14", manufacturer: "Epiroc",
    year_manufactured: 2023, serial_number: "EP-ST14-20230091",
    status: "working", mine: { _id: "M003", name: "Южная" },
    engine_hours: 4200, commissioned_date: "2023-01-20",
    specifications: {
      engine_power_kw: 200, max_payload_tons: 14,
      bucket_volume_m3: 7.0, tram_speed_kmh: 25, operating_weight_kg: 28000
    },
    sensors: [
      { sensor_id: "SNS013", type: "temperature", location: "engine" },
      { sensor_id: "SNS014", type: "gps", location: "cabin" }
    ],
    maintenance: {
      last_service: "2024-12-15", next_service: "2025-03-15",
      history: [
        { date: "2024-12-15", type: "ТО-1", cost: 22000, description: "Плановое ТО" }
      ]
    },
    location: { latitude: 56.8380, longitude: 60.6580, level: -180 }
  },
  {
    _id: "EQ005", type: "Самосвал", model: "TH663i", manufacturer: "Sandvik",
    year_manufactured: 2021, serial_number: "SV-TH663-20210034",
    status: "working", mine: { _id: "M001", name: "Северная" },
    engine_hours: 11200, commissioned_date: "2021-04-25",
    specifications: {
      engine_power_kw: 500, max_payload_tons: 63,
      bed_capacity_m3: 38.0, max_speed_kmh: 40,
      operating_weight_kg: 72000, turning_radius_m: 9.2
    },
    sensors: [
      { sensor_id: "SNS015", type: "temperature", location: "engine" },
      { sensor_id: "SNS016", type: "vibration", location: "chassis" },
      { sensor_id: "SNS017", type: "gps", location: "cabin" },
      { sensor_id: "SNS018", type: "video", location: "cabin_front" },
      { sensor_id: "SNS019", type: "payload_scale", location: "bed" }
    ],
    maintenance: {
      last_service: "2025-01-20", next_service: "2025-04-20",
      history: [
        { date: "2025-01-20", type: "ТО-2", cost: 85000, description: "Замена тормозных колодок" }
      ]
    },
    location: { latitude: 56.8435, longitude: 60.6448, level: -280 }
  },
  {
    _id: "EQ006", type: "Самосвал", model: "MT65", manufacturer: "Epiroc",
    year_manufactured: 2022, serial_number: "EP-MT65-20220012",
    status: "working", mine: { _id: "M002", name: "Центральная" },
    engine_hours: 7850, commissioned_date: "2022-08-12",
    specifications: {
      engine_power_kw: 480, max_payload_tons: 65,
      bed_capacity_m3: 40.0, max_speed_kmh: 38,
      operating_weight_kg: 75000, turning_radius_m: 10.1
    },
    sensors: [
      { sensor_id: "SNS020", type: "temperature", location: "engine" },
      { sensor_id: "SNS021", type: "gps", location: "cabin" },
      { sensor_id: "SNS022", type: "video", location: "cabin_front" },
      { sensor_id: "SNS023", type: "payload_scale", location: "bed" }
    ],
    maintenance: {
      last_service: "2024-11-30", next_service: "2025-02-28",
      history: [
        { date: "2024-11-30", type: "ТО-1", cost: 35000, description: "Плановое ТО, долив масла" }
      ]
    },
    location: { latitude: 56.8515, longitude: 60.6530, level: -350 }
  },
  {
    _id: "EQ007", type: "Самосвал", model: "TH551i", manufacturer: "Sandvik",
    year_manufactured: 2019, serial_number: "SV-TH551-20190078",
    status: "idle", mine: { _id: "M003", name: "Южная" },
    engine_hours: 18900, commissioned_date: "2019-11-01",
    specifications: {
      engine_power_kw: 420, max_payload_tons: 51,
      bed_capacity_m3: 32.0, max_speed_kmh: 35,
      operating_weight_kg: 62000, turning_radius_m: 8.5
    },
    sensors: [
      { sensor_id: "SNS024", type: "temperature", location: "engine" },
      { sensor_id: "SNS025", type: "vibration", location: "chassis" },
      { sensor_id: "SNS026", type: "gps", location: "cabin" }
    ],
    maintenance: {
      last_service: "2024-10-15", next_service: "2025-01-15",
      history: [
        { date: "2024-10-15", type: "ТО-3", cost: 210000, description: "Капитальный ремонт ходовой" },
        { date: "2024-04-20", type: "ТО-2", cost: 78000, description: "Замена фильтров" }
      ]
    },
    location: { latitude: 56.8375, longitude: 60.6590, level: -220 }
  },
  {
    _id: "EQ008", type: "Вагонетка", model: "ВГ-4.5", manufacturer: "Уралмашзавод",
    year_manufactured: 2020, serial_number: "UMZ-VG45-20200015",
    status: "working", mine: { _id: "M001", name: "Северная" },
    engine_hours: 0, commissioned_date: "2020-05-10",
    specifications: {
      capacity_m3: 4.5, max_payload_tons: 12,
      track_gauge_mm: 900, wagon_length_m: 3.8, tare_weight_kg: 2200
    },
    sensors: [
      { sensor_id: "SNS027", type: "payload_scale", location: "body" },
      { sensor_id: "SNS028", type: "rfid", location: "frame" }
    ],
    maintenance: {
      last_service: "2024-09-01", next_service: "2025-09-01",
      history: [
        { date: "2024-09-01", type: "Осмотр", cost: 5000, description: "Ежегодный осмотр рельсового пути и колёс" }
      ]
    },
    assigned_route: { from: "Забой-3", to: "Скиповая яма", horizon: -320 }
  },
  {
    _id: "EQ009", type: "Вагонетка", model: "ВГ-6.0", manufacturer: "Уралмашзавод",
    year_manufactured: 2022, serial_number: "UMZ-VG60-20220008",
    status: "working", mine: { _id: "M002", name: "Центральная" },
    engine_hours: 0, commissioned_date: "2022-10-05",
    specifications: {
      capacity_m3: 6.0, max_payload_tons: 16,
      track_gauge_mm: 900, wagon_length_m: 4.2, tare_weight_kg: 2800
    },
    sensors: [
      { sensor_id: "SNS029", type: "payload_scale", location: "body" },
      { sensor_id: "SNS030", type: "rfid", location: "frame" }
    ],
    maintenance: {
      last_service: "2024-10-01", next_service: "2025-10-01",
      history: [
        { date: "2024-10-01", type: "Осмотр", cost: 5000, description: "Ежегодный осмотр" }
      ]
    },
    assigned_route: { from: "Забой-1", to: "Скиповая яма", horizon: -400 }
  },
  {
    _id: "EQ010", type: "Скип", model: "СН-5.0", manufacturer: "Уралмашзавод",
    year_manufactured: 2018, serial_number: "UMZ-SN50-20180003",
    status: "working", mine: { _id: "M001", name: "Северная" },
    engine_hours: 22500, commissioned_date: "2018-03-20",
    specifications: {
      lift_height_m: 450, skip_volume_m3: 5.0,
      max_payload_tons: 15, hoist_speed_ms: 12,
      rope_diameter_mm: 42, motor_power_kw: 800
    },
    sensors: [
      { sensor_id: "SNS031", type: "vibration", location: "hoist_motor" },
      { sensor_id: "SNS032", type: "temperature", location: "hoist_motor" },
      { sensor_id: "SNS033", type: "rope_tension", location: "headframe" },
      { sensor_id: "SNS034", type: "position", location: "skip" },
      { sensor_id: "SNS035", type: "video", location: "shaft_top" }
    ],
    maintenance: {
      last_service: "2025-01-05", next_service: "2025-04-05",
      history: [
        { date: "2025-01-05", type: "ТО-2", cost: 150000, description: "Проверка каната, замена тормозных накладок" },
        { date: "2024-06-15", type: "ТО-3", cost: 450000, description: "Замена подъёмного каната" }
      ]
    },
    shaft: { name: "Ствол №1", depth_m: 500 }
  },
  {
    _id: "EQ011", type: "Скип", model: "СН-7.5", manufacturer: "НКМЗ",
    year_manufactured: 2020, serial_number: "NKMZ-SN75-20200001",
    status: "working", mine: { _id: "M002", name: "Центральная" },
    engine_hours: 14200, commissioned_date: "2020-07-01",
    specifications: {
      lift_height_m: 600, skip_volume_m3: 7.5,
      max_payload_tons: 22, hoist_speed_ms: 15,
      rope_diameter_mm: 48, motor_power_kw: 1200
    },
    sensors: [
      { sensor_id: "SNS036", type: "vibration", location: "hoist_motor" },
      { sensor_id: "SNS037", type: "temperature", location: "hoist_motor" },
      { sensor_id: "SNS038", type: "rope_tension", location: "headframe" },
      { sensor_id: "SNS039", type: "position", location: "skip" },
      { sensor_id: "SNS040", type: "video", location: "shaft_top" },
      { sensor_id: "SNS041", type: "video", location: "shaft_bottom" }
    ],
    maintenance: {
      last_service: "2024-12-20", next_service: "2025-03-20",
      history: [
        { date: "2024-12-20", type: "ТО-2", cost: 180000, description: "Проверка каната и тормозной системы" }
      ]
    },
    shaft: { name: "Ствол №2", depth_m: 650 }
  },
  {
    _id: "EQ012", type: "Скип", model: "СН-5.0", manufacturer: "Уралмашзавод",
    year_manufactured: 2023, serial_number: "UMZ-SN50-20230002",
    status: "working", mine: { _id: "M003", name: "Южная" },
    engine_hours: 3100, commissioned_date: "2023-05-15",
    specifications: {
      lift_height_m: 350, skip_volume_m3: 5.0,
      max_payload_tons: 15, hoist_speed_ms: 12,
      rope_diameter_mm: 42, motor_power_kw: 800
    },
    sensors: [
      { sensor_id: "SNS042", type: "vibration", location: "hoist_motor" },
      { sensor_id: "SNS043", type: "temperature", location: "hoist_motor" },
      { sensor_id: "SNS044", type: "rope_tension", location: "headframe" },
      { sensor_id: "SNS045", type: "position", location: "skip" }
    ],
    maintenance: {
      last_service: "2025-01-25", next_service: "2025-04-25",
      history: [
        { date: "2025-01-25", type: "ТО-1", cost: 60000, description: "Плановое ТО" }
      ]
    },
    shaft: { name: "Ствол №3", depth_m: 400 }
  }
]);

print("✓ equipment: " + db.equipment.countDocuments() + " документов загружено.");

// --- 2. Загрузка шахт (3 документа, Embedding Pattern) ---

print("Загрузка шахт...");

db.mines.insertMany([
  {
    _id: "M001", name: "Северная", code: "SH-01",
    location: {
      region: "Свердловская область", city: "Качканар",
      coordinates: { latitude: 56.8431, longitude: 60.6454 }
    },
    type: "подземная", depth_m: 500, commissioned_year: 1985, status: "active",
    horizons: [
      { level: -180, name: "Горизонт -180", status: "active",
        ore_type: "магнетитовая руда", avg_fe_pct: 38.5, active_faces: 3, equipment_count: 4 },
      { level: -280, name: "Горизонт -280", status: "active",
        ore_type: "магнетитовая руда", avg_fe_pct: 41.2, active_faces: 5, equipment_count: 6 },
      { level: -320, name: "Горизонт -320", status: "active",
        ore_type: "магнетитовая руда", avg_fe_pct: 43.0, active_faces: 4, equipment_count: 5 },
      { level: -420, name: "Горизонт -420", status: "development",
        ore_type: "магнетитовая руда", avg_fe_pct: null, active_faces: 0, equipment_count: 2 }
    ],
    daily_target_tons: 2500, shifts_per_day: 3, personnel_count: 320,
    ventilation: {
      system: "нагнетательная", capacity_m3s: 250,
      fans: [
        { id: "FAN-001", model: "ВОД-40", power_kw: 500 },
        { id: "FAN-002", model: "ВОД-40", power_kw: 500 }
      ]
    },
    contacts: {
      chief_engineer: "Иванов А.П.",
      phone: "+7-343-555-01-01",
      email: "severnaya@rudaplus.ru"
    }
  },
  {
    _id: "M002", name: "Центральная", code: "SH-02",
    location: {
      region: "Свердловская область", city: "Качканар",
      coordinates: { latitude: 56.8510, longitude: 60.6520 }
    },
    type: "подземная", depth_m: 650, commissioned_year: 1992, status: "active",
    horizons: [
      { level: -250, name: "Горизонт -250", status: "active",
        ore_type: "титаномагнетитовая руда", avg_fe_pct: 36.8, active_faces: 4, equipment_count: 5 },
      { level: -400, name: "Горизонт -400", status: "active",
        ore_type: "титаномагнетитовая руда", avg_fe_pct: 44.1, active_faces: 6, equipment_count: 7 },
      { level: -550, name: "Горизонт -550", status: "development",
        ore_type: "титаномагнетитовая руда", avg_fe_pct: null, active_faces: 1, equipment_count: 3 }
    ],
    daily_target_tons: 3200, shifts_per_day: 3, personnel_count: 410,
    ventilation: {
      system: "комбинированная", capacity_m3s: 380,
      fans: [
        { id: "FAN-003", model: "ВОД-50", power_kw: 630 },
        { id: "FAN-004", model: "ВОД-50", power_kw: 630 },
        { id: "FAN-005", model: "ВОД-30", power_kw: 315 }
      ]
    },
    contacts: {
      chief_engineer: "Петров Б.В.",
      phone: "+7-343-555-02-01",
      email: "centralnaya@rudaplus.ru"
    }
  },
  {
    _id: "M003", name: "Южная", code: "SH-03",
    location: {
      region: "Свердловская область", city: "Качканар",
      coordinates: { latitude: 56.8380, longitude: 60.6580 }
    },
    type: "подземная", depth_m: 400, commissioned_year: 2005, status: "active",
    horizons: [
      { level: -150, name: "Горизонт -150", status: "active",
        ore_type: "магнетитовая руда", avg_fe_pct: 35.2, active_faces: 2, equipment_count: 3 },
      { level: -220, name: "Горизонт -220", status: "active",
        ore_type: "магнетитовая руда", avg_fe_pct: 39.7, active_faces: 3, equipment_count: 4 }
    ],
    daily_target_tons: 1800, shifts_per_day: 2, personnel_count: 180,
    ventilation: {
      system: "нагнетательная", capacity_m3s: 180,
      fans: [
        { id: "FAN-006", model: "ВОД-30", power_kw: 315 }
      ]
    },
    contacts: {
      chief_engineer: "Сидоров В.Г.",
      phone: "+7-343-555-03-01",
      email: "yuzhnaya@rudaplus.ru"
    }
  }
]);

print("✓ mines: " + db.mines.countDocuments() + " документов загружено.");

// --- 3. Загрузка инцидентов (10 документов, Polymorphic Pattern) ---

print("Загрузка инцидентов...");

db.incidents.insertMany([
  {
    _id: "INC001", type: "equipment_failure", severity: "critical",
    equipment_id: "EQ003", mine_name: "Центральная", mine_id: "M002",
    incident_date: "2025-02-10T06:45:00Z", shift: 1,
    description: "Выход из строя трансмиссии ПДМ LH517i. Машина остановлена на горизонте -400.",
    location: { horizon: -400, zone: "Забой-2" },
    reported_by: "Козлов И.С.", response_time_min: 15, downtime_hours: 72,
    root_cause: "Износ шестерён раздаточной коробки",
    actions_taken: ["Эвакуация оператора", "Установка ограждения", "Вызов ремонтной бригады", "Замена трансмиссии в сборе"],
    repair_cost: 280000, status: "resolved", resolved_date: "2025-02-13T18:00:00Z"
  },
  {
    _id: "INC002", type: "sensor_alert", severity: "warning",
    equipment_id: "EQ001", mine_name: "Северная", mine_id: "M001",
    incident_date: "2025-01-28T14:22:00Z", shift: 2,
    description: "Датчик температуры двигателя SNS001 зафиксировал превышение порога 95°C. Температура достигла 102°C.",
    location: { horizon: -320, zone: "Штрек-5" },
    reported_by: "Автоматическая система мониторинга",
    sensor_data: { sensor_id: "SNS001", threshold: 95, actual_value: 102, unit: "°C" },
    response_time_min: 5, downtime_hours: 2,
    root_cause: "Засорение радиатора пылью",
    actions_taken: ["Остановка машины", "Охлаждение двигателя", "Очистка радиатора"],
    repair_cost: 0, status: "resolved", resolved_date: "2025-01-28T16:30:00Z"
  },
  {
    _id: "INC003", type: "safety_violation", severity: "warning",
    mine_name: "Северная", mine_id: "M001",
    incident_date: "2025-01-25T08:10:00Z", shift: 1,
    description: "Нарушение скоростного режима самосвалом EQ005 на спуске к горизонту -280. 45 км/ч при лимите 30 км/ч.",
    location: { horizon: -280, zone: "Уклон №2" },
    reported_by: "Система навигации", equipment_id: "EQ005",
    speed_data: { speed_limit_kmh: 30, actual_speed_kmh: 45, duration_sec: 12 },
    operator: { name: "Новиков Д.А.", tab_number: "T-1042" },
    actions_taken: ["Уведомление диспетчера", "Инструктаж оператора", "Запись в журнал нарушений"],
    status: "resolved", resolved_date: "2025-01-25T09:00:00Z"
  },
  {
    _id: "INC004", type: "equipment_failure", severity: "critical",
    equipment_id: "EQ010", mine_name: "Северная", mine_id: "M001",
    incident_date: "2024-12-15T22:30:00Z", shift: 3,
    description: "Аварийная остановка скипового подъёмника из-за срабатывания датчика натяжения каната.",
    location: { horizon: 0, zone: "Ствол №1" },
    reported_by: "Автоматическая система мониторинга",
    sensor_data: { sensor_id: "SNS033", threshold: 45, actual_value: 28, unit: "кН" },
    response_time_min: 2, downtime_hours: 18,
    root_cause: "Обрыв 3 прядей подъёмного каната",
    actions_taken: ["Аварийная остановка подъёма", "Фиксация скипа", "Осмотр каната", "Решение о замене каната"],
    repair_cost: 450000, status: "resolved", resolved_date: "2024-12-16T16:30:00Z"
  },
  {
    _id: "INC005", type: "ore_quality", severity: "info",
    mine_name: "Центральная", mine_id: "M002",
    incident_date: "2025-02-05T10:00:00Z", shift: 1,
    description: "Снижение содержания железа в руде с горизонта -250 до 32.1% (ниже планового 36%).",
    location: { horizon: -250, zone: "Забой-3" },
    reported_by: "Лаборатория качества",
    quality_data: {
      sample_id: "ORE-2025-0205-01", planned_fe_pct: 36.8, actual_fe_pct: 32.1,
      deviation_pct: -4.7,
      impurities: { silicon_pct: 12.5, sulfur_pct: 0.8, phosphorus_pct: 0.15 }
    },
    actions_taken: ["Уведомление геолога", "Отбор дополнительных проб", "Корректировка плана добычи"],
    status: "monitoring"
  },
  {
    _id: "INC006", type: "sensor_alert", severity: "warning",
    equipment_id: "EQ005", mine_name: "Северная", mine_id: "M001",
    incident_date: "2025-02-08T19:15:00Z", shift: 3,
    description: "Повышенная вибрация шасси самосвала TH663i. Уровень 8.5 мм/с при норме до 6.0 мм/с.",
    location: { horizon: -280, zone: "Транспортный штрек" },
    reported_by: "Автоматическая система мониторинга",
    sensor_data: { sensor_id: "SNS016", threshold: 6.0, actual_value: 8.5, unit: "мм/с" },
    response_time_min: 10, downtime_hours: 4,
    root_cause: "Разбалансировка колёс после наезда на камень",
    actions_taken: ["Остановка самосвала", "Осмотр ходовой части", "Балансировка колёс"],
    repair_cost: 15000, status: "resolved", resolved_date: "2025-02-08T23:20:00Z"
  },
  {
    _id: "INC007", type: "ventilation", severity: "critical",
    mine_name: "Центральная", mine_id: "M002",
    incident_date: "2025-01-18T03:40:00Z", shift: 3,
    description: "Отказ вентилятора ВОД-50 (FAN-003). Снижение подачи воздуха на горизонт -400 до 60%.",
    location: { horizon: 0, zone: "Вентиляционная установка №1" },
    reported_by: "Дежурный вентиляции Егоров К.Л.",
    ventilation_data: { fan_id: "FAN-003", normal_flow_m3s: 190, actual_flow_m3s: 114, co2_level_ppm: 850, co2_threshold_ppm: 500 },
    response_time_min: 8, downtime_hours: 6,
    root_cause: "Выход из строя подшипника электродвигателя",
    actions_taken: ["Переключение на резервный вентилятор", "Эвакуация людей с горизонта -400", "Замена подшипника"],
    repair_cost: 95000, status: "resolved", resolved_date: "2025-01-18T09:45:00Z",
    personnel_evacuated: 42
  },
  {
    _id: "INC008", type: "equipment_failure", severity: "warning",
    equipment_id: "EQ008", mine_name: "Северная", mine_id: "M001",
    incident_date: "2025-02-12T11:30:00Z", shift: 2,
    description: "Сход вагонетки ВГ-4.5 с рельсов на горизонте -320 вблизи пункта загрузки.",
    location: { horizon: -320, zone: "Околоствольный двор" },
    reported_by: "Машинист электровоза Белов Н.Ф.",
    response_time_min: 20, downtime_hours: 3,
    root_cause: "Деформация стрелочного перевода",
    actions_taken: ["Остановка движения на участке", "Подъём вагонетки домкратами", "Ремонт стрелочного перевода"],
    repair_cost: 12000, status: "resolved", resolved_date: "2025-02-12T14:30:00Z"
  },
  {
    _id: "INC009", type: "sensor_alert", severity: "info",
    equipment_id: "EQ011", mine_name: "Центральная", mine_id: "M002",
    incident_date: "2025-02-14T07:50:00Z", shift: 1,
    description: "Повышенная температура двигателя подъёмной машины СН-7.5 — 78°C при рекомендуемом максимуме 75°C.",
    location: { horizon: 0, zone: "Ствол №2" },
    reported_by: "Автоматическая система мониторинга",
    sensor_data: { sensor_id: "SNS037", threshold: 75, actual_value: 78, unit: "°C" },
    response_time_min: 30, downtime_hours: 0,
    root_cause: "Повышенная нагрузка из-за увеличенного плана подъёма",
    actions_taken: ["Увеличение интервалов между подъёмами", "Контроль температуры в ручном режиме"],
    repair_cost: 0, status: "monitoring"
  },
  {
    _id: "INC010", type: "safety_violation", severity: "warning",
    mine_name: "Южная", mine_id: "M003",
    incident_date: "2025-02-16T15:20:00Z", shift: 2,
    description: "Обнаружение персонала без СИЗ (каска, самоспасатель) в зоне работы ПДМ.",
    location: { horizon: -220, zone: "Рабочая зона ПДМ" },
    reported_by: "Инженер по ТБ Краснова Е.М.",
    operator: { name: "Тимофеев А.С.", tab_number: "T-2018" },
    violation_details: { missing_ppe: ["каска", "самоспасатель"], zone_type: "опасная зона", regulation: "ПБ 03-553-03, п.12.4" },
    actions_taken: ["Немедленное удаление из зоны работ", "Составление акта о нарушении", "Внеплановый инструктаж"],
    status: "resolved", resolved_date: "2025-02-16T16:00:00Z"
  }
]);

print("✓ incidents: " + db.incidents.countDocuments() + " документов загружено.");

// --- 4. Загрузка телеметрии (6 бакетов, Bucket Pattern) ---

print("Загрузка телеметрии (Bucket Pattern)...");

db.telemetry_buckets.insertMany([
  {
    _id: "TB001", sensor_id: "SNS001", equipment_id: "EQ001", sensor_type: "temperature",
    bucket_start: "2025-01-15T08:00:00Z", bucket_end: "2025-01-15T09:00:00Z",
    count: NumberInt(60),
    stats: { avg_temp: 72.3, min_temp: 65.1, max_temp: 88.4, std_dev: 5.2 },
    readings: [
      { ts: "2025-01-15T08:00:00Z", temp: 65.1 }, { ts: "2025-01-15T08:05:00Z", temp: 66.8 },
      { ts: "2025-01-15T08:10:00Z", temp: 74.3 }, { ts: "2025-01-15T08:20:00Z", temp: 78.1 },
      { ts: "2025-01-15T08:30:00Z", temp: 82.5 }, { ts: "2025-01-15T08:40:00Z", temp: 88.4 },
      { ts: "2025-01-15T08:45:00Z", temp: 79.3 }, { ts: "2025-01-15T08:55:00Z", temp: 70.2 }
    ]
  },
  {
    _id: "TB002", sensor_id: "SNS001", equipment_id: "EQ001", sensor_type: "temperature",
    bucket_start: "2025-01-15T09:00:00Z", bucket_end: "2025-01-15T10:00:00Z",
    count: NumberInt(60),
    stats: { avg_temp: 74.8, min_temp: 68.9, max_temp: 85.2, std_dev: 4.1 },
    readings: [
      { ts: "2025-01-15T09:00:00Z", temp: 68.9 }, { ts: "2025-01-15T09:10:00Z", temp: 73.2 },
      { ts: "2025-01-15T09:20:00Z", temp: 77.8 }, { ts: "2025-01-15T09:30:00Z", temp: 82.6 },
      { ts: "2025-01-15T09:35:00Z", temp: 85.2 }, { ts: "2025-01-15T09:45:00Z", temp: 74.1 },
      { ts: "2025-01-15T09:55:00Z", temp: 69.5 }
    ]
  },
  {
    _id: "TB003", sensor_id: "SNS002", equipment_id: "EQ001", sensor_type: "vibration",
    bucket_start: "2025-01-15T08:00:00Z", bucket_end: "2025-01-15T09:00:00Z",
    count: NumberInt(60),
    stats: { avg_vibr: 3.8, min_vibr: 1.2, max_vibr: 7.1, std_dev: 1.5 },
    readings: [
      { ts: "2025-01-15T08:00:00Z", vibr: 1.2 }, { ts: "2025-01-15T08:10:00Z", vibr: 3.8 },
      { ts: "2025-01-15T08:20:00Z", vibr: 4.5 }, { ts: "2025-01-15T08:30:00Z", vibr: 5.8 },
      { ts: "2025-01-15T08:35:00Z", vibr: 7.1 }, { ts: "2025-01-15T08:45:00Z", vibr: 3.1 },
      { ts: "2025-01-15T08:55:00Z", vibr: 1.8 }
    ]
  },
  {
    _id: "TB004", sensor_id: "SNS015", equipment_id: "EQ005", sensor_type: "temperature",
    bucket_start: "2025-01-15T08:00:00Z", bucket_end: "2025-01-15T09:00:00Z",
    count: NumberInt(60),
    stats: { avg_temp: 81.5, min_temp: 72.0, max_temp: 96.3, std_dev: 6.8 },
    readings: [
      { ts: "2025-01-15T08:00:00Z", temp: 72.0 }, { ts: "2025-01-15T08:10:00Z", temp: 78.9 },
      { ts: "2025-01-15T08:20:00Z", temp: 85.6 }, { ts: "2025-01-15T08:30:00Z", temp: 92.8 },
      { ts: "2025-01-15T08:35:00Z", temp: 96.3 }, { ts: "2025-01-15T08:45:00Z", temp: 82.1 },
      { ts: "2025-01-15T08:55:00Z", temp: 73.8 }
    ]
  },
  {
    _id: "TB005", sensor_id: "SNS032", equipment_id: "EQ010", sensor_type: "temperature",
    bucket_start: "2025-01-15T08:00:00Z", bucket_end: "2025-01-15T09:00:00Z",
    count: NumberInt(60),
    stats: { avg_temp: 58.2, min_temp: 45.0, max_temp: 71.5, std_dev: 7.3 },
    readings: [
      { ts: "2025-01-15T08:00:00Z", temp: 45.0 }, { ts: "2025-01-15T08:10:00Z", temp: 52.1 },
      { ts: "2025-01-15T08:20:00Z", temp: 59.4 }, { ts: "2025-01-15T08:30:00Z", temp: 67.2 },
      { ts: "2025-01-15T08:35:00Z", temp: 71.5 }, { ts: "2025-01-15T08:45:00Z", temp: 58.1 },
      { ts: "2025-01-15T08:55:00Z", temp: 47.2 }
    ]
  },
  {
    _id: "TB006", sensor_id: "SNS031", equipment_id: "EQ010", sensor_type: "vibration",
    bucket_start: "2025-01-15T08:00:00Z", bucket_end: "2025-01-15T09:00:00Z",
    count: NumberInt(60),
    stats: { avg_vibr: 4.5, min_vibr: 0.8, max_vibr: 9.2, std_dev: 2.4 },
    readings: [
      { ts: "2025-01-15T08:00:00Z", vibr: 0.8 }, { ts: "2025-01-15T08:10:00Z", vibr: 4.5 },
      { ts: "2025-01-15T08:20:00Z", vibr: 9.2 }, { ts: "2025-01-15T08:30:00Z", vibr: 5.1 },
      { ts: "2025-01-15T08:40:00Z", vibr: 1.5 }, { ts: "2025-01-15T08:50:00Z", vibr: 7.8 },
      { ts: "2025-01-15T08:55:00Z", vibr: 5.6 }
    ]
  }
]);

print("✓ telemetry_buckets: " + db.telemetry_buckets.countDocuments() + " документов загружено.");

// --- 5. Итоговая проверка ---

print("\n=== Итого загружено ===");
print("  equipment:          " + db.equipment.countDocuments());
print("  mines:              " + db.mines.countDocuments());
print("  incidents:          " + db.incidents.countDocuments());
print("  telemetry_buckets:  " + db.telemetry_buckets.countDocuments());

print("\n✓ Скрипт 02 выполнен. Все данные загружены.");
