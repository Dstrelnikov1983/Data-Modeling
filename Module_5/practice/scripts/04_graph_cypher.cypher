// ============================================================
// Практикум по анализу и моделированию данных
// Модуль 5. Специализированное моделирование данных
// Скрипт 4: Графовое моделирование в Neo4j (Cypher)
// Предприятие: "Руда+" — добыча железной руды
//
// Содержание:
//   А. Очистка и ограничения
//   Б. Создание узлов (шахты, горизонты, оборудование, операторы, датчики)
//   В. Создание связей
//   Г. События обслуживания
//   Д. Запросы и аналитика
//   Е. Графовая аналитика
//   Ж. Обнаружение аномальных паттернов
//
// ВАЖНО: Выполняйте блоки последовательно в Neo4j Browser.
// Каждый блок вставляйте отдельно и нажимайте Ctrl+Enter.
// ============================================================


// ============================================================
// А. ОЧИСТКА И ОГРАНИЧЕНИЯ
// ============================================================

// А.1 Полная очистка базы (только для учебных целей!)
MATCH (n) DETACH DELETE n;

// А.2 Создание ограничений уникальности и индексов
CREATE CONSTRAINT mine_id_unique IF NOT EXISTS
FOR (m:Mine) REQUIRE m.mine_id IS UNIQUE;

CREATE CONSTRAINT equipment_id_unique IF NOT EXISTS
FOR (e:Equipment) REQUIRE e.equipment_id IS UNIQUE;

CREATE CONSTRAINT operator_id_unique IF NOT EXISTS
FOR (o:Operator) REQUIRE o.operator_id IS UNIQUE;

CREATE CONSTRAINT horizon_id_unique IF NOT EXISTS
FOR (h:Horizon) REQUIRE h.horizon_id IS UNIQUE;

CREATE CONSTRAINT sensor_id_unique IF NOT EXISTS
FOR (s:Sensor) REQUIRE s.sensor_id IS UNIQUE;

CREATE CONSTRAINT maint_event_id_unique IF NOT EXISTS
FOR (me:MaintenanceEvent) REQUIRE me.event_id IS UNIQUE;

// Индексы для часто используемых полей
CREATE INDEX equipment_type_index IF NOT EXISTS
FOR (e:Equipment) ON (e.type);

CREATE INDEX operator_qualification_index IF NOT EXISTS
FOR (o:Operator) ON (o.qualification);

CREATE INDEX maint_event_type_index IF NOT EXISTS
FOR (me:MaintenanceEvent) ON (me.type);


// ============================================================
// Б. СОЗДАНИЕ УЗЛОВ
// ============================================================

// --------------------------------------------------------
// Б.1 Шахты (Mine) — 4 шахты предприятия «Руда+»
// --------------------------------------------------------

CREATE (m1:Mine {
    mine_id: 'MINE-001',
    name: 'Северная',
    region: 'Кольский полуостров',
    depth_m: 850,
    status: 'Активная',
    commissioned_year: 1985,
    annual_capacity_tons: 2500000,
    employees: 320
});

CREATE (m2:Mine {
    mine_id: 'MINE-002',
    name: 'Южная',
    region: 'Курская магнитная аномалия',
    depth_m: 620,
    status: 'Активная',
    commissioned_year: 1992,
    annual_capacity_tons: 1800000,
    employees: 250
});

CREATE (m3:Mine {
    mine_id: 'MINE-003',
    name: 'Восточная',
    region: 'Урал',
    depth_m: 450,
    status: 'Активная',
    commissioned_year: 2005,
    annual_capacity_tons: 1200000,
    employees: 180
});

CREATE (m4:Mine {
    mine_id: 'MINE-004',
    name: 'Глубокая',
    region: 'Кольский полуостров',
    depth_m: 1200,
    status: 'Разведка',
    commissioned_year: 2020,
    annual_capacity_tons: 500000,
    employees: 75
});

// --------------------------------------------------------
// Б.2 Горизонты (Horizon) — уровни шахт
// --------------------------------------------------------

// Шахта «Северная» — 3 горизонта
CREATE (:Horizon {horizon_id: 'HRZ-001', name: 'Горизонт -350м', depth_m: 350, mine_id: 'MINE-001', status: 'Активный', ore_type: 'Магнетит'});
CREATE (:Horizon {horizon_id: 'HRZ-002', name: 'Горизонт -500м', depth_m: 500, mine_id: 'MINE-001', status: 'Активный', ore_type: 'Магнетит'});
CREATE (:Horizon {horizon_id: 'HRZ-003', name: 'Горизонт -750м', depth_m: 750, mine_id: 'MINE-001', status: 'Подготовка', ore_type: 'Гематит'});

// Шахта «Южная» — 3 горизонта
CREATE (:Horizon {horizon_id: 'HRZ-004', name: 'Горизонт -200м', depth_m: 200, mine_id: 'MINE-002', status: 'Активный', ore_type: 'Гематит'});
CREATE (:Horizon {horizon_id: 'HRZ-005', name: 'Горизонт -380м', depth_m: 380, mine_id: 'MINE-002', status: 'Активный', ore_type: 'Гематит'});
CREATE (:Horizon {horizon_id: 'HRZ-006', name: 'Горизонт -550м', depth_m: 550, mine_id: 'MINE-002', status: 'Разведка', ore_type: 'Сидерит'});

// Шахта «Восточная» — 2 горизонта
CREATE (:Horizon {horizon_id: 'HRZ-007', name: 'Горизонт -150м', depth_m: 150, mine_id: 'MINE-003', status: 'Активный', ore_type: 'Магнетит'});
CREATE (:Horizon {horizon_id: 'HRZ-008', name: 'Горизонт -320м', depth_m: 320, mine_id: 'MINE-003', status: 'Активный', ore_type: 'Сидерит'});

// Шахта «Глубокая» — 2 горизонта
CREATE (:Horizon {horizon_id: 'HRZ-009', name: 'Горизонт -600м', depth_m: 600, mine_id: 'MINE-004', status: 'Разведка', ore_type: 'Магнетит'});
CREATE (:Horizon {horizon_id: 'HRZ-010', name: 'Горизонт -1000м', depth_m: 1000, mine_id: 'MINE-004', status: 'Разведка', ore_type: 'Гематит'});

// --------------------------------------------------------
// Б.3 Оборудование (Equipment) — 12 единиц
// --------------------------------------------------------

// ПДМ-машины (погрузочно-доставочные машины)
CREATE (:Equipment {equipment_id: 'EQ-001', name: 'ПДМ-01', type: 'ПДМ', manufacturer: 'Sandvik', model: 'LH517i', status: 'В работе', year: 2019, engine_hours: 12500, max_payload_tons: 17.0});
CREATE (:Equipment {equipment_id: 'EQ-002', name: 'ПДМ-02', type: 'ПДМ', manufacturer: 'Caterpillar', model: 'R1700', status: 'В работе', year: 2020, engine_hours: 9800, max_payload_tons: 15.0});
CREATE (:Equipment {equipment_id: 'EQ-003', name: 'ПДМ-03', type: 'ПДМ', manufacturer: 'Sandvik', model: 'LH514', status: 'На ТО', year: 2018, engine_hours: 18200, max_payload_tons: 14.0});

// Шахтные самосвалы
CREATE (:Equipment {equipment_id: 'EQ-004', name: 'Самосвал-01', type: 'Шахтный самосвал', manufacturer: 'Sandvik', model: 'TH663i', status: 'В работе', year: 2021, engine_hours: 7600, max_payload_tons: 63.0});
CREATE (:Equipment {equipment_id: 'EQ-005', name: 'Самосвал-02', type: 'Шахтный самосвал', manufacturer: 'Caterpillar', model: 'AD63', status: 'В работе', year: 2019, engine_hours: 14100, max_payload_tons: 63.0});
CREATE (:Equipment {equipment_id: 'EQ-006', name: 'Самосвал-03', type: 'Шахтный самосвал', manufacturer: 'Sandvik', model: 'TH663i', status: 'В резерве', year: 2022, engine_hours: 3200, max_payload_tons: 63.0});

// Вагонетки
CREATE (:Equipment {equipment_id: 'EQ-007', name: 'Вагонетка-01', type: 'Вагонетка', manufacturer: 'УЗТМ', model: 'ВГ-4.5', status: 'В работе', year: 2017, engine_hours: 0, max_payload_tons: 4.5});
CREATE (:Equipment {equipment_id: 'EQ-008', name: 'Вагонетка-02', type: 'Вагонетка', manufacturer: 'УЗТМ', model: 'ВГ-4.5', status: 'В работе', year: 2017, engine_hours: 0, max_payload_tons: 4.5});
CREATE (:Equipment {equipment_id: 'EQ-009', name: 'Вагонетка-03', type: 'Вагонетка', manufacturer: 'УЗТМ', model: 'ВГ-4.5', status: 'В работе', year: 2018, engine_hours: 0, max_payload_tons: 4.5});

// Скиповые подъёмники
CREATE (:Equipment {equipment_id: 'EQ-010', name: 'Скип-01', type: 'Скиповый подъёмник', manufacturer: 'НКМЗ', model: 'СК-20', status: 'В работе', year: 2015, engine_hours: 22000, max_payload_tons: 20.0});
CREATE (:Equipment {equipment_id: 'EQ-011', name: 'Скип-02', type: 'Скиповый подъёмник', manufacturer: 'НКМЗ', model: 'СК-15', status: 'В работе', year: 2016, engine_hours: 19500, max_payload_tons: 15.0});
CREATE (:Equipment {equipment_id: 'EQ-012', name: 'Скип-03', type: 'Скиповый подъёмник', manufacturer: 'НКМЗ', model: 'СК-20', status: 'В резерве', year: 2020, engine_hours: 5600, max_payload_tons: 20.0});

// --------------------------------------------------------
// Б.4 Операторы (Operator) — 10 операторов
// --------------------------------------------------------

CREATE (:Operator {operator_id: 'OP-001', name: 'Иванов А.А.', first_name: 'Алексей', last_name: 'Иванов', position: 'Машинист ПДМ', qualification: '5 разряд', experience_years: 12, birth_year: 1985});
CREATE (:Operator {operator_id: 'OP-002', name: 'Петров Б.Б.', first_name: 'Борис', last_name: 'Петров', position: 'Машинист ПДМ', qualification: '4 разряд', experience_years: 8, birth_year: 1990});
CREATE (:Operator {operator_id: 'OP-003', name: 'Сидоров В.В.', first_name: 'Виктор', last_name: 'Сидоров', position: 'Водитель самосвала', qualification: '5 разряд', experience_years: 15, birth_year: 1980});
CREATE (:Operator {operator_id: 'OP-004', name: 'Козлов Г.Г.', first_name: 'Геннадий', last_name: 'Козлов', position: 'Водитель самосвала', qualification: '4 разряд', experience_years: 6, birth_year: 1993});
CREATE (:Operator {operator_id: 'OP-005', name: 'Новиков Д.Д.', first_name: 'Дмитрий', last_name: 'Новиков', position: 'Оператор подъёмника', qualification: '5 разряд', experience_years: 20, birth_year: 1975});
CREATE (:Operator {operator_id: 'OP-006', name: 'Морозов Е.Е.', first_name: 'Евгений', last_name: 'Морозов', position: 'Машинист ПДМ', qualification: '3 разряд', experience_years: 3, birth_year: 1998});
CREATE (:Operator {operator_id: 'OP-007', name: 'Волков Ж.Ж.', first_name: 'Жан', last_name: 'Волков', position: 'Водитель самосвала', qualification: '4 разряд', experience_years: 7, birth_year: 1991});
CREATE (:Operator {operator_id: 'OP-008', name: 'Соколов З.З.', first_name: 'Захар', last_name: 'Соколов', position: 'Оператор подъёмника', qualification: '4 разряд', experience_years: 10, birth_year: 1987});
CREATE (:Operator {operator_id: 'OP-009', name: 'Лебедев И.И.', first_name: 'Игорь', last_name: 'Лебедев', position: 'Механик', qualification: '5 разряд', experience_years: 18, birth_year: 1978});
CREATE (:Operator {operator_id: 'OP-010', name: 'Кузнецов К.К.', first_name: 'Константин', last_name: 'Кузнецов', position: 'Механик', qualification: '4 разряд', experience_years: 5, birth_year: 1995});

// --------------------------------------------------------
// Б.5 Датчики (Sensor) — 20 датчиков на оборудовании
// --------------------------------------------------------

// Датчики ПДМ-01 (EQ-001)
CREATE (:Sensor {sensor_id: 'SENS-001', type: 'Температура двигателя', unit: 'celsius', alarm_threshold: 100, install_date: date('2022-03-15'), equipment_id: 'EQ-001'});
CREATE (:Sensor {sensor_id: 'SENS-002', type: 'Давление гидравлики', unit: 'bar', alarm_threshold: 200, install_date: date('2022-03-15'), equipment_id: 'EQ-001'});
CREATE (:Sensor {sensor_id: 'SENS-003', type: 'Вибрация', unit: 'mm/s', alarm_threshold: 7.0, install_date: date('2022-03-15'), equipment_id: 'EQ-001'});
CREATE (:Sensor {sensor_id: 'SENS-004', type: 'Скорость', unit: 'km/h', alarm_threshold: 15, install_date: date('2022-03-15'), equipment_id: 'EQ-001'});

// Датчики ПДМ-02 (EQ-002)
CREATE (:Sensor {sensor_id: 'SENS-005', type: 'Температура двигателя', unit: 'celsius', alarm_threshold: 100, install_date: date('2022-06-10'), equipment_id: 'EQ-002'});
CREATE (:Sensor {sensor_id: 'SENS-006', type: 'Давление гидравлики', unit: 'bar', alarm_threshold: 200, install_date: date('2022-06-10'), equipment_id: 'EQ-002'});
CREATE (:Sensor {sensor_id: 'SENS-007', type: 'Вибрация', unit: 'mm/s', alarm_threshold: 7.0, install_date: date('2022-06-10'), equipment_id: 'EQ-002'});

// Датчики ПДМ-03 (EQ-003)
CREATE (:Sensor {sensor_id: 'SENS-008', type: 'Температура двигателя', unit: 'celsius', alarm_threshold: 100, install_date: date('2021-11-20'), equipment_id: 'EQ-003'});
CREATE (:Sensor {sensor_id: 'SENS-009', type: 'Вибрация', unit: 'mm/s', alarm_threshold: 7.0, install_date: date('2021-11-20'), equipment_id: 'EQ-003'});

// Датчики самосвалов
CREATE (:Sensor {sensor_id: 'SENS-010', type: 'Температура двигателя', unit: 'celsius', alarm_threshold: 105, install_date: date('2023-01-10'), equipment_id: 'EQ-004'});
CREATE (:Sensor {sensor_id: 'SENS-011', type: 'Вибрация', unit: 'mm/s', alarm_threshold: 8.0, install_date: date('2023-01-10'), equipment_id: 'EQ-004'});
CREATE (:Sensor {sensor_id: 'SENS-012', type: 'Уровень топлива', unit: 'percent', alarm_threshold: 15, install_date: date('2023-01-10'), equipment_id: 'EQ-004'});
CREATE (:Sensor {sensor_id: 'SENS-013', type: 'Температура двигателя', unit: 'celsius', alarm_threshold: 105, install_date: date('2022-09-05'), equipment_id: 'EQ-005'});
CREATE (:Sensor {sensor_id: 'SENS-014', type: 'Вибрация', unit: 'mm/s', alarm_threshold: 8.0, install_date: date('2022-09-05'), equipment_id: 'EQ-005'});

// Датчики скиповых подъёмников
CREATE (:Sensor {sensor_id: 'SENS-015', type: 'Вибрация каната', unit: 'mm/s', alarm_threshold: 3.0, install_date: date('2021-05-20'), equipment_id: 'EQ-010'});
CREATE (:Sensor {sensor_id: 'SENS-016', type: 'Нагрузка', unit: 'tons', alarm_threshold: 22, install_date: date('2021-05-20'), equipment_id: 'EQ-010'});
CREATE (:Sensor {sensor_id: 'SENS-017', type: 'Скорость подъёма', unit: 'm/s', alarm_threshold: 12, install_date: date('2021-05-20'), equipment_id: 'EQ-010'});
CREATE (:Sensor {sensor_id: 'SENS-018', type: 'Вибрация каната', unit: 'mm/s', alarm_threshold: 3.0, install_date: date('2022-01-15'), equipment_id: 'EQ-011'});
CREATE (:Sensor {sensor_id: 'SENS-019', type: 'Нагрузка', unit: 'tons', alarm_threshold: 17, install_date: date('2022-01-15'), equipment_id: 'EQ-011'});
CREATE (:Sensor {sensor_id: 'SENS-020', type: 'Скорость подъёма', unit: 'm/s', alarm_threshold: 12, install_date: date('2022-01-15'), equipment_id: 'EQ-011'});


// ============================================================
// В. СОЗДАНИЕ СВЯЗЕЙ
// ============================================================

// --------------------------------------------------------
// В.1 Горизонты принадлежат шахтам (PART_OF)
// --------------------------------------------------------

MATCH (h:Horizon), (m:Mine)
WHERE h.mine_id = m.mine_id
CREATE (h)-[:PART_OF]->(m);

// --------------------------------------------------------
// В.2 Оборудование расположено в шахтах (LOCATED_IN)
// --------------------------------------------------------

// ПДМ и самосвалы — в шахте «Северная»
MATCH (e:Equipment {equipment_id: 'EQ-001'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2022-01-15'), horizon: 'Горизонт -350м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-002'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2022-03-20'), horizon: 'Горизонт -500м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-003'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (e)-[:LOCATED_IN {since: date('2021-06-10'), horizon: 'Горизонт -200м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-004'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2023-02-01'), horizon: 'Горизонт -350м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-005'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (e)-[:LOCATED_IN {since: date('2022-08-15'), horizon: 'Горизонт -380м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-006'}), (m:Mine {mine_id: 'MINE-003'})
CREATE (e)-[:LOCATED_IN {since: date('2023-05-10'), horizon: 'Горизонт -150м'}]->(m);

// Вагонетки
MATCH (e:Equipment {equipment_id: 'EQ-007'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2020-01-10'), horizon: 'Горизонт -350м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-008'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2020-01-10'), horizon: 'Горизонт -500м'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-009'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (e)-[:LOCATED_IN {since: date('2021-03-15'), horizon: 'Горизонт -200м'}]->(m);

// Скиповые подъёмники
MATCH (e:Equipment {equipment_id: 'EQ-010'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (e)-[:LOCATED_IN {since: date('2015-06-01'), horizon: 'Главный ствол'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-011'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (e)-[:LOCATED_IN {since: date('2016-09-01'), horizon: 'Главный ствол'}]->(m);

MATCH (e:Equipment {equipment_id: 'EQ-012'}), (m:Mine {mine_id: 'MINE-003'})
CREATE (e)-[:LOCATED_IN {since: date('2020-11-01'), horizon: 'Главный ствол'}]->(m);

// --------------------------------------------------------
// В.3 Оборудование на горизонте (ON_HORIZON)
// --------------------------------------------------------

MATCH (e:Equipment {equipment_id: 'EQ-001'}), (h:Horizon {horizon_id: 'HRZ-001'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-002'}), (h:Horizon {horizon_id: 'HRZ-002'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-003'}), (h:Horizon {horizon_id: 'HRZ-004'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-004'}), (h:Horizon {horizon_id: 'HRZ-001'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-005'}), (h:Horizon {horizon_id: 'HRZ-005'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-007'}), (h:Horizon {horizon_id: 'HRZ-001'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-008'}), (h:Horizon {horizon_id: 'HRZ-002'})
CREATE (e)-[:ON_HORIZON]->(h);

MATCH (e:Equipment {equipment_id: 'EQ-009'}), (h:Horizon {horizon_id: 'HRZ-004'})
CREATE (e)-[:ON_HORIZON]->(h);

// --------------------------------------------------------
// В.4 Операторы работают на шахтах (WORKS_AT)
// --------------------------------------------------------

MATCH (o:Operator {operator_id: 'OP-001'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (o)-[:WORKS_AT {since: date('2015-03-01'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-002'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (o)-[:WORKS_AT {since: date('2018-06-15'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-003'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (o)-[:WORKS_AT {since: date('2012-01-10'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-004'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (o)-[:WORKS_AT {since: date('2020-09-01'), shift: 'Ночная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-005'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (o)-[:WORKS_AT {since: date('2008-04-20'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-006'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (o)-[:WORKS_AT {since: date('2023-02-01'), shift: 'Ночная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-007'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (o)-[:WORKS_AT {since: date('2019-07-15'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-008'}), (m:Mine {mine_id: 'MINE-002'})
CREATE (o)-[:WORKS_AT {since: date('2017-11-01'), shift: 'Ночная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-009'}), (m:Mine {mine_id: 'MINE-001'})
CREATE (o)-[:WORKS_AT {since: date('2010-05-15'), shift: 'Дневная'}]->(m);

MATCH (o:Operator {operator_id: 'OP-010'}), (m:Mine {mine_id: 'MINE-003'})
CREATE (o)-[:WORKS_AT {since: date('2022-01-10'), shift: 'Дневная'}]->(m);

// --------------------------------------------------------
// В.5 Операторы управляют оборудованием (OPERATES)
// --------------------------------------------------------

// Иванов А.А. → ПДМ-01
MATCH (o:Operator {operator_id: 'OP-001'}), (e:Equipment {equipment_id: 'EQ-001'})
CREATE (o)-[:OPERATES {since: date('2022-01-15'), certified: true, hours_logged: 3200}]->(e);

// Иванов А.А. → ПДМ-02 (может управлять двумя машинами)
MATCH (o:Operator {operator_id: 'OP-001'}), (e:Equipment {equipment_id: 'EQ-002'})
CREATE (o)-[:OPERATES {since: date('2022-04-01'), certified: true, hours_logged: 1500}]->(e);

// Петров Б.Б. → ПДМ-02
MATCH (o:Operator {operator_id: 'OP-002'}), (e:Equipment {equipment_id: 'EQ-002'})
CREATE (o)-[:OPERATES {since: date('2022-03-20'), certified: true, hours_logged: 2800}]->(e);

// Морозов Е.Е. → ПДМ-03
MATCH (o:Operator {operator_id: 'OP-006'}), (e:Equipment {equipment_id: 'EQ-003'})
CREATE (o)-[:OPERATES {since: date('2023-02-01'), certified: true, hours_logged: 1200}]->(e);

// Сидоров В.В. → Самосвал-01
MATCH (o:Operator {operator_id: 'OP-003'}), (e:Equipment {equipment_id: 'EQ-004'})
CREATE (o)-[:OPERATES {since: date('2023-02-01'), certified: true, hours_logged: 2100}]->(e);

// Козлов Г.Г. → Самосвал-02
MATCH (o:Operator {operator_id: 'OP-004'}), (e:Equipment {equipment_id: 'EQ-005'})
CREATE (o)-[:OPERATES {since: date('2022-08-15'), certified: true, hours_logged: 1800}]->(e);

// Волков Ж.Ж. → Самосвал-03
MATCH (o:Operator {operator_id: 'OP-007'}), (e:Equipment {equipment_id: 'EQ-006'})
CREATE (o)-[:OPERATES {since: date('2023-05-10'), certified: true, hours_logged: 900}]->(e);

// Новиков Д.Д. → Скип-01
MATCH (o:Operator {operator_id: 'OP-005'}), (e:Equipment {equipment_id: 'EQ-010'})
CREATE (o)-[:OPERATES {since: date('2015-06-01'), certified: true, hours_logged: 15000}]->(e);

// Соколов З.З. → Скип-02
MATCH (o:Operator {operator_id: 'OP-008'}), (e:Equipment {equipment_id: 'EQ-011'})
CREATE (o)-[:OPERATES {since: date('2017-11-01'), certified: true, hours_logged: 8500}]->(e);

// --------------------------------------------------------
// В.6 У оборудования есть датчики (HAS_SENSOR)
// --------------------------------------------------------

MATCH (e:Equipment), (s:Sensor)
WHERE s.equipment_id = e.equipment_id
CREATE (e)-[:HAS_SENSOR {installed: s.install_date}]->(s);

// --------------------------------------------------------
// В.7 Маршрутные связи: цепочка транспортировки руды (CONNECTED_TO)
// --------------------------------------------------------

// Шахта «Северная» — маршрут 1:
// ПДМ-01 → Вагонетка-01 → Скип-01
MATCH (a:Equipment {equipment_id: 'EQ-001'}), (b:Equipment {equipment_id: 'EQ-007'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 1', distance_m: 500, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-007'}), (b:Equipment {equipment_id: 'EQ-010'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 1', distance_m: 1200, transport_type: 'рельсовый'}]->(b);

// Шахта «Северная» — маршрут 2:
// ПДМ-02 → Самосвал-01 → Вагонетка-01 → Скип-01
MATCH (a:Equipment {equipment_id: 'EQ-002'}), (b:Equipment {equipment_id: 'EQ-004'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 2', distance_m: 300, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-004'}), (b:Equipment {equipment_id: 'EQ-008'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 2', distance_m: 800, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-008'}), (b:Equipment {equipment_id: 'EQ-010'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 2', distance_m: 1000, transport_type: 'рельсовый'}]->(b);

// Шахта «Южная» — маршрут:
// ПДМ-03 → Самосвал-02 → Вагонетка-03 → Скип-02
MATCH (a:Equipment {equipment_id: 'EQ-003'}), (b:Equipment {equipment_id: 'EQ-005'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 3', distance_m: 400, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-005'}), (b:Equipment {equipment_id: 'EQ-009'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 3', distance_m: 600, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-009'}), (b:Equipment {equipment_id: 'EQ-011'})
CREATE (a)-[:CONNECTED_TO {route: 'Маршрут 3', distance_m: 900, transport_type: 'рельсовый'}]->(b);

// Альтернативный маршрут (резервный): ПДМ-01 → Самосвал-01 → Скип-01
MATCH (a:Equipment {equipment_id: 'EQ-001'}), (b:Equipment {equipment_id: 'EQ-004'})
CREATE (a)-[:CONNECTED_TO {route: 'Резервный маршрут', distance_m: 350, transport_type: 'перегрузка'}]->(b);

MATCH (a:Equipment {equipment_id: 'EQ-004'}), (b:Equipment {equipment_id: 'EQ-010'})
CREATE (a)-[:CONNECTED_TO {route: 'Резервный маршрут', distance_m: 2000, transport_type: 'прямая доставка'}]->(b);


// ============================================================
// Г. СОБЫТИЯ ОБСЛУЖИВАНИЯ (MaintenanceEvent)
// ============================================================

// --------------------------------------------------------
// Г.1 Создание событий обслуживания
// --------------------------------------------------------

// Плановые ТО
CREATE (:MaintenanceEvent {event_id: 'MNT-001', date: date('2025-01-15'), type: 'Плановое ТО', description: 'Замена масла и фильтров', duration_hours: 8, cost: 45000, equipment_id: 'EQ-001', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-002', date: date('2025-02-10'), type: 'Плановое ТО', description: 'Проверка гидравлики, замена уплотнений', duration_hours: 12, cost: 78000, equipment_id: 'EQ-002', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-003', date: date('2025-01-20'), type: 'Плановое ТО', description: 'Замена масла и проверка ходовой', duration_hours: 6, cost: 35000, equipment_id: 'EQ-004', performed_by: 'OP-010'});
CREATE (:MaintenanceEvent {event_id: 'MNT-004', date: date('2025-03-01'), type: 'Плановое ТО', description: 'Замена масла, проверка тормозов', duration_hours: 10, cost: 52000, equipment_id: 'EQ-005', performed_by: 'OP-010'});
CREATE (:MaintenanceEvent {event_id: 'MNT-005', date: date('2025-02-25'), type: 'Плановое ТО', description: 'Проверка каната и тормозной системы', duration_hours: 16, cost: 120000, equipment_id: 'EQ-010', performed_by: 'OP-009'});

// Аварийные ремонты
CREATE (:MaintenanceEvent {event_id: 'MNT-006', date: date('2025-03-15'), type: 'Аварийный', description: 'Перегрев двигателя, замена термостата и охлаждающей жидкости', duration_hours: 24, cost: 185000, equipment_id: 'EQ-001', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-007', date: date('2025-03-20'), type: 'Аварийный', description: 'Разрушение подшипника, замена подшипникового узла', duration_hours: 36, cost: 320000, equipment_id: 'EQ-003', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-008', date: date('2025-03-25'), type: 'Аварийный', description: 'Утечка гидравлической жидкости, замена шлангов', duration_hours: 18, cost: 95000, equipment_id: 'EQ-002', performed_by: 'OP-010'});

// Диагностика
CREATE (:MaintenanceEvent {event_id: 'MNT-009', date: date('2025-02-05'), type: 'Диагностика', description: 'Вибродиагностика подшипников', duration_hours: 4, cost: 15000, equipment_id: 'EQ-003', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-010', date: date('2025-03-05'), type: 'Диагностика', description: 'Контроль состояния каната', duration_hours: 3, cost: 12000, equipment_id: 'EQ-010', performed_by: 'OP-009'});

// Модернизация
CREATE (:MaintenanceEvent {event_id: 'MNT-011', date: date('2025-02-20'), type: 'Модернизация', description: 'Установка новых датчиков вибрации', duration_hours: 8, cost: 65000, equipment_id: 'EQ-001', performed_by: 'OP-010'});
CREATE (:MaintenanceEvent {event_id: 'MNT-012', date: date('2025-01-25'), type: 'Модернизация', description: 'Обновление системы навигации', duration_hours: 6, cost: 42000, equipment_id: 'EQ-004', performed_by: 'OP-010'});

// Дополнительные аварийные для EQ-003 (проблемное оборудование)
CREATE (:MaintenanceEvent {event_id: 'MNT-013', date: date('2024-11-10'), type: 'Аварийный', description: 'Обрыв гидравлического шланга', duration_hours: 14, cost: 110000, equipment_id: 'EQ-003', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-014', date: date('2024-12-22'), type: 'Аварийный', description: 'Перегрев трансмиссии', duration_hours: 20, cost: 250000, equipment_id: 'EQ-003', performed_by: 'OP-009'});
CREATE (:MaintenanceEvent {event_id: 'MNT-015', date: date('2025-01-30'), type: 'Плановое ТО', description: 'Полное ТО после аварийного ремонта', duration_hours: 24, cost: 180000, equipment_id: 'EQ-003', performed_by: 'OP-009'});

// --------------------------------------------------------
// Г.2 Связи обслуживания
// --------------------------------------------------------

// Оборудование → Событие обслуживания (REQUIRED_MAINTENANCE)
MATCH (e:Equipment), (me:MaintenanceEvent)
WHERE me.equipment_id = e.equipment_id
CREATE (e)-[:REQUIRED_MAINTENANCE {urgency: CASE me.type WHEN 'Аварийный' THEN 'Высокая' WHEN 'Плановое ТО' THEN 'Средняя' ELSE 'Низкая' END}]->(me);

// Событие обслуживания → Оператор (MAINTAINED_BY)
MATCH (me:MaintenanceEvent), (o:Operator)
WHERE me.performed_by = o.operator_id
CREATE (me)-[:MAINTAINED_BY]->(o);


// ============================================================
// Д. ЗАПРОСЫ И АНАЛИТИКА
// ============================================================

// --------------------------------------------------------
// Д.1 Базовые запросы с паттернами MATCH
// --------------------------------------------------------

// Д.1.1 Всё оборудование в шахте «Северная»
MATCH (e:Equipment)-[:LOCATED_IN]->(m:Mine {name: 'Северная'})
RETURN e.equipment_id AS id, e.name AS equipment, e.type AS type,
       e.status AS status, e.manufacturer AS manufacturer
ORDER BY e.type, e.name;

// Д.1.2 Операторы, управляющие ПДМ-машинами
MATCH (o:Operator)-[r:OPERATES]->(e:Equipment {type: 'ПДМ'})
RETURN o.name AS operator, o.qualification AS qualification,
       e.name AS equipment, r.hours_logged AS hours
ORDER BY r.hours_logged DESC;

// Д.1.3 Полная цепочка: Оператор → Оборудование → Шахта
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)-[:LOCATED_IN]->(m:Mine)
RETURN o.name AS operator, o.position AS position,
       e.name AS equipment, e.type AS type,
       m.name AS mine
ORDER BY m.name, e.type, o.name;

// Д.1.4 Датчики конкретного оборудования
MATCH (e:Equipment {equipment_id: 'EQ-001'})-[:HAS_SENSOR]->(s:Sensor)
RETURN e.name AS equipment,
       s.type AS sensor_type,
       s.unit AS unit,
       s.alarm_threshold AS threshold,
       s.install_date AS installed
ORDER BY s.type;

// --------------------------------------------------------
// Д.2 Агрегации в Cypher
// --------------------------------------------------------

// Д.2.1 Количество оборудования по шахтам
MATCH (e:Equipment)-[:LOCATED_IN]->(m:Mine)
RETURN m.name AS mine,
       m.region AS region,
       COUNT(e) AS equipment_count,
       COLLECT(e.name) AS equipment_list
ORDER BY equipment_count DESC;

// Д.2.2 Количество датчиков по оборудованию
MATCH (e:Equipment)-[:HAS_SENSOR]->(s:Sensor)
RETURN e.name AS equipment, e.type AS type,
       COUNT(s) AS sensor_count,
       COLLECT(s.type) AS sensor_types
ORDER BY sensor_count DESC;

// Д.2.3 Стоимость обслуживания по типам оборудования
MATCH (e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
RETURN e.type AS equipment_type,
       COUNT(me) AS total_events,
       SUM(me.duration_hours) AS total_hours,
       SUM(me.cost) AS total_cost,
       ROUND(AVG(me.cost)) AS avg_cost
ORDER BY total_cost DESC;

// Д.2.4 Операторы-ремонтники: кто проводил больше всего обслуживаний
MATCH (me:MaintenanceEvent)-[:MAINTAINED_BY]->(o:Operator)
RETURN o.name AS mechanic,
       o.qualification AS qualification,
       COUNT(me) AS events,
       SUM(me.duration_hours) AS total_hours,
       SUM(me.cost) AS total_cost
ORDER BY events DESC;


// ============================================================
// Е. ГРАФОВАЯ АНАЛИТИКА
// ============================================================

// --------------------------------------------------------
// Е.1 Кратчайший путь транспортировки руды
// --------------------------------------------------------

// Все пути от ПДМ-01 до скипового подъёмника
MATCH path = (start:Equipment {equipment_id: 'EQ-001'})
    -[:CONNECTED_TO*1..5]->
    (finish:Equipment {type: 'Скиповый подъёмник'})
RETURN [n IN nodes(path) | n.name] AS route,
       length(path) AS hops,
       REDUCE(d = 0, r IN relationships(path) | d + r.distance_m) AS total_distance_m
ORDER BY hops, total_distance_m;

// Кратчайший путь (по количеству перегрузок)
MATCH path = shortestPath(
    (start:Equipment {equipment_id: 'EQ-001'})
    -[:CONNECTED_TO*]->
    (finish:Equipment {equipment_id: 'EQ-010'})
)
RETURN [n IN nodes(path) | n.name] AS shortest_route,
       length(path) AS hops;

// --------------------------------------------------------
// Е.2 Изолированные узлы (оборудование без оператора)
// --------------------------------------------------------

MATCH (e:Equipment)
WHERE NOT (e)<-[:OPERATES]-(:Operator)
RETURN e.equipment_id AS id,
       e.name AS equipment,
       e.type AS type,
       e.status AS status
ORDER BY e.type;

// --------------------------------------------------------
// Е.3 Степень связности (Degree Centrality)
// --------------------------------------------------------

// Самые «связанные» узлы оборудования
MATCH (e:Equipment)
OPTIONAL MATCH (e)-[r]-()
WITH e, COUNT(r) AS total_connections,
     COUNT(r) AS degree
RETURN e.name AS equipment,
       e.type AS type,
       total_connections,
       // Распределение по типам связей
       SIZE([(e)-[:LOCATED_IN]->() | 1]) AS located_in,
       SIZE([(e)-[:CONNECTED_TO]->() | 1]) AS outgoing_routes,
       SIZE([()-[:CONNECTED_TO]->(e) | 1]) AS incoming_routes,
       SIZE([(e)-[:HAS_SENSOR]->() | 1]) AS sensors,
       SIZE([(e)-[:REQUIRED_MAINTENANCE]->() | 1]) AS maintenance_events,
       SIZE([()-[:OPERATES]->(e) | 1]) AS operators
ORDER BY total_connections DESC;

// --------------------------------------------------------
// Е.4 Рекомендация оператора для оборудования
// --------------------------------------------------------

// Для Самосвал-03 (EQ-006) — кто из операторов имеет опыт с самосвалами?
MATCH (target:Equipment {equipment_id: 'EQ-006'})
WITH target, target.type AS target_type

// Найти операторов, управляющих тем же типом оборудования
MATCH (o:Operator)-[r:OPERATES]->(similar:Equipment {type: target_type})
WHERE NOT (o)-[:OPERATES]->(target)
RETURN o.name AS recommended_operator,
       o.qualification AS qualification,
       o.experience_years AS experience,
       COUNT(similar) AS machines_of_this_type,
       SUM(r.hours_logged) AS total_hours_on_type
ORDER BY total_hours_on_type DESC
LIMIT 5;

// --------------------------------------------------------
// Е.5 Зависимости: что произойдёт при отказе оборудования?
// --------------------------------------------------------

// Если скип-01 выйдет из строя: какие маршруты прервутся?
MATCH path = (start:Equipment)-[:CONNECTED_TO*]->(broken:Equipment {equipment_id: 'EQ-010'})
RETURN DISTINCT start.name AS affected_equipment,
       start.type AS type,
       length(path) AS distance_in_chain
ORDER BY distance_in_chain;

// Альтернативные маршруты: есть ли путь без EQ-010?
MATCH path = (start:Equipment {type: 'ПДМ'})
    -[:CONNECTED_TO*1..6]->
    (finish:Equipment {type: 'Скиповый подъёмник'})
WHERE NONE(n IN nodes(path) WHERE n.equipment_id = 'EQ-010')
RETURN [n IN nodes(path) | n.name] AS alternative_route,
       length(path) AS hops;


// ============================================================
// Ж. ОБНАРУЖЕНИЕ АНОМАЛЬНЫХ ПАТТЕРНОВ ОБСЛУЖИВАНИЯ
// ============================================================

// --------------------------------------------------------
// Ж.1 Оборудование с частыми поломками
// --------------------------------------------------------

MATCH (e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
WITH e,
     COUNT(me) AS total_events,
     COUNT(CASE WHEN me.type = 'Аварийный' THEN 1 END) AS emergency_events,
     SUM(me.duration_hours) AS total_downtime,
     SUM(me.cost) AS total_cost
WHERE total_events >= 2
RETURN e.name AS equipment,
       e.type AS type,
       total_events,
       emergency_events,
       total_downtime AS downtime_hours,
       total_cost,
       ROUND(toFloat(emergency_events) / total_events * 100) AS emergency_pct
ORDER BY emergency_events DESC, total_cost DESC;

// --------------------------------------------------------
// Ж.2 Операторы и аварийные события
// --------------------------------------------------------

// После работы каких операторов оборудование чаще ломалось?
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent {type: 'Аварийный'})
RETURN o.name AS operator,
       o.qualification AS qualification,
       COLLECT(DISTINCT e.name) AS broken_equipment,
       COUNT(me) AS emergency_events,
       SUM(me.cost) AS total_emergency_cost
ORDER BY emergency_events DESC;

// --------------------------------------------------------
// Ж.3 Временные паттерны обслуживания
// --------------------------------------------------------

// Среднее время между обслуживаниями (MTBM) для каждого оборудования
MATCH (e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)
WITH e, me
ORDER BY me.date
WITH e, COLLECT(me.date) AS dates
WHERE SIZE(dates) > 1
UNWIND RANGE(1, SIZE(dates) - 1) AS i
WITH e, dates[i].epochMillis - dates[i-1].epochMillis AS interval_ms
RETURN e.name AS equipment,
       e.type AS type,
       COUNT(interval_ms) + 1 AS events,
       ROUND(AVG(interval_ms) / 86400000.0) AS avg_days_between_events,
       ROUND(MIN(interval_ms) / 86400000.0) AS min_days_between,
       ROUND(MAX(interval_ms) / 86400000.0) AS max_days_between
ORDER BY avg_days_between_events;

// --------------------------------------------------------
// Ж.4 Граф зависимостей: визуализация
// --------------------------------------------------------

// Полный граф шахты «Северная» для визуализации
MATCH (m:Mine {name: 'Северная'})
OPTIONAL MATCH (e:Equipment)-[:LOCATED_IN]->(m)
OPTIONAL MATCH (o:Operator)-[:WORKS_AT]->(m)
OPTIONAL MATCH (o)-[:OPERATES]->(e)
OPTIONAL MATCH (e)-[:HAS_SENSOR]->(s:Sensor)
OPTIONAL MATCH (e)-[:CONNECTED_TO]->(e2:Equipment)
RETURN m, e, o, s, e2;

// --------------------------------------------------------
// Ж.5 Поиск треугольников (клик) — неожиданные связи
// --------------------------------------------------------

// Ищем «треугольники»: Оператор → Оборудование → Обслуживание ← Оператор
// Если тот же оператор и управляет машиной, и чинит её — это риск
MATCH (o:Operator)-[:OPERATES]->(e:Equipment)-[:REQUIRED_MAINTENANCE]->(me:MaintenanceEvent)-[:MAINTAINED_BY]->(o)
RETURN o.name AS operator_and_mechanic,
       e.name AS equipment,
       me.description AS maintenance,
       me.cost AS cost
ORDER BY me.cost DESC;


// ============================================================
// ПРОВЕРКА: Статистика графа
// ============================================================

// Количество узлов по типам
MATCH (n)
RETURN labels(n)[0] AS label,
       COUNT(*) AS count
ORDER BY count DESC;

// Количество связей по типам
MATCH ()-[r]->()
RETURN type(r) AS relationship_type,
       COUNT(*) AS count
ORDER BY count DESC;

// Общая статистика
MATCH (n)
WITH COUNT(n) AS nodes
MATCH ()-[r]->()
WITH nodes, COUNT(r) AS relationships
RETURN nodes, relationships,
       toFloat(relationships) / nodes AS avg_degree;
