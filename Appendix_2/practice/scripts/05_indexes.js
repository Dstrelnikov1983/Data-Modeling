// ============================================================
// Скрипт 05: Индексы и производительность
// Предприятие «Руда+» — MES-система
// Среда: Yandex StoreDoc (MongoDB-совместимая СУБД)
// IDE: JetBrains DataGrip
// ============================================================
// Выполняйте запросы пошагово, сравнивая результаты explain
// ============================================================


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 1: Сравнение COLLSCAN и IXSCAN                    ║
// ╚════════════════════════════════════════════════════════════╝

// --- 1.1. Текущие индексы ---
print("=== Текущие индексы equipment ===");
db.equipment.getIndexes().forEach(function(idx) {
  print("  " + idx.name + ": " + JSON.stringify(idx.key));
});

// --- 1.2. Запрос БЕЗ индекса (COLLSCAN) ---
print("\n=== Запрос БЕЗ индекса (status) ===");
var explainNoIdx = db.equipment.find({ status: "working" }).explain("executionStats");
print("  winningPlan.stage: " + explainNoIdx.queryPlanner.winningPlan.stage);
print("  totalDocsExamined: " + explainNoIdx.executionStats.totalDocsExamined);
print("  executionTimeMillis: " + explainNoIdx.executionStats.executionTimeMillis);

// --- 1.3. Создание индекса по status ---
db.equipment.createIndex({ status: 1 });
print("\n✓ Индекс {status: 1} создан.");

// --- 1.4. Запрос С индексом (IXSCAN) ---
print("\n=== Запрос С индексом (status) ===");
var explainWithIdx = db.equipment.find({ status: "working" }).explain("executionStats");

// Для explain с индексом winningPlan может иметь вложенную структуру
var stage = explainWithIdx.queryPlanner.winningPlan.stage;
if (explainWithIdx.queryPlanner.winningPlan.inputStage) {
  stage = stage + " → " + explainWithIdx.queryPlanner.winningPlan.inputStage.stage;
}
print("  winningPlan.stage: " + stage);
print("  totalDocsExamined: " + explainWithIdx.executionStats.totalDocsExamined);
print("  executionTimeMillis: " + explainWithIdx.executionStats.executionTimeMillis);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЗАДАНИЕ 1 (самостоятельно):                              ║
// ║  Зафиксируйте totalDocsExamined и executionTimeMillis     ║
// ║  до и после создания индекса. Какой stage используется?   ║
// ║                                                            ║
// ║  Без индекса: COLLSCAN — сканирует ВСЕ документы          ║
// ║  С индексом:  IXSCAN  — сканирует только по индексу       ║
// ╚════════════════════════════════════════════════════════════╝


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 2: Составной индекс (Compound Index)               ║
// ╚════════════════════════════════════════════════════════════╝

// --- 2.1. Частый запрос: оборудование на шахте + статус ---
print("\n=== Запрос mine._id + status БЕЗ составного индекса ===");
var explain1 = db.equipment.find({
  "mine._id": "M001",
  status: "working"
}).explain("executionStats");
print("  totalDocsExamined: " + explain1.executionStats.totalDocsExamined);

// --- 2.2. Создание составного индекса ---
db.equipment.createIndex({ "mine._id": 1, status: 1 });
print("\n✓ Составной индекс {mine._id: 1, status: 1} создан.");

// --- 2.3. Повторный запрос ---
print("\n=== Запрос mine._id + status С составным индексом ===");
var explain2 = db.equipment.find({
  "mine._id": "M001",
  status: "working"
}).explain("executionStats");
print("  totalDocsExamined: " + explain2.executionStats.totalDocsExamined);

// --- 2.4. Индекс покрывает только левый префикс ---
// Этот запрос может использовать составной индекс (mine._id — первое поле)
print("\n=== Запрос только по mine._id ===");
db.equipment.find({ "mine._id": "M001" }).explain("executionStats");

// А этот НЕ использует составной индекс (status — второе поле)
print("\n=== Запрос только по status (без mine._id) ===");
// Будет использовать одиночный индекс {status: 1}, если он есть
db.equipment.find({ status: "maintenance" }).explain("executionStats");


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 3: Индексы для коллекции incidents                 ║
// ╚════════════════════════════════════════════════════════════╝

// --- 3.1. Индекс по severity ---
db.incidents.createIndex({ severity: 1 });
print("\n✓ Индекс {severity: 1} создан для incidents.");

// --- 3.2. Составной индекс: mine_id + type ---
db.incidents.createIndex({ mine_id: 1, type: 1 });
print("✓ Составной индекс {mine_id: 1, type: 1} создан для incidents.");

// --- 3.3. Проверка ---
print("\n=== Индексы incidents ===");
db.incidents.getIndexes().forEach(function(idx) {
  print("  " + idx.name + ": " + JSON.stringify(idx.key));
});


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 4: TTL-индекс (автоудаление)                      ║
// ╚════════════════════════════════════════════════════════════╝

// TTL-индекс автоматически удаляет документы по истечении срока.
// Подходит для данных с ограниченным сроком хранения (логи, телеметрия).

// --- 4.1. TTL-индекс для телеметрии (30 дней) ---
// ВАЖНО: TTL работает только с полями типа Date или ISODate.
// В нашем примере bucket_end — строка, поэтому TTL не сработает.
// Для реального применения необходимо хранить даты как ISODate.

// Пример создания TTL-индекса (для демонстрации):
// db.telemetry_buckets.createIndex(
//   { bucket_end: 1 },
//   { expireAfterSeconds: 2592000 }  // 30 дней = 30 × 24 × 60 × 60
// );
// print("✓ TTL-индекс создан: документы удаляются через 30 дней после bucket_end.");

print("\n--- TTL-индекс ---");
print("TTL-индекс автоматически удаляет документы по истечении срока.");
print("expireAfterSeconds: 2592000 = 30 дней");
print("ВАЖНО: TTL работает только с полями типа ISODate/Date.");


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 5: Текстовый индекс (Full-Text Search)             ║
// ╚════════════════════════════════════════════════════════════╝

// --- 5.1. Создание текстового индекса ---
db.incidents.createIndex({ description: "text" });
print("\n✓ Текстовый индекс {description: 'text'} создан для incidents.");

// --- 5.2. Полнотекстовый поиск ---
print("\n=== Поиск: 'перегрев двигатель' ===");
db.incidents.find(
  { $text: { $search: "перегрев двигатель" } },
  { score: { $meta: "textScore" }, description: 1 }
).sort({ score: { $meta: "textScore" } });

// --- 5.3. Поиск: «трансмиссия» ---
print("\n=== Поиск: 'трансмиссия' ===");
db.incidents.find(
  { $text: { $search: "трансмиссия" } },
  { score: { $meta: "textScore" }, _id: 1, description: 1 }
);

// --- 5.4. Поиск: «вагонетка рельсы» ---
print("\n=== Поиск: 'вагонетка рельсы' ===");
db.incidents.find(
  { $text: { $search: "вагонетка рельсы" } },
  { score: { $meta: "textScore" }, _id: 1, description: 1 }
);


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 6: Индексы для вложенных документов и массивов     ║
// ╚════════════════════════════════════════════════════════════╝

// --- 6.1. Multikey индекс (индекс по элементам массива) ---
db.equipment.createIndex({ "sensors.type": 1 });
print("\n✓ Multikey индекс {sensors.type: 1} создан.");

// --- 6.2. Проверка — поиск по типу датчика ---
print("\n=== Поиск оборудования с GPS-датчиками ===");
var explainGps = db.equipment.find({ "sensors.type": "gps" }).explain("executionStats");
print("  stage: " + JSON.stringify(explainGps.queryPlanner.winningPlan));

// --- 6.3. Индекс по вложенному полю ---
db.equipment.createIndex({ "mine.name": 1 });
print("✓ Индекс {mine.name: 1} создан.");


// ╔════════════════════════════════════════════════════════════╗
// ║  ЧАСТЬ 7: Управление индексами                            ║
// ╚════════════════════════════════════════════════════════════╝

// --- 7.1. Список всех индексов ---
print("\n=== Все индексы equipment ===");
db.equipment.getIndexes().forEach(function(idx) {
  print("  " + idx.name + ": " + JSON.stringify(idx.key));
});

print("\n=== Все индексы incidents ===");
db.incidents.getIndexes().forEach(function(idx) {
  print("  " + idx.name + ": " + JSON.stringify(idx.key));
});

// --- 7.2. Статистика использования индексов ---
// db.equipment.aggregate([{ $indexStats: {} }]);

// --- 7.3. Удаление индекса (по имени) ---
// db.equipment.dropIndex("status_1");
// print("Индекс status_1 удалён.");

// --- 7.4. Удаление всех пользовательских индексов ---
// ВНИМАНИЕ: не удаляйте — это для справки!
// db.equipment.dropIndexes();


// ╔════════════════════════════════════════════════════════════╗
// ║  ЗАДАНИЕ 2 (самостоятельно):                              ║
// ║  1. Создайте составной индекс {equipment_id: 1,           ║
// ║     incident_date: -1} для коллекции incidents            ║
// ║  2. Выполните explain для запроса:                         ║
// ║     db.incidents.find({equipment_id: "EQ005"})            ║
// ║        .sort({incident_date: -1})                         ║
// ║  3. Убедитесь, что используется IXSCAN                   ║
// ╚════════════════════════════════════════════════════════════╝

// Ваш код:



print("\n✓ Скрипт 05 выполнен. Индексы продемонстрированы.");
