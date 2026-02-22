-- ============================================================
-- Практикум по анализу и моделированию данных
-- Модуль 5. Специализированное моделирование данных
-- Скрипт 3: Аналитические запросы для временных рядов
-- Предприятие: "Руда+" — добыча железной руды
--
-- Содержание:
--   А. Агрегация по временным корзинам (time_bucket)
--   Б. Скользящие средние и оконные функции
--   В. Обнаружение аномалий (Z-score)
--   Г. Анализ трендов и скорость изменения
--   Д. Обнаружение пиков
--   Е. Непрерывные агрегаты (Continuous Aggregates)
--   Ж. Политики хранения данных
--   З. Fallback-запросы для стандартного PostgreSQL
--
-- ВАЖНО: Выполните скрипты 01 и 02 перед этим.
-- ============================================================

SET search_path TO timeseries, public;

-- ============================================================
-- А. АГРЕГАЦИЯ ПО ВРЕМЕННЫМ КОРЗИНАМ (time_bucket)
-- ============================================================
-- time_bucket() — функция TimescaleDB для группировки по
-- произвольным временным интервалам.
-- Аналог date_trunc(), но с поддержкой любого интервала.
-- ============================================================

-- А.1 Средняя температура за каждые 5 минут (EQ-001, 15 марта)
-- Видим нарастание аномалии с утра
SELECT '--- А.1: Агрегация 5 минут (температура EQ-001, 15 марта) ---' AS info;

SELECT time_bucket('5 minutes', reading_time) AS bucket,
       equipment_id,
       ROUND(AVG(value)::numeric, 1) AS avg_temp,
       ROUND(MIN(value)::numeric, 1) AS min_temp,
       ROUND(MAX(value)::numeric, 1) AS max_temp,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE sensor_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-15 06:00:00'
  AND reading_time < '2025-03-15 14:00:00'
GROUP BY bucket, equipment_id
ORDER BY bucket;

-- А.2 Часовые агрегаты всех типов датчиков (EQ-001, 1 марта)
SELECT '--- А.2: Часовая агрегация (EQ-001, 1 марта) ---' AS info;

SELECT time_bucket('1 hour', reading_time) AS hour,
       sensor_type,
       ROUND(AVG(value)::numeric, 2) AS avg_value,
       ROUND(MIN(value)::numeric, 2) AS min_value,
       ROUND(MAX(value)::numeric, 2) AS max_value,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-01'
  AND reading_time < '2025-03-02'
GROUP BY hour, sensor_type
ORDER BY sensor_type, hour;

-- А.3 Суточные агрегаты вибрации по всему оборудованию
-- Видим постепенный рост вибрации у EQ-003
SELECT '--- А.3: Суточная агрегация вибрации (весь месяц) ---' AS info;

SELECT time_bucket('1 day', reading_time) AS day,
       equipment_id,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       ROUND(MAX(value)::numeric, 3) AS max_vibration,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY day, equipment_id
ORDER BY equipment_id, day;

-- А.4 Сравнение оборудования: средние показатели за весь месяц
SELECT '--- А.4: Сводка по оборудованию за месяц ---' AS info;

SELECT equipment_id,
       sensor_type,
       ROUND(AVG(value)::numeric, 2) AS avg_value,
       ROUND(STDDEV(value)::numeric, 2) AS stddev_value,
       ROUND(MIN(value)::numeric, 2) AS min_value,
       ROUND(MAX(value)::numeric, 2) AS max_value,
       COUNT(*) AS total_readings,
       SUM(CASE WHEN quality != 'good' THEN 1 ELSE 0 END) AS anomaly_readings
FROM timeseries.sensor_readings
GROUP BY equipment_id, sensor_type
ORDER BY equipment_id, sensor_type;


-- ============================================================
-- Б. СКОЛЬЗЯЩИЕ СРЕДНИЕ И ОКОННЫЕ ФУНКЦИИ
-- ============================================================
-- Скользящие средние сглаживают шумы и выявляют тренды.
-- Используем оконные функции PostgreSQL.
-- ============================================================

-- Б.1 Скользящее среднее вибрации за 7 и 24 точки (EQ-003)
-- Демонстрирует постепенный рост (износ подшипника)
SELECT '--- Б.1: Скользящие средние вибрации (EQ-003, 15-22 марта) ---' AS info;

SELECT reading_time,
       value AS raw_value,
       ROUND(AVG(value) OVER (
           ORDER BY reading_time
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       )::numeric, 3) AS ma_7,
       ROUND(AVG(value) OVER (
           ORDER BY reading_time
           ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
       )::numeric, 3) AS ma_24,
       -- Разница между коротким и длинным MA (сигнал пересечения)
       ROUND((
           AVG(value) OVER (ORDER BY reading_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
         - AVG(value) OVER (ORDER BY reading_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
       )::numeric, 3) AS ma_crossover
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
  AND equipment_id = 'EQ-003'
  AND reading_time >= '2025-03-15'
  AND reading_time < '2025-03-22'
ORDER BY reading_time;

-- Б.2 Экспоненциальное скользящее среднее (EMA)
-- PostgreSQL не имеет встроенного EMA, но можно аппроксимировать
SELECT '--- Б.2: Кумулятивное среднее температуры (EQ-001, 15 марта) ---' AS info;

SELECT reading_time,
       value,
       ROUND(AVG(value) OVER (
           ORDER BY reading_time
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )::numeric, 2) AS cumulative_avg,
       -- Отклонение от кумулятивного среднего
       ROUND((value - AVG(value) OVER (
           ORDER BY reading_time
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ))::numeric, 2) AS deviation
FROM timeseries.sensor_readings
WHERE sensor_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-15 06:00:00'
  AND reading_time < '2025-03-15 14:00:00'
ORDER BY reading_time;

-- Б.3 Ранжирование показаний: LAG / LEAD
-- Показывает изменение между соседними показаниями
SELECT '--- Б.3: Скорость изменения (LAG/LEAD) ---' AS info;

SELECT reading_time,
       value AS current_value,
       LAG(value) OVER (ORDER BY reading_time) AS prev_value,
       ROUND((value - LAG(value) OVER (ORDER BY reading_time))::numeric, 2) AS change,
       ROUND((
           (value - LAG(value) OVER (ORDER BY reading_time))
           / NULLIF(LAG(value) OVER (ORDER BY reading_time), 0)
           * 100
       )::numeric, 2) AS change_pct
FROM timeseries.sensor_readings
WHERE sensor_type = 'temperature'
  AND equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-15 08:00:00'
  AND reading_time < '2025-03-15 13:00:00'
ORDER BY reading_time;


-- ============================================================
-- В. ОБНАРУЖЕНИЕ АНОМАЛИЙ (Z-SCORE)
-- ============================================================
-- Z-score = (значение - среднее) / стандартное_отклонение
-- Значения с |Z| > 2 считаются аномальными (95% доверительный интервал)
-- Значения с |Z| > 3 — экстремальные (99.7% доверительный интервал)
-- ============================================================

-- В.1 Все аномалии по всему оборудованию и датчикам
SELECT '--- В.1: Аномалии (Z-score > 2) ---' AS info;

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
       ROUND(sr.value::numeric, 2) AS value,
       sr.unit,
       ROUND(s.mean_val::numeric, 2) AS mean,
       ROUND(s.std_val::numeric, 2) AS stddev,
       ROUND(((sr.value - s.mean_val) / NULLIF(s.std_val, 0))::numeric, 2) AS z_score,
       CASE
           WHEN ABS(sr.value - s.mean_val) > 3 * s.std_val THEN 'ЭКСТРЕМАЛЬНАЯ'
           WHEN ABS(sr.value - s.mean_val) > 2 * s.std_val THEN 'АНОМАЛИЯ'
       END AS category
FROM timeseries.sensor_readings sr
JOIN stats s ON sr.equipment_id = s.equipment_id
            AND sr.sensor_type = s.sensor_type
WHERE ABS(sr.value - s.mean_val) > 2 * s.std_val
ORDER BY ABS((sr.value - s.mean_val) / NULLIF(s.std_val, 0)) DESC
LIMIT 50;

-- В.2 Аномалии с привязкой к алертам
-- Сопоставление автоматически обнаруженных аномалий с зарегистрированными алертами
SELECT '--- В.2: Аномалии с привязкой к алертам ---' AS info;

WITH anomalies AS (
    SELECT sr.reading_time,
           sr.equipment_id,
           sr.sensor_type,
           sr.value,
           AVG(sr.value) OVER w AS rolling_avg,
           STDDEV(sr.value) OVER w AS rolling_std
    FROM timeseries.sensor_readings sr
    WINDOW w AS (
        PARTITION BY sr.equipment_id, sr.sensor_type
        ORDER BY sr.reading_time
        ROWS BETWEEN 143 PRECEDING AND CURRENT ROW  -- ~24 часа (144 x 10 мин)
    )
)
SELECT a.reading_time,
       a.equipment_id,
       a.sensor_type,
       ROUND(a.value::numeric, 2) AS value,
       ROUND(a.rolling_avg::numeric, 2) AS rolling_avg,
       ROUND(((a.value - a.rolling_avg) / NULLIF(a.rolling_std, 0))::numeric, 2) AS z_score,
       al.severity AS alert_severity,
       al.message AS alert_message
FROM anomalies a
LEFT JOIN timeseries.alerts al
    ON a.equipment_id = al.equipment_id
   AND a.sensor_type = al.sensor_type
   AND a.reading_time BETWEEN al.alert_time - INTERVAL '30 minutes'
                           AND al.alert_time + INTERVAL '30 minutes'
WHERE a.rolling_std > 0
  AND ABS(a.value - a.rolling_avg) > 2.5 * a.rolling_std
ORDER BY a.reading_time
LIMIT 30;

-- В.3 Подсчёт аномалий по дням и оборудованию
-- Позволяет увидеть «проблемные» дни
SELECT '--- В.3: Количество аномалий по дням ---' AS info;

WITH stats AS (
    SELECT equipment_id,
           sensor_type,
           AVG(value) AS mean_val,
           STDDEV(value) AS std_val
    FROM timeseries.sensor_readings
    GROUP BY equipment_id, sensor_type
)
SELECT time_bucket('1 day', sr.reading_time) AS day,
       sr.equipment_id,
       COUNT(*) FILTER (WHERE ABS(sr.value - s.mean_val) > 2 * s.std_val) AS anomalies_2sigma,
       COUNT(*) FILTER (WHERE ABS(sr.value - s.mean_val) > 3 * s.std_val) AS anomalies_3sigma,
       COUNT(*) AS total_readings
FROM timeseries.sensor_readings sr
JOIN stats s ON sr.equipment_id = s.equipment_id
            AND sr.sensor_type = s.sensor_type
GROUP BY time_bucket('1 day', sr.reading_time), sr.equipment_id
HAVING COUNT(*) FILTER (WHERE ABS(sr.value - s.mean_val) > 2 * s.std_val) > 0
ORDER BY anomalies_3sigma DESC, anomalies_2sigma DESC;


-- ============================================================
-- Г. АНАЛИЗ ТРЕНДОВ
-- ============================================================

-- Г.1 Понедельное сравнение средней вибрации
-- Видим рост вибрации у EQ-003 — сигнал износа
SELECT '--- Г.1: Вибрация по неделям ---' AS info;

SELECT time_bucket('1 week', reading_time) AS week,
       equipment_id,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       ROUND(MAX(value)::numeric, 3) AS max_vibration,
       COUNT(*) AS readings,
       -- Изменение относительно предыдущей недели
       ROUND((AVG(value) - LAG(AVG(value)) OVER (
           PARTITION BY equipment_id ORDER BY time_bucket('1 week', reading_time)
       ))::numeric, 3) AS week_over_week_change
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY week, equipment_id
ORDER BY equipment_id, week;

-- Г.2 Линейная регрессия: оценка скорости роста вибрации
-- regr_slope даёт наклон линии тренда
SELECT '--- Г.2: Линейная регрессия вибрации ---' AS info;

SELECT equipment_id,
       COUNT(*) AS data_points,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       -- Наклон (мм/с за секунду) — переводим в мм/с за неделю
       ROUND((regr_slope(value, EXTRACT(EPOCH FROM reading_time)) * 86400 * 7)::numeric, 4)
           AS trend_per_week,
       -- R² — коэффициент детерминации (качество линейной модели)
       ROUND(regr_r2(value, EXTRACT(EPOCH FROM reading_time))::numeric, 4)
           AS r_squared,
       -- Прогноз: при текущем тренде, когда достигнет 7 мм/с (критический порог)?
       CASE
           WHEN regr_slope(value, EXTRACT(EPOCH FROM reading_time)) > 0
           THEN ROUND((
               (7.0 - regr_intercept(value, EXTRACT(EPOCH FROM reading_time)))
               / regr_slope(value, EXTRACT(EPOCH FROM reading_time))
               - EXTRACT(EPOCH FROM MAX(reading_time))
           )::numeric / 86400, 0)
           ELSE NULL
       END AS days_to_critical
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY equipment_id
ORDER BY trend_per_week DESC;

-- Г.3 Сравнение первой и последней недели
SELECT '--- Г.3: Первая vs последняя неделя ---' AS info;

WITH weekly AS (
    SELECT equipment_id,
           sensor_type,
           CASE
               WHEN reading_time < '2025-03-08' THEN 'Неделя 1'
               WHEN reading_time >= '2025-03-24' THEN 'Неделя 4'
           END AS period,
           AVG(value) AS avg_val,
           STDDEV(value) AS std_val
    FROM timeseries.sensor_readings
    WHERE reading_time < '2025-03-08' OR reading_time >= '2025-03-24'
    GROUP BY equipment_id, sensor_type, period
)
SELECT w1.equipment_id,
       w1.sensor_type,
       ROUND(w1.avg_val::numeric, 2) AS avg_week1,
       ROUND(w4.avg_val::numeric, 2) AS avg_week4,
       ROUND((w4.avg_val - w1.avg_val)::numeric, 2) AS change,
       ROUND(((w4.avg_val - w1.avg_val) / NULLIF(w1.avg_val, 0) * 100)::numeric, 1) AS change_pct
FROM weekly w1
JOIN weekly w4 ON w1.equipment_id = w4.equipment_id
              AND w1.sensor_type = w4.sensor_type
WHERE w1.period = 'Неделя 1'
  AND w4.period = 'Неделя 4'
ORDER BY ABS(w4.avg_val - w1.avg_val) DESC;


-- ============================================================
-- Д. ОБНАРУЖЕНИЕ ПИКОВ (PEAK DETECTION)
-- ============================================================

-- Д.1 Локальные максимумы температуры
-- Показание является пиком, если оно больше обоих соседей
SELECT '--- Д.1: Пики температуры (EQ-001) ---' AS info;

WITH ordered AS (
    SELECT reading_time,
           value,
           LAG(value) OVER (ORDER BY reading_time) AS prev_val,
           LEAD(value) OVER (ORDER BY reading_time) AS next_val
    FROM timeseries.sensor_readings
    WHERE sensor_type = 'temperature'
      AND equipment_id = 'EQ-001'
      AND reading_time >= '2025-03-14'
      AND reading_time < '2025-03-16'
)
SELECT reading_time,
       ROUND(value::numeric, 1) AS peak_value,
       ROUND(prev_val::numeric, 1) AS prev,
       ROUND(next_val::numeric, 1) AS next
FROM ordered
WHERE value > prev_val AND value > next_val
  AND value > 80  -- Только значимые пики (выше 80°C)
ORDER BY value DESC
LIMIT 20;

-- Д.2 Время до пика: сколько минут от нормы до максимума?
SELECT '--- Д.2: Время нарастания аномалии (EQ-001, 15 марта) ---' AS info;

WITH temp_data AS (
    SELECT reading_time,
           value,
           FIRST_VALUE(reading_time) OVER (ORDER BY value DESC) AS peak_time,
           FIRST_VALUE(value) OVER (ORDER BY value DESC) AS peak_value
    FROM timeseries.sensor_readings
    WHERE sensor_type = 'temperature'
      AND equipment_id = 'EQ-001'
      AND reading_time >= '2025-03-15 06:00:00'
      AND reading_time < '2025-03-15 14:00:00'
)
SELECT
    MIN(reading_time) FILTER (WHERE value > 80) AS exceeded_80c_at,
    MIN(reading_time) FILTER (WHERE value > 90) AS exceeded_90c_at,
    MIN(reading_time) FILTER (WHERE value > 100) AS exceeded_100c_at,
    MAX(peak_time) AS peak_time,
    MAX(peak_value) AS peak_value,
    -- Время от 80°C до пика
    EXTRACT(MINUTES FROM (
        MAX(peak_time) - MIN(reading_time) FILTER (WHERE value > 80)
    )) AS minutes_80_to_peak
FROM temp_data;


-- ============================================================
-- Е. НЕПРЕРЫВНЫЕ АГРЕГАТЫ (Continuous Aggregates)
-- ============================================================
-- Непрерывные агрегаты — материализованные представления
-- TimescaleDB, которые автоматически обновляются.
-- Значительно ускоряют повторяющиеся аналитические запросы.
-- ============================================================

-- Е.1 Создание часового агрегата
SELECT '--- Е.1: Создание непрерывного агрегата (hourly) ---' AS info;

-- Удаляем, если существует
DROP MATERIALIZED VIEW IF EXISTS timeseries.sensor_hourly CASCADE;

CREATE MATERIALIZED VIEW timeseries.sensor_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', reading_time) AS hour,
       equipment_id,
       sensor_type,
       AVG(value) AS avg_value,
       MIN(value) AS min_value,
       MAX(value) AS max_value,
       STDDEV(value) AS stddev_value,
       COUNT(*) AS reading_count
FROM timeseries.sensor_readings
GROUP BY hour, equipment_id, sensor_type
WITH NO DATA;

-- Заполнение агрегата
CALL refresh_continuous_aggregate('timeseries.sensor_hourly', NULL, NULL);

-- Проверка
SELECT COUNT(*) AS hourly_records FROM timeseries.sensor_hourly;

-- Е.2 Создание суточного агрегата
SELECT '--- Е.2: Создание непрерывного агрегата (daily) ---' AS info;

DROP MATERIALIZED VIEW IF EXISTS timeseries.sensor_daily CASCADE;

CREATE MATERIALIZED VIEW timeseries.sensor_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', reading_time) AS day,
       equipment_id,
       sensor_type,
       AVG(value) AS avg_value,
       MIN(value) AS min_value,
       MAX(value) AS max_value,
       STDDEV(value) AS stddev_value,
       COUNT(*) AS reading_count
FROM timeseries.sensor_readings
GROUP BY day, equipment_id, sensor_type
WITH NO DATA;

CALL refresh_continuous_aggregate('timeseries.sensor_daily', NULL, NULL);

-- Е.3 Запрос к агрегату (мгновенный ответ вместо полного сканирования)
SELECT '--- Е.3: Запрос к суточному агрегату ---' AS info;

SELECT day,
       equipment_id,
       sensor_type,
       ROUND(avg_value::numeric, 2) AS avg_value,
       ROUND(min_value::numeric, 2) AS min_value,
       ROUND(max_value::numeric, 2) AS max_value,
       reading_count
FROM timeseries.sensor_daily
WHERE sensor_type = 'vibration'
  AND equipment_id IN ('EQ-001', 'EQ-003')
ORDER BY equipment_id, day;

-- Е.4 Настройка автоматического обновления
-- В продакшне агрегаты обновляются по расписанию:
SELECT add_continuous_aggregate_policy('timeseries.sensor_hourly',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('timeseries.sensor_daily',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);


-- ============================================================
-- Ж. ПОЛИТИКИ ХРАНЕНИЯ ДАННЫХ
-- ============================================================

-- Ж.1 Просмотр текущих чанков и их размеров
SELECT '--- Ж.1: Информация о чанках ---' AS info;

SELECT chunk_name,
       hypertable_name,
       range_start,
       range_end,
       is_compressed,
       pg_size_pretty(
           pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))
       ) AS chunk_size
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'timeseries'
  AND hypertable_name = 'sensor_readings'
ORDER BY range_start;

-- Ж.2 Ручное сжатие конкретного чанка (пример)
-- SELECT compress_chunk(c.chunk_name)
-- FROM timescaledb_information.chunks c
-- WHERE c.hypertable_name = 'sensor_readings'
--   AND c.range_end < '2025-03-08'
--   AND NOT c.is_compressed;

-- Ж.3 Удаление старых данных (пример — НЕ ВЫПОЛНЯЙТЕ в учебной базе!)
-- SELECT drop_chunks('timeseries.sensor_readings', older_than => INTERVAL '6 months');

-- Ж.4 Установка политики удаления (пример)
-- SELECT add_retention_policy('timeseries.sensor_readings',
--     drop_after => INTERVAL '1 year',
--     if_not_exists => TRUE
-- );

-- Ж.5 Просмотр всех настроенных политик
SELECT '--- Ж.5: Настроенные политики ---' AS info;

SELECT application_name, schedule_interval, config
FROM timescaledb_information.jobs
WHERE application_name LIKE '%timeseries%'
   OR hypertable_schema = 'timeseries'
ORDER BY application_name;


-- ============================================================
-- З. FALLBACK-ЗАПРОСЫ ДЛЯ СТАНДАРТНОГО PostgreSQL
-- ============================================================
-- Если TimescaleDB недоступен, используйте эти запросы.
-- Замены:
--   time_bucket('X', ts)  →  date_trunc('X', ts)
--   Continuous Aggregate  →  обычный MATERIALIZED VIEW
-- ============================================================

-- З.1 Агрегация по часам (аналог А.2)
-- FALLBACK: используем date_trunc вместо time_bucket
SELECT '--- З.1 [FALLBACK]: Часовая агрегация ---' AS info;

SELECT date_trunc('hour', reading_time) AS hour,
       sensor_type,
       ROUND(AVG(value)::numeric, 2) AS avg_value,
       ROUND(MIN(value)::numeric, 2) AS min_value,
       ROUND(MAX(value)::numeric, 2) AS max_value,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE equipment_id = 'EQ-001'
  AND reading_time >= '2025-03-01'
  AND reading_time < '2025-03-02'
GROUP BY date_trunc('hour', reading_time), sensor_type
ORDER BY sensor_type, hour;

-- З.2 Суточная агрегация (аналог А.3)
-- FALLBACK
SELECT '--- З.2 [FALLBACK]: Суточная агрегация вибрации ---' AS info;

SELECT date_trunc('day', reading_time) AS day,
       equipment_id,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       ROUND(MAX(value)::numeric, 3) AS max_vibration,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY date_trunc('day', reading_time), equipment_id
ORDER BY equipment_id, day;

-- З.3 Понедельное сравнение (аналог Г.1)
-- FALLBACK: используем date_trunc('week', ...)
SELECT '--- З.3 [FALLBACK]: Понедельное сравнение ---' AS info;

SELECT date_trunc('week', reading_time) AS week,
       equipment_id,
       ROUND(AVG(value)::numeric, 3) AS avg_vibration,
       ROUND(MAX(value)::numeric, 3) AS max_vibration,
       COUNT(*) AS readings
FROM timeseries.sensor_readings
WHERE sensor_type = 'vibration'
GROUP BY date_trunc('week', reading_time), equipment_id
ORDER BY equipment_id, week;

-- З.4 Материализованное представление (аналог Е.1)
-- FALLBACK: обычный MATERIALIZED VIEW (без автообновления!)
SELECT '--- З.4 [FALLBACK]: Материализованное представление ---' AS info;

DROP MATERIALIZED VIEW IF EXISTS timeseries.sensor_hourly_fallback;

CREATE MATERIALIZED VIEW timeseries.sensor_hourly_fallback AS
SELECT date_trunc('hour', reading_time) AS hour,
       equipment_id,
       sensor_type,
       AVG(value) AS avg_value,
       MIN(value) AS min_value,
       MAX(value) AS max_value,
       STDDEV(value) AS stddev_value,
       COUNT(*) AS reading_count
FROM timeseries.sensor_readings
GROUP BY date_trunc('hour', reading_time), equipment_id, sensor_type;

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_sensor_hourly_fb_equip
    ON timeseries.sensor_hourly_fallback (equipment_id, sensor_type, hour);

-- Для обновления (вручную или по расписанию через pg_cron):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY timeseries.sensor_hourly_fallback;

-- З.5 Ручное заполнение таблицы агрегатов (аналог непрерывного агрегата)
SELECT '--- З.5 [FALLBACK]: Заполнение таблицы часовых агрегатов ---' AS info;

TRUNCATE TABLE timeseries.equipment_metrics_hourly;

INSERT INTO timeseries.equipment_metrics_hourly
    (hour_start, equipment_id, sensor_type, avg_value, min_value, max_value, stddev_value, reading_count, anomaly_count)
WITH stats AS (
    SELECT equipment_id, sensor_type,
           AVG(value) AS global_avg,
           STDDEV(value) AS global_std
    FROM timeseries.sensor_readings
    GROUP BY equipment_id, sensor_type
)
SELECT date_trunc('hour', sr.reading_time) AS hour_start,
       sr.equipment_id,
       sr.sensor_type,
       AVG(sr.value),
       MIN(sr.value),
       MAX(sr.value),
       STDDEV(sr.value),
       COUNT(*),
       COUNT(*) FILTER (WHERE ABS(sr.value - s.global_avg) > 2 * s.global_std)
FROM timeseries.sensor_readings sr
JOIN stats s ON sr.equipment_id = s.equipment_id
            AND sr.sensor_type = s.sensor_type
GROUP BY date_trunc('hour', sr.reading_time), sr.equipment_id, sr.sensor_type;

SELECT '--- Агрегатов записано: ' || COUNT(*) || ' ---' AS info
FROM timeseries.equipment_metrics_hourly;


-- ============================================================
-- ИТОГОВЫЙ ЗАПРОС: Дашборд состояния оборудования
-- ============================================================
-- Этот запрос объединяет все метрики в единый отчёт
-- для каждого оборудования за последние 24 часа.
-- Используйте его как основу для MES-дашборда.
-- ============================================================

SELECT '=== ДАШБОРД: Состояние оборудования (последние 24 часа) ===' AS info;

WITH last_day AS (
    SELECT equipment_id,
           sensor_type,
           AVG(value) AS avg_val,
           MAX(value) AS max_val,
           MIN(value) AS min_val,
           STDDEV(value) AS std_val,
           COUNT(*) AS readings,
           SUM(CASE WHEN quality = 'bad' THEN 1 ELSE 0 END) AS bad_readings
    FROM timeseries.sensor_readings
    WHERE reading_time >= '2025-03-31 00:00:00'
      AND reading_time < '2025-04-01'
    GROUP BY equipment_id, sensor_type
),
alert_counts AS (
    SELECT equipment_id,
           COUNT(*) AS alerts_today
    FROM timeseries.alerts
    WHERE alert_time >= '2025-03-25'
    GROUP BY equipment_id
)
SELECT ld.equipment_id,
       ld.sensor_type,
       ROUND(ld.avg_val::numeric, 2) AS avg_value,
       ROUND(ld.max_val::numeric, 2) AS max_value,
       ROUND(ld.std_val::numeric, 2) AS volatility,
       ld.bad_readings,
       COALESCE(ac.alerts_today, 0) AS recent_alerts,
       CASE
           WHEN ld.bad_readings > 0 THEN 'КРИТИЧНО'
           WHEN ld.std_val > ld.avg_val * 0.3 THEN 'ВНИМАНИЕ'
           ELSE 'НОРМА'
       END AS status
FROM last_day ld
LEFT JOIN alert_counts ac ON ld.equipment_id = ac.equipment_id
ORDER BY
    CASE WHEN ld.bad_readings > 0 THEN 0 ELSE 1 END,
    ld.equipment_id, ld.sensor_type;

SELECT '=== Аналитические запросы завершены ===' AS info;
