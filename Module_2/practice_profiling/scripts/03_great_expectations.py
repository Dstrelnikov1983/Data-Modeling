"""
Практикум по анализу и моделированию данных
Модуль 2 (доп.): Валидация данных с Great Expectations
Предприятие: «Руда+» — добыча железной руды

Скрипт создаёт Expectation Suite для каждого CSV-файла Модуля 1,
валидирует «чистые» данные, затем создаёт «грязную» копию и показывает,
как GX ловит ошибки качества.
"""

import copy
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration

# --- Пути ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR.parent.parent / "Module_1" / "practice" / "data"


# ============================================================
# Вспомогательные функции
# ============================================================

def print_header(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_results(results: dict, dataset_name: str):
    """Выводит результаты валидации в читаемом формате."""
    total = len(results["results"])
    passed = sum(1 for r in results["results"] if r["success"])
    failed = total - passed

    status = "PASS" if results["success"] else "FAIL"
    color_start = ""

    print(f"\n  {dataset_name}: {passed}/{total} {status}")

    if not results["success"]:
        for r in results["results"]:
            if not r["success"]:
                exp_type = r["expectation_config"]["expectation_type"]
                column = r["expectation_config"].get("kwargs", {}).get("column", "—")
                # Формируем читаемое описание
                short_type = exp_type.replace("expect_column_values_to_", "").replace("expect_column_", "").replace("expect_table_", "table: ")
                print(f"    ✗ {column}: {short_type}")
    else:
        print(f"    Все {total} проверок пройдены успешно")


# ============================================================
# Функции валидации для каждого датасета
# ============================================================

def validate_equipment(context, df: pd.DataFrame) -> dict:
    """Валидация equipment.csv."""

    ds = context.sources.add_or_update_pandas("equipment_ds")
    asset = ds.add_dataframe_asset("equipment")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # --- Структурные проверки ---
    expectations = [
        # PK: не NULL, уникальный
        batch.expect_column_values_to_not_be_null("equipment_id"),
        batch.expect_column_values_to_be_unique("equipment_id"),

        # Обязательные поля
        batch.expect_column_values_to_not_be_null("equipment_name"),
        batch.expect_column_values_to_not_be_null("mine_id"),

        # Допустимые значения
        batch.expect_column_values_to_be_in_set(
            "mine_id", ["MINE-1", "MINE-2"]
        ),
        batch.expect_column_values_to_be_in_set(
            "status", ["В работе", "На ТО", "Простой"]
        ),

        # --- Бизнес-правила ---
        batch.expect_column_values_to_be_between(
            "year_manufactured", min_value=2010, max_value=2026
        ),
        batch.expect_column_values_to_be_between(
            "engine_hours", min_value=0
        ),
        batch.expect_column_values_to_be_between(
            "max_payload_tons", min_value=0, max_value=100
        ),

        # --- Полнота ---
        batch.expect_table_row_count_to_be_between(min_value=10),
    ]

    # Собираем результаты
    results = {
        "success": all(e["success"] for e in expectations),
        "results": expectations,
    }
    return results


def validate_sensor_readings(context, df: pd.DataFrame, df_equipment: pd.DataFrame) -> dict:
    """Валидация sensor_readings.csv."""

    ds = context.sources.add_or_update_pandas("sensors_ds")
    asset = ds.add_dataframe_asset("sensors")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    equipment_ids = df_equipment["equipment_id"].tolist()

    expectations = [
        # PK
        batch.expect_column_values_to_not_be_null("reading_id"),
        batch.expect_column_values_to_be_unique("reading_id"),

        # Обязательные поля
        batch.expect_column_values_to_not_be_null("equipment_id"),
        batch.expect_column_values_to_not_be_null("sensor_type"),
        batch.expect_column_values_to_not_be_null("reading_timestamp"),

        # Допустимые значения
        batch.expect_column_values_to_be_in_set(
            "quality_flag", ["OK", "WARN", "ALARM"]
        ),

        # Бизнес-правила
        batch.expect_column_values_to_be_between(
            "reading_value", min_value=0
        ),

        # Ссылочная целостность: equipment_id должен быть в справочнике
        batch.expect_column_values_to_be_in_set(
            "equipment_id", equipment_ids
        ),

        # Полнота
        batch.expect_table_row_count_to_be_between(min_value=40),
    ]

    results = {
        "success": all(e["success"] for e in expectations),
        "results": expectations,
    }
    return results


def validate_ore_production(context, df: pd.DataFrame) -> dict:
    """Валидация ore_production.csv."""

    ds = context.sources.add_or_update_pandas("production_ds")
    asset = ds.add_dataframe_asset("production")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        # PK
        batch.expect_column_values_to_not_be_null("production_id"),
        batch.expect_column_values_to_be_unique("production_id"),

        # Обязательные поля
        batch.expect_column_values_to_not_be_null("mine_id"),
        batch.expect_column_values_to_not_be_null("equipment_id"),
        batch.expect_column_values_to_not_be_null("production_date"),

        # Допустимые значения
        batch.expect_column_values_to_be_in_set("shift", [1, 2]),
        batch.expect_column_values_to_be_in_set(
            "status", ["Завершена", "Прервана"]
        ),

        # Бизнес-правила: диапазоны
        batch.expect_column_values_to_be_between(
            "tonnage_extracted", min_value=0, max_value=500
        ),
        batch.expect_column_values_to_be_between(
            "fe_content_pct", min_value=0, max_value=100
        ),
        batch.expect_column_values_to_be_between(
            "moisture_pct", min_value=0, max_value=100
        ),

        # Полнота
        batch.expect_table_row_count_to_be_between(min_value=10),
    ]

    results = {
        "success": all(e["success"] for e in expectations),
        "results": expectations,
    }
    return results


def validate_downtime_events(context, df: pd.DataFrame) -> dict:
    """Валидация downtime_events.csv."""

    ds = context.sources.add_or_update_pandas("downtime_ds")
    asset = ds.add_dataframe_asset("downtime")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        # PK
        batch.expect_column_values_to_not_be_null("event_id"),
        batch.expect_column_values_to_be_unique("event_id"),

        # Обязательные поля
        batch.expect_column_values_to_not_be_null("equipment_id"),
        batch.expect_column_values_to_not_be_null("event_type"),

        # Допустимые значения
        batch.expect_column_values_to_be_in_set(
            "severity", ["Низкая", "Средняя", "Высокая", "Критическая", "Плановое"]
        ),
        batch.expect_column_values_to_be_in_set(
            "event_type", ["Незапланированный", "Плановое ТО"]
        ),

        # Бизнес-правила
        batch.expect_column_values_to_be_between(
            "duration_minutes", min_value=1, max_value=1440
        ),

        # Полнота
        batch.expect_table_row_count_to_be_between(min_value=5),
    ]

    results = {
        "success": all(e["success"] for e in expectations),
        "results": expectations,
    }
    return results


# ============================================================
# Создание «грязных» данных
# ============================================================

def create_dirty_data(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт копию ore_production с типичными ошибками качества."""

    dirty = df.copy()

    # 1. NULL в обязательном поле
    dirty.loc[dirty.index[0], "mine_id"] = None

    # 2. Дубликат production_id
    dup_row = dirty.iloc[1].copy()
    dirty = pd.concat([dirty, pd.DataFrame([dup_row])], ignore_index=True)

    # 3. Значение вне диапазона: fe_content_pct = 150%
    dirty.loc[dirty.index[2], "fe_content_pct"] = 150.0

    # 4. Отрицательный тоннаж
    dirty.loc[dirty.index[3], "tonnage_extracted"] = -10.0

    # 5. Некорректный статус
    dirty.loc[dirty.index[4], "status"] = "Неизвестно"

    return dirty


# ============================================================
# Главная функция
# ============================================================

def main():
    print_header("Great Expectations: Валидация данных «Руда+»")

    # --- Загрузка данных ---
    print("\nЗагрузка CSV-файлов...")

    files = {
        "equipment": "equipment.csv",
        "sensor_readings": "sensor_readings.csv",
        "ore_production": "ore_production.csv",
        "downtime_events": "downtime_events.csv",
    }

    dataframes = {}
    for name, filename in files.items():
        filepath = DATA_DIR / filename
        if not filepath.exists():
            print(f"  [!] Файл не найден: {filepath}")
            return
        dataframes[name] = pd.read_csv(filepath)
        print(f"  {filename}: {len(dataframes[name])} строк")

    # --- Создание контекста GX ---
    context = gx.get_context()

    # ==============================
    # ЧАСТЬ 1: Валидация чистых данных
    # ==============================
    print_header("ЧАСТЬ 1: Валидация оригинальных (чистых) данных")

    print("\n[1/4] Валидация equipment.csv")
    res_eq = validate_equipment(context, dataframes["equipment"])
    print_results(res_eq, "equipment.csv")

    print("\n[2/4] Валидация sensor_readings.csv")
    res_sr = validate_sensor_readings(
        context, dataframes["sensor_readings"], dataframes["equipment"]
    )
    print_results(res_sr, "sensor_readings.csv")

    print("\n[3/4] Валидация ore_production.csv")
    res_op = validate_ore_production(context, dataframes["ore_production"])
    print_results(res_op, "ore_production.csv")

    print("\n[4/4] Валидация downtime_events.csv")
    res_dt = validate_downtime_events(context, dataframes["downtime_events"])
    print_results(res_dt, "downtime_events.csv")

    # --- Сводка по чистым данным ---
    all_results = [
        ("equipment.csv", res_eq),
        ("sensor_readings.csv", res_sr),
        ("ore_production.csv", res_op),
        ("downtime_events.csv", res_dt),
    ]

    print("\n" + "+" + "-" * 50 + "+")
    print("|  СВОДКА: Валидация чистых данных                 |")
    print("+" + "-" * 50 + "+")
    for name, res in all_results:
        total = len(res["results"])
        passed = sum(1 for r in res["results"] if r["success"])
        status = "PASS" if res["success"] else "FAIL"
        print(f"|  {name:<28} {passed:>2}/{total:<2} {status:<5}|")
    print("+" + "-" * 50 + "+")

    # ==============================
    # ЧАСТЬ 2: Валидация грязных данных
    # ==============================
    print_header("ЧАСТЬ 2: Валидация 'грязных' данных (ore_production)")

    print("\nСоздание грязной копии ore_production.csv...")
    dirty_df = create_dirty_data(dataframes["ore_production"])
    print(f"  Грязный датасет: {len(dirty_df)} строк (было {len(dataframes['ore_production'])})")
    print("  Внесённые ошибки:")
    print("    - NULL в mine_id (строка 0)")
    print("    - Дубликат production_id (копия строки 1)")
    print("    - fe_content_pct = 150% (строка 2)")
    print("    - tonnage_extracted = -10 (строка 3)")
    print("    - status = 'Неизвестно' (строка 4)")

    # Валидация грязного датасета теми же правилами
    context_dirty = gx.get_context()
    res_dirty = validate_ore_production(context_dirty, dirty_df)
    print_results(res_dirty, "ore_production_dirty.csv")

    total = len(res_dirty["results"])
    failed = sum(1 for r in res_dirty["results"] if not r["success"])

    print("\n" + "+" + "-" * 50 + "+")
    print(f"|  ГРЯЗНЫЕ ДАННЫЕ: {failed}/{total} проверок НЕ пройдены       |")
    print("+" + "-" * 50 + "+")

    for r in res_dirty["results"]:
        exp_type = r["expectation_config"]["expectation_type"]
        column = r["expectation_config"].get("kwargs", {}).get("column", "table")
        status_mark = "  " if r["success"] else "✗ "
        short = exp_type.replace("expect_column_values_to_", "").replace("expect_column_", "").replace("expect_table_", "table: ")
        print(f"|  {status_mark}{column:<20} {short:<27}|")

    print("+" + "-" * 50 + "+")

    # --- Итог ---
    print_header("Работа завершена")
    print("""
  Great Expectations позволяет:
  1. Описать ожидания к данным декларативно
  2. Автоматически проверять каждую загрузку
  3. Ловить ошибки ДО попадания в хранилище
  4. Документировать требования к качеству

  Следующий шаг: интеграция GX в ETL-пайплайн
  (Airflow, dbt, Prefect, Dagster)
""")


if __name__ == "__main__":
    main()
