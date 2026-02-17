"""
Практикум по анализу и моделированию данных
Модуль 2 (доп.): Профилирование данных с YData-Profiling
Предприятие: «Руда+» — добыча железной руды

Скрипт генерирует HTML-отчёты профилирования для CSV-файлов Модуля 1.
"""

import os
import sys
from pathlib import Path

import pandas as pd
from ydata_profiling import ProfileReport

# --- Пути ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR.parent.parent / "Module_1" / "practice" / "data"
REPORTS_DIR = PROJECT_DIR / "reports"

# Создаём папку для отчётов
REPORTS_DIR.mkdir(exist_ok=True)


def profile_csv(filename: str, title: str, minimal: bool = False) -> pd.DataFrame:
    """Загружает CSV и создаёт HTML-отчёт профилирования."""

    filepath = DATA_DIR / filename
    if not filepath.exists():
        print(f"  [!] Файл не найден: {filepath}")
        sys.exit(1)

    print(f"  Загрузка {filename}...")
    df = pd.read_csv(filepath)
    print(f"  Загружено: {len(df)} строк, {len(df.columns)} колонок")

    report_name = f"profile_{filename.replace('.csv', '')}.html"
    report_path = REPORTS_DIR / report_name

    print(f"  Генерация отчёта ({report_name})...")
    profile = ProfileReport(
        df,
        title=title,
        minimal=minimal,
        explorative=True,
        correlations={
            "auto": {"calculate": True},
            "pearson": {"calculate": True},
            "spearman": {"calculate": True},
        },
    )
    profile.to_file(report_path)
    print(f"  Отчёт сохранён: {report_path}")

    return df


def profile_comparison(df: pd.DataFrame, column: str, values: list, titles: list,
                       report_name: str, main_title: str):
    """Создаёт сравнительный отчёт для двух подмножеств данных."""

    if len(values) < 2:
        print(f"  [!] Недостаточно значений для сравнения по колонке '{column}'")
        return

    df1 = df[df[column] == values[0]]
    df2 = df[df[column] == values[1]]

    if df1.empty or df2.empty:
        print(f"  [!] Одна из подвыборок пуста, сравнение невозможно")
        return

    print(f"  Сравнение: {titles[0]} ({len(df1)} строк) vs {titles[1]} ({len(df2)} строк)")

    report1 = ProfileReport(df1, title=titles[0], minimal=True)
    report2 = ProfileReport(df2, title=titles[1], minimal=True)

    comparison = report1.compare(report2)

    report_path = REPORTS_DIR / report_name
    comparison.to_file(report_path)
    print(f"  Сравнительный отчёт: {report_path}")


def main():
    print("=" * 60)
    print("YData-Profiling: Профилирование данных «Руда+»")
    print("=" * 60)

    # --- 1. Оборудование ---
    print("\n[1/5] Профилирование equipment.csv")
    profile_csv(
        "equipment.csv",
        "Руда+ | Оборудование (equipment)",
    )

    # --- 2. Показания датчиков ---
    print("\n[2/5] Профилирование sensor_readings.csv")
    profile_csv(
        "sensor_readings.csv",
        "Руда+ | Показания датчиков (sensor_readings)",
    )

    # --- 3. Добыча руды ---
    print("\n[3/5] Профилирование ore_production.csv")
    df_prod = profile_csv(
        "ore_production.csv",
        "Руда+ | Добыча руды (ore_production)",
    )

    # --- 4. Простои ---
    print("\n[4/5] Профилирование downtime_events.csv")
    profile_csv(
        "downtime_events.csv",
        "Руда+ | Простои оборудования (downtime_events)",
    )

    # --- 5. Сравнительный отчёт: смена 1 vs смена 2 ---
    print("\n[5/5] Сравнительный отчёт: Смена 1 vs Смена 2")
    if "shift" in df_prod.columns:
        shifts = sorted(df_prod["shift"].dropna().unique())
        if len(shifts) >= 2:
            profile_comparison(
                df_prod,
                column="shift",
                values=[shifts[0], shifts[1]],
                titles=[f"Смена {shifts[0]}", f"Смена {shifts[1]}"],
                report_name="profile_production_comparison.html",
                main_title="Руда+ | Сравнение смен",
            )
        else:
            print("  [!] В данных только одна смена, сравнение невозможно")
    else:
        print("  [!] Колонка 'shift' не найдена")

    # --- Итог ---
    print("\n" + "=" * 60)
    print("Готово! Отчёты сохранены в папке:")
    print(f"  {REPORTS_DIR}")
    print()
    print("Откройте HTML-файлы в браузере для изучения.")
    print("=" * 60)


if __name__ == "__main__":
    main()
