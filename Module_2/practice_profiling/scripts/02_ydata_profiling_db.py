"""
Практикум по анализу и моделированию данных
Модуль 2 (доп.): Профилирование данных из PostgreSQL
Предприятие: «Руда+» — добыча железной руды

Скрипт подключается к PostgreSQL и профилирует таблицу ore_production.
Перед запуском отредактируйте параметры подключения ниже.
"""

import os
from pathlib import Path

import pandas as pd
from ydata_profiling import ProfileReport

# --- Параметры подключения к PostgreSQL ---
# Отредактируйте под ваше окружение:
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", "ruda_plus"),
    "user": os.getenv("PG_USER", "student"),
    "password": os.getenv("PG_PASSWORD", "student_password"),
}

# Или используйте переменную окружения DATABASE_URL:
# DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://student:password@localhost:5432/ruda_plus")

SCRIPT_DIR = Path(__file__).resolve().parent
REPORTS_DIR = SCRIPT_DIR.parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def main():
    print("=" * 60)
    print("YData-Profiling: Профилирование из PostgreSQL")
    print("=" * 60)

    # --- Подключение через SQLAlchemy ---
    try:
        from sqlalchemy import create_engine

        connection_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_string)

        print(f"\nПодключение к {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}...")
        print("Пользователь:", DB_CONFIG["user"])

    except ImportError:
        print("[!] Для работы с PostgreSQL нужен пакет sqlalchemy + psycopg2:")
        print("    pip install sqlalchemy psycopg2-binary")
        return

    # --- Запрос данных ---
    query = """
    SELECT
        p.production_id,
        p.mine_id,
        p.production_date,
        p.shift,
        p.block_id,
        p.ore_type,
        p.tonnage_extracted,
        p.fe_content_pct,
        p.moisture_pct,
        p.equipment_id,
        p.status
    FROM ore_production p
    ORDER BY p.production_date, p.shift;
    """

    try:
        print("Выполнение запроса...")
        df = pd.read_sql(query, engine)
        print(f"Загружено: {len(df)} строк, {len(df.columns)} колонок")
    except Exception as e:
        print(f"[!] Ошибка при выполнении запроса: {e}")
        print("\nУбедитесь, что:")
        print("  1. PostgreSQL запущен и доступен")
        print("  2. База данных 'ruda_plus' создана")
        print("  3. Таблица 'ore_production' содержит данные (Модуль 1)")
        print("  4. Параметры подключения в скрипте корректны")
        return

    # --- Профилирование ---
    print("\nГенерация отчёта...")
    report_path = REPORTS_DIR / "profile_ore_production_db.html"

    profile = ProfileReport(
        df,
        title="Руда+ | Добыча руды (из PostgreSQL)",
        explorative=True,
    )
    profile.to_file(report_path)

    print(f"\nОтчёт сохранён: {report_path}")
    print("Откройте файл в браузере для изучения.")

    engine.dispose()


if __name__ == "__main__":
    main()
