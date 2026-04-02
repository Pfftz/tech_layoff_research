import os

import pandas as pd
from pandas.errors import DatabaseError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Bisa dioverride lewat env var: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")
DB_NAME = os.getenv("DB_NAME", "tech_layoffs_dw")

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=disable"
)


def detect_gold_schema() -> str:
    query = """
    SELECT table_schema
    FROM information_schema.tables
    WHERE table_name = 'v_komparasi_indo_global'
      AND table_schema IN ('gold', 'public_gold')
    ORDER BY CASE WHEN table_schema = 'gold' THEN 0 ELSE 1 END
    LIMIT 1
    """
    schema_df = pd.read_sql(query, ENGINE)
    if schema_df.empty:
        return "gold"
    return str(schema_df.iloc[0]["table_schema"])


def main() -> None:
    print("Mengekstrak data dari Gold Layer...")
    print(f"Koneksi DB: {DB_HOST}:{DB_PORT}/{DB_NAME} (user: {DB_USER})")

    # 1) Data komparasi sektor Indonesia vs Global
    try:
        gold_schema = detect_gold_schema()
        print(f"Schema Gold terdeteksi: {gold_schema}")

        df_komparasi = pd.read_sql(
            f"SELECT * FROM {gold_schema}.v_komparasi_indo_global", ENGINE
        )
        df_komparasi.to_csv("data_komparasi_looker.csv", index=False)
        print("1. File data_komparasi_looker.csv berhasil dibuat")

        # 2) Data peta (spasial) untuk Looker Studio
        query_peta = f"""
        SELECT
            f.total_laid_off,
            c.company_name,
            c.stage_group,
            i.industry_name,
            l.city,
            l.country,
            l.latitude,
            l.longitude,
            d.year,
            d.quarter
        FROM {gold_schema}.fact_layoffs f
        LEFT JOIN {gold_schema}.dim_company c ON f.company_id = c.company_id
        LEFT JOIN {gold_schema}.dim_industry i ON f.industry_id = i.industry_id
        LEFT JOIN {gold_schema}.dim_location l ON f.location_id = l.location_id
        LEFT JOIN {gold_schema}.dim_date d ON f.date_id = d.date_id
        """

        df_peta = pd.read_sql(query_peta, ENGINE)
        df_peta.to_csv("data_peta_looker.csv", index=False)
        print("2. File data_peta_looker.csv berhasil dibuat")

        print("\nSelesai. Tinggal kirim dua file CSV ke temanmu untuk di-upload ke Looker Studio.")
    except OperationalError as exc:
        print("\nGagal konek ke PostgreSQL.")
        print("Pastikan container database jalan dan user/password sesuai.")
        print("Jika sebelumnya pernah ganti kredensial, reset volume DB:")
        print("  docker compose down -v")
        print("  docker compose up -d")
        print("Atau jalankan script dengan env var, contoh:")
        print(
            "  $env:DB_PASSWORD='password_aktual'; "
            "python export_buat_temen.py"
        )
        raise SystemExit(1) from exc
    except DatabaseError as exc:
        if "v_komparasi_indo_global" in str(exc):
            print("\nModel Gold belum tersedia: v_komparasi_indo_global tidak ditemukan.")
            print("Jalankan pipeline/dbt dulu agar schema Gold terbentuk:")
            print("  python run_pipeline.py")
            print("atau")
            print("  cd dbt_layoffs && dbt run --profiles-dir .")
            raise SystemExit(1) from exc
        raise


if __name__ == "__main__":
    main()
