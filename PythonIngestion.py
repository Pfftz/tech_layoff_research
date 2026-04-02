#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pandas as pd
from sqlalchemy import create_engine, types, inspect, text

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DB_URL = "postgresql+psycopg://admin:admin123@127.0.0.1:5432/tech_layoffs_dw?sslmode=disable"

def prepare_bronze_layer(df_main, df_lookup):
    df = df_main.copy()
    df['Date_layoffs'] = pd.to_datetime(df['Date_layoffs'], errors='coerce')
    df['Location_HQ'] = df['Location_HQ'].str.strip()
    df_lookup['location_HQ'] = df_lookup['location_HQ'].str.strip()
    df_merged = pd.merge(df, df_lookup[['location_HQ', 'latitude', 'longitude']], 
        left_on='Location_HQ', right_on='location_HQ', how='left', suffixes=('', '_ref'))
    df_merged['latitude'] = df_merged['latitude'].fillna(df_merged['latitude_ref'])
    df_merged['longitude'] = df_merged['longitude'].fillna(df_merged['longitude_ref'])
    df_final = df_merged.drop(columns=['location_HQ', 'latitude_ref', 'longitude_ref'])
    return df_final

def ingest_to_postgres(engine, df):
    dtype_map = {
        'Date_layoffs': types.Date, 'Laid_Off': types.Numeric, 
        'Percentage': types.Numeric, 'Money_Raised_in__mil': types.Numeric,
        'latitude': types.Float, 'longitude': types.Float, 'Year': types.Integer
    }

    table_name = 'bronze_tech_layoffs'
    inspector = inspect(engine)

    # Bootstrap table once if it does not exist, then keep structure stable.
    if not inspector.has_table(table_name, schema='public'):
        df.head(0).to_sql(name=table_name, con=engine, if_exists='fail', index=False, dtype=dtype_map)

    # Avoid DROP TABLE because downstream dbt views depend on this table.
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE public.{table_name}'))

    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, dtype=dtype_map)
    return "Ingestion successful: {} rows sent to PostgreSQL.".format(len(df))

if __name__ == "__main__":
    print("=" * 60)
    print("BRONZE LAYER - Data Ingestion (Tech Layoffs)")
    print("=" * 60)
    try:
        engine = create_engine(DB_URL, echo=False)
        with engine.connect() as conn:
            print("[OK] Connected to PostgreSQL")
        print("Loading CSV files...")
        df_layoffs = pd.read_csv("Cleaned_tech_layoffs.csv")
        df_coords = pd.read_csv("layoffs_location_with_coordinates.csv")
        print("  - Layoffs: {} rows".format(len(df_layoffs)))
        print("  - Coordinates: {} rows".format(len(df_coords)))
        print("Preparing bronze layer...")
        df_bronze = prepare_bronze_layer(df_layoffs, df_coords)
        print("Ingesting to PostgreSQL...")
        status = ingest_to_postgres(engine, df_bronze)
        print(status)
        print("=" * 60)
        print("SUCCESS: Bronze Layer Ingestion Complete")
        print("=" * 60)
    except Exception as e:
        print("ERROR: {}: {}".format(type(e).__name__, str(e)))
        raise
