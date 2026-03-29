import marimo

__generated_with = "0.20.2"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from sqlalchemy import create_engine, types

    # Cell 1: Inisialisasi Koneksi
    engine = create_engine("postgresql://admin:admin123@localhost:5433/tech_layoffs_dw")
    return engine, pd, types


@app.cell
def _(pd):
    # Cell 2: Load Data
    # Menggunakan data dari sample yang kamu berikan
    df_layoffs = pd.read_csv("Cleaned_tech_layoffs.csv")
    df_coords = pd.read_csv("layoffs_location_with_coordinates.csv")
    return df_coords, df_layoffs


@app.cell
def _(df_coords, df_layoffs, pd):
    # 3. Data Preparation & Joining (Technical Standardization)
    def prepare_bronze_layer(df_main, df_lookup):
        # Salin data agar tidak mengubah original dataframe di Marimo state
        df = df_main.copy()
    
        # A. Standarisasi Tanggal: Kolom 'Date_layoffs'
        df['Date_layoffs'] = pd.to_datetime(df['Date_layoffs'], errors='coerce')
    
        # B. Pembersihan Key untuk Join
        # 'Location_HQ' (Main) match dengan 'location_HQ' (Lookup)
        df['Location_HQ'] = df['Location_HQ'].str.strip()
        df_lookup['location_HQ'] = df_lookup['location_HQ'].str.strip()
    
        # C. Spatial Joining (Left Join)
        # Kita ambil koordinat dari lookup untuk mengisi data yang mungkin kosong di file utama
        df_merged = pd.merge(
            df, 
            df_lookup[['location_HQ', 'latitude', 'longitude']], 
            left_on='Location_HQ', 
            right_on='location_HQ', 
            how='left',
            suffixes=('', '_ref')
        )
    
        # D. Imputasi Koordinat: Jika latitude/longitude di file utama NULL, isi dari file lookup
        df_merged['latitude'] = df_merged['latitude'].fillna(df_merged['latitude_ref'])
        df_merged['longitude'] = df_merged['longitude'].fillna(df_merged['longitude_ref'])
    
        # E. Cleanup: Hapus kolom hasil join yang tidak diperlukan
        df_final = df_merged.drop(columns=['location_HQ', 'latitude_ref', 'longitude_ref'])
    
        return df_final

    df_bronze_ready = prepare_bronze_layer(df_layoffs, df_coords)
    return (df_bronze_ready,)


@app.cell
def _(df_bronze_ready):
    # Cek apakah ada data yang koordinatnya masih kosong setelah join
    df_bronze_ready[['latitude', 'longitude']].isna().sum()
    return


@app.cell
def _(df_bronze_ready, engine, types):
    # 4. Ingestion ke PostgreSQL Lapisan Bronze
    def ingest_to_postgres(df):
        # Definisikan Schema SQL agar PostgreSQL mengenali tipe data dengan benar
        # Terutama untuk 'Money_Raised_in__mil' dan koordinat
        dtype_map = {
            'Date_layoffs': types.Date,
            'Laid_Off': types.Numeric,
            'Percentage': types.Numeric,
            'Money_Raised_in__mil': types.Numeric,
            'latitude': types.Float,
            'longitude': types.Float,
            'Year': types.Integer
        }
    
        df.to_sql(
            name='bronze_tech_layoffs', 
            con=engine, 
            if_exists='replace', # Gunakan 'replace' untuk pengembangan awal
            index=False,
            dtype=dtype_map
        )
        return f"Ingestion Sukses! {len(df)} baris terkirim ke PostgreSQL."

    status = ingest_to_postgres(df_bronze_ready)
    status
    return


@app.cell
def _(engine):
    import sqlalchemy

    # Gunakan engine yang sama dengan yang kamu pakai untuk ingest
    inspector = sqlalchemy.inspect(engine)
    tables = inspector.get_table_names()

    print(f"Database saat ini: {engine.url.database}")
    print(f"Daftar tabel yang ditemukan: {tables}")

    # Jika 'bronze_tech_layoffs' ada di list, berarti data AMAN.
    if 'bronze_tech_layoffs' in tables:
        # Cek jumlah baris secara langsung
        with engine.connect() as conn:
            count = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM bronze_tech_layoffs")).scalar()
            print(f"Konfirmasi: Tabel ditemukan dengan {count} baris data.")
    else:
        print("Peringatan: Tabel TIDAK ditemukan di database ini!")
    return


if __name__ == "__main__":
    app.run()
