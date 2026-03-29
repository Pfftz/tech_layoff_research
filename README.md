# Implementasi Arsitektur Medallion pada Data Warehouse untuk Analisis Spasial-Sektoral Dampak PHK pada Industri Teknologi Global (2020-2025)

## 📌 Project Overview / Konsep Riset
Penelitian ini bertujuan untuk membangun sebuah ekosistem *Data Warehouse* terintegrasi yang menyatukan data historis pemutusan hubungan kerja (PHK) global yang sebelumnya terfragmentasi. Melalui siklus ELT (*Extract, Load, Transform*), sistem ini mentransformasikan data mentah dan menyajikan *insight* yang interaktif. 

Fokus utama aplikasi ini adalah visualisasi makro-global yang dapat difilter hingga level mikro-regional. Hal ini memungkinkan pemantauan korelasi antara letak geografis perusahaan dan tingkat kerentanan berbagai sektor industri teknologi (seperti Fintech, Edtech) selama masa krisis 2020-2025.

### ✨ Novelty (Kebaruan)
Kebaruan penelitian ini terletak pada penerapan arsitektur Medallion untuk menangani data publik yang bersifat heterogen dan memiliki tingkat inkonsistensi yang tinggi:
- **Silver Layer Automation:** Berfokus pada proses otomatisasi untuk normalisasi kategorisasi industri dan validasi data geolokasi secara spasial (menggunakan PostGIS).
- **Gold Layer Integrity:** Menjamin integritas data pada *Star Schema*, memungkinkan analisis sektoral yang lebih presisi (berdasarkan tahap pendanaan dan ukuran perusahaan di berbagai wilayah dunia).

### 🎯 Sasaran Pengguna & Tujuan
- **Sasaran Pengguna:** Analis data, peneliti ekonomi digital, praktisi teknologi, dan pengambil kebijakan (*stakeholders*) di sektor ketenagakerjaan serta investasi.
- **Tujuan:** Menyediakan landasan data yang akurat pada *BI Dashboard* (Looker Studio) untuk memfasilitasi pengambilan keputusan strategis dan memetakan dampak krisis teknologi.

---

## 🏗 Tech Stack & Architecture
- **Data Ingestion:** Python (Pandas & SQLAlchemy) untku ekstraksi dataset Kaggle (`layoffs.fyi`).
- **Data Warehouse:** PostgreSQL dengan ekstensi **PostGIS** untuk optimalisasi penyimpanan data spasial / koordinat.
- **Transformation:** **dbt (data build tool)** dengan *Medallion Architecture* (Bronze, Silver, Gold).
- **Visualization:** Looker Studio.

### Medallion Architecture Workflow
1. **Bronze Layer (`public.bronze_tech_layoffs`)**
   - Raw data ingestion dilakukan melalui Python (`PythonIngestion.py`).
2. **Staging Layer (`staging.stg_layoffs`)**
   - View ringan untuk standardisasi penamaan kolom dan tipe data dasar.
3. **Silver Layer (`silver.layoffs_standardized`)**
   - Standardisasi dan pembersihan data. Normalisasi kategori industri dan pengklasifikasian *funding stage*.
4. **Gold Layer (Star Schema)**
   - Dimensional modeling dengan tabel fakta terpusat dan dimensi terisolasi.

## 📊 Entity-Relationship Diagram (Gold Layer)
Berikut adalah ERD untuk Star Schema pada Layer Gold:

```mermaid
erDiagram
    gold_fact_layoffs {
        INT fact_id PK
        INT company_id FK
        INT industry_id FK
        INT location_id FK
        INT date_id FK
        NUMERIC total_laid_off
        NUMERIC percentage
        NUMERIC funds_raised
    }
    gold_dim_company {
        INT company_id PK
        VARCHAR company_name
        VARCHAR stage_group
    }
    gold_dim_industry {
        INT industry_id PK
        VARCHAR industry_name
    }
    gold_dim_location {
        INT location_id PK
        VARCHAR city
        VARCHAR country
        VARCHAR continent
        NUMERIC latitude
        NUMERIC longitude
    }
    gold_dim_date {
        INT date_id PK
        DATE full_date
        INT day
        INT month
        INT quarter
        INT year
    }

    gold_fact_layoffs }|--|| gold_dim_company : "belongs to"
    gold_fact_layoffs }|--|| gold_dim_industry : "in industry"
    gold_fact_layoffs }|--|| gold_dim_location : "located at"
    gold_fact_layoffs }|--|| gold_dim_date : "occurred on"
```

## 🚀 How to Run (Setup Instructions)

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.9+
- dbt-core & dbt-postgres

### 2. Environment Setup
Install the necessary Python dependencies:
```bash
pip install -r requirements.txt
```

### 3. Spin up the Data Warehouse
Start the PostGIS enabled PostgreSQL container:
```bash
docker-compose up -d
```

### 4. Run the Pipeline
Eksekusi keseluruhan proses pipeline (Ingestion -> dbt run -> dbt test):
```bash
python run_pipeline.py
```
Atau jalankan tools individual:
- **Ingestion:** `python PythonIngestion.py`
- **Transformation:** `cd dbt_layoffs && dbt run`
- **Data Quality Check:** `cd dbt_layoffs && dbt test`

