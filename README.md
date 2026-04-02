# Tech Layoffs Data Warehouse (2020-2025)
## Medallion Architecture | PostgreSQL | dbt | Python

Sistem Data Warehouse terintegrasi untuk analisis historis dampak PHK industri teknologi global dengan fokus komparasi Indonesia vs Global.

---

## 🎯 Objective
- **Data Integration:** Menyatukan data layoffs historis (2020-2025) dari berbagai sumber
- **Standardization:** Normalisasi dan pembersihan data di layer Silver menggunakan dbt
- **Analysis:** Star Schema untuk mendukung analisis multi-dimensional (Geographic, Sectoral, Temporal)
- **Comparative View:** Indonesia vs Global aggregation dengan breakdown per tahun, kuartal,  industri

---

## 🏗 Tech Stack
| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Python 3.13 + Pandas + SQLAlchemy | Extract dari CSV → PostgreSQL Bronze |
| **Warehouse** | PostgreSQL 15 (PostGIS) | Storage & spatial data support |
| **Transform** | dbt 1.11 | Medallion Architecture (Bronze → Silver → Gold) |
| **Testing** | dbt tests | 24 data quality checks (uniqueness, foreign keys, not-null) |

---

## 📂 Project Structure
```
.
├── docker-compose.yml              # PostgreSQL container config
├── requirements.txt                # Python dependencies
├── PythonIngestion.py              # Bronze layer ingestion script
├── run_pipeline.py                 # Main pipeline orchestrator
├── dbt_layoffs/                    # dbt project root
│   ├── dbt_project.yml             # dbt config (schemas, materialization)
│   ├── profiles.yml                # Database connection (5432)
│   └── models/
│       ├── staging/
│       │   ├── sources.yml         # Source mapping (bronze_tech_layoffs)
│       │   └── stg_layoffs.sql     # Column standardization
│       ├── silver/
│       │   └── layoffs_standardized.sql  # Industry + stage normalization
│       └── gold/
│           ├── dim_company.sql             # Company dimension
│           ├── dim_industry.sql            # Industry dimension
│           ├── dim_location.sql            # Geographic dimension
│           ├── dim_date.sql                # Date dimension
│           ├── fact_layoffs.sql            # Central fact table
│           ├── v_komparasi_indo_global.sql # **NEW** Comparative view
│           └── schema.yml                  # Table documentation & tests
├── Cleaned_tech_layoffs.csv
├── layoffs_location_with_coordinates.csv
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.13+
- PostgreSQL client tools (optional, for manual queries)

### Installation & Setup

**1. Clone & navigate to project:**
```bash
cd c:\Users\Lenovo\Downloads\PROJET\datawarehouseing
```

**2. Create Python virtual environment:**
```bash
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Start PostgreSQL container:**
```bash
docker-compose up -d
# Wait 10-15 seconds for initialization
docker ps  # Verify postgres_layoffs listening on 5432
```

**5. Run complete pipeline:**
```bash
python run_pipeline.py
```

### Pipeline Phases

#### Phase 1: Bronze Layer (Data Ingestion)
- **Script:** `PythonIngestion.py`
- **Input:** CSV files (2 dataset)
- **Output:** `public.bronze_tech_layoffs` table (~1,745 rows)
- **Time:** ~1-2 seconds

#### Phase 2: dbt Transformations (Silver → Gold)
- **Silver:** Standardization + normalization
- **Gold:** Star Schema (1 fact + 4 dimensions + 1 analytical view)
- **Output:** 8 models total
- **Time:** ~1 second
- **Metrics:**
  - Companies: 1,374
  - Industries: 58
  - Locations: 184
  - Dates: 741
  - Facts: 1,745 rows
  - Komparasi rows: 396

#### Phase 3: Data Quality Tests
- **Tests:** 24 automated checks
- **Coverage:** Uniqueness, NOT NULL, Foreign Keys, Relationships
- **Result:** All PASS ✓
- **Time:** ~1 second

---

## 🔍 Key Features

### 1. **Industry Normalization** (Silver Layer)
Menghubungkan berbagai varian pembuatan nama industri ke kategori standar:
- `Finance | Fintech` → **Fintech**
- `Transportation | Transportion` → **Transportation**
- Keep others as-is

### 2. **Funding Stage Grouping** (Silver Layer)
Mengkategorisasi 20+ stage menjadi 5 kelompok:
- **Early Stage:** Seed, Series A
- **Growth Stage:** Series B, C, D
- **Late Stage:** Series E-J
- **Mature/Public:** Post-IPO, Acquired, Private Equity, Subsidiary
- **Unknown:** NULL atau Unknown

### 3. **Geographic Enrichment** (Silver Layer)
Mengintegrasikan koordinat latitude/longitude untuk setiap lokasi HQ.

### 4. **Star Schema** (Gold Layer)
Central fact table (`fact_layoffs`) dengan joins ke:
- `dim_company` → Company info + funding stage
- `dim_industry` → Clean industry classification
- `dim_location` → Geographic + spatial data
- `dim_date` → Day, Month, Quarter, Year extraction

### 5. **Indonesia vs Global Comparison** (Gold Layer - NEW)
**Model:** `v_komparasi_indo_global`

Aggregates by:
- **Year, Quarter, Industry**
- **Region Scope:** Indonesia | Global
- **Metrics:**
  - `total_companies_impacted` (distinct company count)
  - `total_layoffs` (sum of layoff counts)

Contoh query:
```sql
SELECT * FROM gold.v_komparasi_indo_global
WHERE year = 2023 
  AND region_scope = 'Indonesia' 
  AND industry = 'Fintech'
ORDER BY quarter;
```

---

## 📊 Database Architecture

### Connection Details
- **Host:** localhost
- **Port:** 5432
- **Database:** tech_layoffs_dw
- **User:** admin
- **Password:** admin123

### Schema Layout
- **public:** Sources + Bronze tables
- **staging:** Staging views
- **silver:** Standardized tables
- **gold:** Dimensional tables + analytical views

---

## 🧪 Verification Checklist

After running `python run_pipeline.py`, verify success:

```bash
# Check table row counts
docker exec postgres_layoffs psql -U admin -d tech_layoffs_dw -c "
SELECT 
  'bronze_tech_layoffs' as table_name, count(*) FROM public.bronze_tech_layoffs
UNION ALL
SELECT 'dim_company', count(*) FROM gold.dim_company
UNION ALL
SELECT 'dim_industry', count(*) FROM gold.dim_industry
UNION ALL
SELECT 'dim_location', count(*) FROM gold.dim_location
UNION ALL
SELECT 'dim_date', count(*) FROM gold.dim_date
UNION ALL
SELECT 'fact_layoffs', count(*) FROM gold.fact_layoffs
UNION ALL
SELECT 'v_komparasi_indo_global', count(*) FROM gold.v_komparasi_indo_global;
"
```

**Expected Output:**
```
         table_name         | count
----------------------------+-------
 bronze_tech_layoffs        |  1745
 dim_company                |  1374
 dim_industry               |    58
 dim_location               |   184
 dim_date                   |   741
 fact_layoffs               |  1745
 v_komparasi_indo_global    |   396
```

---

## 🔧 Troubleshooting

### Error: "Password authentication failed"
**Solution:** 
- Verify `profiles.yml` port matches docker-compose: `5432`
- Verify connection string in `PythonIngestion.py`
- Run: `docker-compose down -v && docker-compose up -d` to reset

### Error: "Cannot drop table...dependent objects"
**Solution:** Clean database before re-running:
```bash
docker exec postgres_layoffs psql -U admin -d tech_layoffs_dw -c \
  "DROP SCHEMA IF EXISTS public, staging, silver, gold CASCADE; \
   CREATE SCHEMA public; CREATE SCHEMA staging; CREATE SCHEMA silver; CREATE SCHEMA gold;"
```

### dbt models not building
**Solution:** Ensure dbt profiles.yml pointing to correct host/port:
```bash
cd dbt_layoffs
dbt debug --profiles-dir .
```

---

## 📈 Example Queries

### Top 10 Companies by Layoffs
```sql
SELECT 
  c.company_name,
  f.total_laid_off,
  i.industry_name,
  d.year
FROM gold.fact_layoffs f
JOIN gold.dim_company c ON f.company_id = c.company_id
JOIN gold.dim_industry i ON f.industry_id = i.industry_id
JOIN gold.dim_date d ON f.date_id = d.date_id
ORDER BY f.total_laid_off DESC NULLS LAST
LIMIT 10;
```

### Indonesia vs Global Layoffs by Sector (2023)
```sql
SELECT 
  industry,
  region_scope,
  SUM(total_companies_impacted) as companies,
  SUM(total_layoffs) as layoffs
FROM gold.v_komparasi_indo_global
WHERE year = 2023
GROUP BY industry, region_scope
ORDER BY layoffs DESC NULLS LAST;
```

---

## 📝 Development Notes

### dbt Execution
```bash
cd dbt_layoffs

# Full run
dbt run --profiles-dir .

# Tests only
dbt test --profiles-dir .

# Specific model
dbt run -s "tag:gold" --profiles-dir .

# Dry run
dbt compile --profiles-dir .
```

### Python Ingestion Flags
Edit `PythonIngestion.py` to customize:
- `if_exists='replace'` → Clear & reload (default: ✓)
- `if_exists='append'` → Append only new rows
- `if_exists='fail'` → Error if table exists

---

## ✅ Test Results

**Latest Run:** 2026-04-02T07:15
- ✓ Bronze Layer: 1,745 rows loaded
- ✓ dbt Transformations: 8 models created (0.95s)
- ✓ Data Quality Tests: 24/24 PASSED (0.85s)
- ✓ Pipeline Status: SUCCESS

---

## 🤝 Contributing
Untuk improvements atau bug reports, hubungi team data engineering.

## 📄 License
Proprietary - Tech Layoffs Analysis Project

---

## 📞 Support
- **dbt Docs:** Run `dbt docs generate && dbt docs serve` for auto-generated documentation
- **PostgreSQL Client:** `psql -h localhost -p 5432 -U admin -d tech_layoffs_dw`
- **Container Logs:** `docker logs postgres_layoffs -f`
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

