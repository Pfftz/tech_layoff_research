# Implementasi Arsitektur Medallion dan Data Build Tool (dbt) pada Data Warehouse PostgreSQL untuk Analisis Komparatif Spasial-Sektoral Pemutusan Hubungan Kerja Sektor Teknologi: Studi Kasus Indonesia vs Global (2020–2025)

---

## Bab 1 — Pendahuluan

### 1.1 Latar Belakang

Fenomena pemutusan hubungan kerja massal (*tech layoffs*) di sektor teknologi telah menjadi isu global yang memiliki implikasi ekonomi signifikan sejak tahun 2020 hingga 2025. Pandemi COVID-19, kontraksi modal ventura, serta restrukturisasi strategis korporasi teknologi telah memicu gelombang efisiensi tenaga kerja di berbagai perusahaan rintisan (*startup*) maupun korporasi teknologi multinasional. Berdasarkan dataset publik yang dikompilasi oleh layoffs.fyi dan tersedia di platform Kaggle, total 1.745 insiden *layoffs* tercatat dalam kurun waktu tersebut, melibatkan perusahaan dari 58 sektor industri yang tersebar di 184 lokasi unik di berbagai benua, termasuk Indonesia.

Di Indonesia, sejumlah perusahaan teknologi berskala besar turut terdampak secara langsung. Data menunjukkan bahwa perusahaan-perusahaan Indonesia seperti Gojek (Jakarta, 430 karyawan di-PHK pada Q2 2020), GoTo Group (Jakarta, 1.300 karyawan pada Q4 2022), KoinWorks (Jakarta, 70 karyawan pada Q4 2022), dan Ajaib (Jakarta, 67 karyawan pada Q4 2022) mengalami gelombang efisiensi serupa dengan tren global. Fenomena ini menimbulkan pertanyaan penting mengenai derajat kerentanan sektor teknologi Indonesia relatif terhadap dinamika *layoffs* global, khususnya pada sektor-sektor seperti *Fintech* dan *Transportation* yang mendominasi ekosistem *startup* Indonesia.

Namun demikian, analisis komparatif yang mendalam terhambat oleh kendala teknis fundamental dalam aspek rekayasa data. Dataset publik *tech layoffs* umumnya memiliki karakteristik sebagai berikut: (1) inkonsistensi penamaan sektor industri—sebagai contoh, varian 'Finance', 'Fintech', dan 'Financial Technology' dapat merujuk pada entitas industri yang sama; (2) ketiadaan struktur dimensional yang memadai untuk analisis berbasis *Business Intelligence* (BI); serta (3) potensi duplikasi baris(*Cartesian product*) pada operasi *join* data geospasial akibat ketidakpresisian kunci relasi spasial. Permasalahan-permasalahan ini membuat data mentah tidak dapat langsung diintegrasikan ke alat visualisasi seperti Looker Studio tanpa melalui proses rekayasa data yang terstruktur.

### 1.2 Rumusan Masalah

Berdasarkan latar belakang tersebut, penelitian ini merumuskan permasalahan utama sebagai berikut:
1. Bagaimana merancang dan mengimplementasikan *Data Warehouse* berbasis Arsitektur Medallion (Bronze–Silver–Gold) pada PostgreSQL untuk menstandarisasi dataset *tech layoffs* global yang bersifat inkonsisten?
2. Bagaimana memanfaatkan *Data Build Tool* (dbt) untuk mengotomatisasi proses normalisasi sektoral dan resolusi data spasial guna mengeliminasi anomali *Cartesian product*?
3. Bagaimana menyediakan lapisan data analitik (*data mart*) yang siap pakai bagi *BI developer* untuk melakukan analisis komparatif antara kerentanan sektor teknologi Indonesia dan tren global secara spasial dan temporal?

### 1.3 Tujuan Penelitian

Penelitian ini bertujuan untuk:
1. Membangun *pipeline* ELT (*Extract, Load, Transform*) end-to-end yang mengintegrasikan ingesti data Python, transformasi dbt, dan validasi kualitas data otomatis.
2. Mengimplementasikan pemodelan *Star Schema* (1 tabel fakta, 4 tabel dimensi) pada *Gold Layer* PostgreSQL yang dioptimalkan untuk kueri analitik multi-dimensional.
3. Merancang *view* analitik komparatif `v_komparasi_indo_global` yang mengagregasi data berdasarkan Tahun, Kuartal, Industri, dan cakupan regional (Indonesia vs Global) untuk digunakan langsung oleh Looker Studio.

### 1.4 Kebaruan (Novelty)

Kebaruan penelitian ini terletak pada aspek rekayasa data berikut:
1. Implementasi Arsitektur Medallion tiga lapis (Bronze → Silver → Gold) secara penuh menggunakan dbt pada PostgreSQL dengan materialisasi terdiferensiasi (*view* untuk *staging*, *table* untuk *silver* dan *gold*).
2. Resolusi masalah *Cartesian product* pada data geospasial melalui strategi *compound join key* yang mengkombinasikan kota, negara, *latitude*, dan *longitude* sebagai kunci gabungan dalam operasi `LEFT JOIN` ke tabel `dim_location`.
3. Orkestrasi *pipeline* end-to-end yang terpadu melalui skrip `run_pipeline.py` yang mengeksekusi ingesti Python, transformasi dbt, dan 24 pengujian kualitas data secara sekuensial dan otomatis.

---

## Bab 2 — Tinjauan Pustaka

### 2.1 Data Warehouse dan Paradigma ELT

*Data Warehouse* (DW) merupakan sistem basis data relasional yang dirancang secara khusus untuk mendukung beban kerja analitik dan pelaporan. Berbeda dari basis data operasional (OLTP), DW mengoptimalkan operasi pembacaan data agregat berskala besar (Kimball & Ross, 2013). Dalam arsitektur DW modern, paradigma *Extract, Load, Transform* (ELT) telah menggantikan pendekatan ETL tradisional. Pada ELT, data mentah diekstraksi dari sumbernya dan dimuat terlebih dahulu ke dalam wadah penyimpanan data analitis (seperti PostgreSQL, BigQuery, atau Snowflake) sebelum proses transformasi dijalankan secara *in-database* menggunakan kapabilitas komputasi SQL yang dimiliki oleh sistem penyimpanan tersebut. Pendekatan ini mengeliminasi kebutuhan akan *staging server* terpisah dan memungkinkan transformasi yang lebih fleksibel serta iteratif.

### 2.2 Arsitektur Medallion (Bronze–Silver–Gold)

Arsitektur Medallion merupakan kerangka kerja organisasional yang mengategorikan maturitas data ke dalam tiga lapisan logis berdasarkan tingkat kesiapan analitiknya:

1. **Lapisan Bronze (*Raw Data Layer*):** Repositori data mentah dengan format dan skema asli dari sumber data. Lapisan ini berfungsi sebagai *single source of truth* historis dan menjaga *raw fidelity* untuk keperluan audit dan *data lineage*. Dalam konteks PostgreSQL, lapisan ini diimplementasikan sebagai tabel di skema `public`.

2. **Lapisan Silver (*Cleansed/Standardized Layer*):** Merepresentasikan data yang telah melewati proses pembersihan, normalisasi, dan standardisasi. Operasi khas pada lapisan ini meliputi penanganan nilai *NULL*, deduplikasi, normalisasi nomenklatur, dan konversi tipe data. Lapisan ini menjembatani kesenjangan antara data mentah dan kebutuhan analitik.

3. **Lapisan Gold (*Curated/Business Layer*):** Menyediakan struktur data bisnis tingkat tinggi berupa model dimensional yang telah dioptimalkan untuk performa kueri pelaporan. Data pada lapisan ini telah diagregasi, dimodelkan, dan siap dikonsumsi langsung oleh alat *Business Intelligence*.

### 2.3 Pemodelan Data Dimensional — Star Schema

*Star Schema* merupakan pendekatan pemodelan data dimensional yang memisahkan data analitik menjadi dua komponen utama (Kimball & Ross, 2013):

- **Tabel Fakta (*Fact Table*):** Menyimpan metrik kuantitatif atau ukuran (*measures*) yang merepresentasikan peristiwa bisnis. Setiap baris pada tabel fakta merepresentasikan satu observasi atau transaksi historis, dengan kunci asing (*foreign key*) yang mereferensi setiap tabel dimensi.

- **Tabel Dimensi (*Dimension Table*):** Mendeskripsikan atribut kontekstual dari metrik yang tersimpan dalam tabel fakta. Tabel dimensi bersifat deskriptif dan memungkinkan proses *slicing* dan *dicing* data fakta berdasarkan berbagai perspektif analitis (contoh: Waktu, Lokasi, Perusahaan, Industri).

Keunggulan *Star Schema* dalam konteks DW terletak pada kesederhanaan struktur relasionalnya yang mempercepat eksekusi kueri *JOIN* pada alat BI, serta mempermudah pemahaman model data oleh *analyst* non-teknis.

### 2.4 Data Build Tool (dbt) sebagai Orkestrasi Transformasi

dbt (*Data Build Tool*) merupakan kerangka kerja transformasi data yang memungkinkan *analytics engineer* untuk mendefinisikan logika transformasi SQL secara modular, terversikan, dan teruji. Fitur kunci dbt yang relevan dengan penelitian ini meliputi:

1. **SQL Jinja Templating:** dbt menggunakan *Jinja templating engine* pada SQL, memungkinkan penggunaan fungsi `{{ ref('model_name') }}` untuk mendefinisikan ketergantungan antar model secara deklaratif dan menghasilkan *Directed Acyclic Graph* (DAG) yang mencerminkan alur transformasi data.

2. **Materialisasi Terdiferensiasi:** dbt mendukung berbagai strategi materialisasi (*view*, *table*, *incremental*, *ephemeral*) yang dapat dikonfigurasi per model atau per direktori melalui `dbt_project.yml`.

3. **Pengujian Otomatis (*Automated Tests*):** dbt menyediakan mekanisme pengujian deklaratif melalui file `schema.yml` yang mendukung pengujian *uniqueness*, *not_null*, dan *relationships* (integritas referensial antar tabel). Selain itu, pengujian singular (*custom test*) dapat didefinisikan sebagai file SQL terpisah di direktori `tests/`.

4. **Data Lineage:** dbt secara otomatis membangun graf ketergantungan (*DAG*) dari semua model, memungkinkan penelusuran asal-usul data dari sumber hingga lapisan konsumsi.

---

## Bab 3 — Metodologi

Penelitian ini menggunakan pendekatan *Applied Analytics Engineering* dengan arsitektur perangkat lunak yang diilustrasikan pada Gambar 1. Seluruh infrastruktur basis data dijalankan pada kontainer Docker menggunakan citra PostGIS (`postgis/postgis:15-3.4`) yang dipetakan ke port `5433`.

![Gambar 1. Diagram Arsitektur Medallion Pipeline](images/arsitektur_medallion.png)

### 3.1 Arsitektur Infrastruktur dan Lingkungan Pengembangan

Infrastruktur *Data Warehouse* dibangun dengan spesifikasi teknis sebagai berikut:

| Komponen | Teknologi | Versi | Keterangan |
|---|---|---|---|
| Basis Data | PostgreSQL + PostGIS | 15-3.4 | Kontainer Docker, port `5433` |
| Ingesti Data | Python + Pandas + SQLAlchemy | 3.13+ / 2.0+ / 2.1+ | Skrip `PythonIngestion.py` |
| Transformasi | dbt-core + dbt-postgres | 1.7+ | Proyek `dbt_layoffs/` |
| Orkestrasi | Python (`run_pipeline.py`) | 3.13+ | Eksekusi sekuensial 3 fase |
| Kontainerisasi | Docker Compose | - | File `docker-compose.yml` |

Konfigurasi koneksi basis data didefinisikan secara konsisten pada file `profiles.yml` (untuk dbt) dan *connection string* SQLAlchemy (untuk Python):

```
postgresql+psycopg2://admin:admin123@127.0.0.1:5433/tech_layoffs_dw
```

Empat skema basis data diinisialisasi untuk memisahkan lapisan arsitektur: `public` (Bronze), `public_staging` (Staging/View), `public_silver` (Silver), dan `public_gold` (Gold).

### 3.2 Pengumpulan Data dan Ingesti ke Bronze Layer

Tahap pertama melibatkan ekstraksi dua dataset CSV ke dalam lapisan *Bronze*:

1. **`Cleaned_tech_layoffs.csv`** (1.745 baris, 18 kolom): Dataset utama berisi catatan historis *layoffs* dengan atribut perusahaan, lokasi, industri, *funding stage*, jumlah PHK, dan tanggal.
2. **`layoffs_location_with_coordinates.csv`** (243 baris): Dataset pelengkap berisi koordinat *latitude/longitude* untuk setiap lokasi *headquarters* perusahaan.

Proses ingesti diimplementasikan pada skrip `PythonIngestion.py` dengan langkah-langkah sebagai berikut:

**a) Pra-pemrosesan dan Penggabungan Koordinat Spasial:**  
Fungsi `prepare_bronze_layer()` melakukan `pd.merge()` antara dataset utama dan dataset koordinat menggunakan kolom `Location_HQ` sebagai kunci *join*. Strategi *merge* yang diterapkan adalah `how='left'` dengan penanganan kolom duplikat melalui parameter `suffixes=('', '_ref')`. Kolom *latitude* dan *longitude* yang bernilai *NULL* pada dataset utama diisi secara otomatis dari dataset referensi menggunakan `fillna()`, sebelum kolom redundan (`location_HQ`, `latitude_ref`, `longitude_ref`) dihapus.

**b) Ingesti ke PostgreSQL menggunakan SQLAlchemy:**  
Fungsi `ingest_to_postgres()` memanfaatkan `DataFrame.to_sql()` dengan pemetaan tipe data eksplisit (`types.Date`, `types.Numeric`, `types.Float`, `types.Integer`) untuk memastikan ketepatan skema kolom di PostgreSQL. Strategi penulisan menggunakan pola `TRUNCATE + append`—yaitu tabel `bronze_tech_layoffs` di-`TRUNCATE` terlebih dahulu sebelum data baru disisipkan, menghindari `DROP TABLE` yang akan mengganggu dependensi *view/table* dbt di hilir. Pembuatan tabel awal (*bootstrap*) dilakukan secara kondisional menggunakan pemeriksaan `inspector.has_table()`.

**c) Penanganan Encoding:** Untuk mengatasi masalah `UnicodeEncodeError` pada lingkungan Windows dengan *code page* CP1252, skrip menerapkan pembungkusan `sys.stdout` dengan `io.TextIOWrapper` ber-encoding UTF-8.

Hasil akhir tahap ini adalah tabel `public.bronze_tech_layoffs` berisi 1.745 baris—identik dengan jumlah baris sumber CSV, memastikan *row parity* sejak awal.

### 3.3 Transformasi Data menggunakan dbt (Staging → Silver Layer)

Transformasi data dilakukan dalam dua tahap melalui proyek dbt `dbt_layoffs/`, yang dikonfigurasikan pada `dbt_project.yml` dengan materialisasi terdiferensiasi:

```yaml
models:
  dbt_layoffs:
    staging:
      +materialized: view     # Ringan, hanya proxy ke Bronze
      +schema: staging
    silver:
      +materialized: table    # Persistensi hasil cleansing
      +schema: silver
    gold:
      +materialized: table    # Optimasi performa kueri BI
      +schema: gold
```

**a) Staging Layer (`stg_layoffs.sql`) — Dimaterialisasi sebagai *View*:**  
Model *staging* berfungsi sebagai lapisan abstraksi yang mentranslasi penamaan kolom mentah (*double-quoted column names*) ke format yang terstandarisasi menggunakan alias deskriptif. Sebagai contoh, `"Company"` dipetakan ke `company_name`, `"Laid_Off"` ke `laid_off_count`, dan `"Money_Raised_in__mil"` ke `funds_raised_mil`. Sumber data dideklarasikan pada file `sources.yml` yang memetakan tabel `bronze_tech_layoffs` di skema `public` sebagai sumber yang dapat direferensikan melalui fungsi `{{ source('public', 'bronze_tech_layoffs') }}`.

**b) Silver Layer (`layoffs_standardized.sql`) — Dimaterialisasi sebagai *Table*:**  
Model *Silver* mengaplikasikan dua transformasi normalisasi kritis menggunakan ekspresi `CASE WHEN`:

*Normalisasi Nomenklatur Industri:*
```sql
CASE 
    WHEN "Industry" ILIKE '%Finance%' OR "Industry" = 'Fintech' THEN 'Fintech'
    WHEN "Industry" IN ('Transportation', 'Transportion') THEN 'Transportation'
    ELSE "Industry"
END AS industry_clean
```
Logika ini meleburkan varian penamaan seperti 'Finance' dan 'Fintech' ke dalam satu kategori 'Fintech', serta memperbaiki *typo* 'Transportion' menjadi 'Transportation'. Nilai industri lainnya dipertahankan sebagaimana aslinya.

*Kategorisasi Funding Stage:*
```sql
CASE
    WHEN "Stage" IN ('Seed', 'Series A') THEN 'Early Stage'
    WHEN "Stage" IN ('Series B', 'Series C', 'Series D') THEN 'Growth Stage'
    WHEN "Stage" IN ('Series E', 'Series F', ..., 'Series J') THEN 'Late Stage'
    WHEN "Stage" IN ('Post-IPO', 'Acquired', 'Private Equity', 'Subsidiary') THEN 'Mature/Public'
    WHEN "Stage" IS NULL OR "Stage" = 'Unknown' THEN 'Unknown'
    ELSE 'Other'
END AS stage_group
```
Pengelompokan ini merangkum 20+ varian *funding stage* menjadi 5 kategori analitis yang bermakna bisnis.

### 3.4 Pemodelan Dimensional — Gold Layer (Star Schema)

Pada lapisan *Gold*, data yang telah dibersihkan dimodelkan ke dalam *Star Schema* yang terdiri dari 1 tabel fakta sentral dan 4 tabel dimensi, seluruhnya didefinisikan sebagai model dbt:

**a) Tabel Dimensi:**

| Model dbt | Tabel Output | Kolom Utama | Jumlah Baris | Strategi Deduplikasi |
|---|---|---|---|---|
| `dim_company.sql` | `public_gold.dim_company` | `company_id` (PK), `company_name`, `stage_group` | 1.374 | `SELECT DISTINCT` + `ROW_NUMBER()` |
| `dim_industry.sql` | `public_gold.dim_industry` | `industry_id` (PK), `industry_name` | 58 | `SELECT DISTINCT industry_clean` |
| `dim_location.sql` | `public_gold.dim_location` | `location_id` (PK), `city`, `country`, `continent`, `latitude`, `longitude` | 184 | `SELECT DISTINCT` + `CAST(latitude/longitude AS NUMERIC)` |
| `dim_date.sql` | `public_gold.dim_date` | `date_id` (PK), `full_date`, `day`, `month`, `quarter`, `year` | 741 | `SELECT DISTINCT date_layoffs` + `EXTRACT()` |

Setiap *primary key* dimensi dihasilkan melalui fungsi `ROW_NUMBER() OVER(ORDER BY ...)`, memastikan kunci yang deterministik dan berurutan. Dimensi waktu menggunakan fungsi `EXTRACT()` PostgreSQL untuk menurunkan komponen temporal hari, bulan, kuartal, dan tahun dari kolom `date_layoffs`.

**b) Tabel Fakta (`fact_layoffs.sql`):**

Tabel fakta didefinisikan melalui operasi `LEFT JOIN` bertingkat dari tabel *Silver* (`layoffs_standardized`) ke seluruh tabel dimensi. Resolusi *Cartesian product* pada data spasial dicapai melalui *compound join key* pada `dim_location`:

```sql
LEFT JOIN dim_location l
    ON l.city = s.location_hq
    AND l.country = s.country
    AND l.latitude = CAST(s.latitude AS NUMERIC)
    AND l.longitude = CAST(s.longitude AS NUMERIC)
```

Strategi 4-kolom `JOIN` ini memastikan bahwa dua kota berbeda dengan nama identik di negara berbeda (misalnya, kota 'Richmond' di USA vs di Australia) tidak menghasilkan baris duplikat. Penanganan nilai *NULL* pada kolom metrik diterapkan menggunakan `COALESCE(s.laid_off_count, 0)` untuk memastikan integritas agregasi. Tabel fakta akhir berisi tepat 1.745 baris—identik dengan jumlah baris sumber *Bronze*, mengonfirmasi *zero row inflation*.

![Gambar 2. Entity Relationship Diagram (ERD) Star Schema](images/erd_star_schema.png)

### 3.5 Rekayasa View Analitik Komparatif (`v_komparasi_indo_global`)

Sebagai *Golden Output* tingkat tertinggi, model dbt `v_komparasi_indo_global.sql` dirancang secara khusus untuk melayani kebutuhan visualisasi Looker Studio tanpa membebani performa *dashboard* dengan kueri agregasi *on-the-fly*. Logika SQL-nya terdiri dari dua *CTE* (*Common Table Expression*):

**CTE `base`:** Mengklasifikasikan setiap baris ke dalam `region_scope` menggunakan logika:
```sql
CASE
    WHEN country = 'Indonesia' THEN 'Indonesia'
    ELSE 'Global'
END AS region_scope
```

**Kueri akhir:** Mengagregasi data berdasarkan empat dimensi dengan metrik bisnis:
```sql
SELECT
    year, quarter, industry, region_scope,
    COUNT(DISTINCT company_name) AS total_companies_impacted,
    SUM(laid_off_count) AS total_layoffs
FROM base
GROUP BY year, quarter, industry, region_scope
```

Model ini dimaterialisasi sebagai *table* dan menghasilkan 396 baris agregasi, memungkinkan Looker Studio mengeksekusi kueri `SELECT *` sederhana tanpa komputasi agregasi tambahan.

### 3.6 Orkestrasi Pipeline End-to-End

Seluruh alur eksekusi diorkestrasi oleh skrip `run_pipeline.py` yang mengeksekusi tiga fase secara sekuensial:

```
Fase 1: python PythonIngestion.py     → Bronze Layer (1.745 baris)
Fase 2: dbt run --profiles-dir .      → Staging + Silver + Gold (8 model)
Fase 3: dbt test --profiles-dir .     → 24 pengujian kualitas data
```

Skrip ini menerapkan mekanisme resolusi *interpreter* Python dan *executable* dbt secara otomatis melalui fungsi `resolve_python()` dan `resolve_dbt()`, yang secara berurutan memeriksa *virtual environment* (`.venv/Scripts/python.exe`), *system Python*, dan *dbt on PATH*. Jika ada kegagalan pada salah satu fase, eksekusi dihentikan segera melalui `exit(1)`.

---

## Bab 4 — Hasil & Pembahasan

### 4.1 Hasil Implementasi Pipeline dbt

Eksekusi `dbt run` berhasil membangun 8 model secara konsisten tanpa error dalam waktu ~0.95 detik. DAG (*Directed Acyclic Graph*) yang dihasilkan dbt memvisualisasikan ketergantungan antar model:

```
source(bronze_tech_layoffs)
    ├── stg_layoffs (view)
    └── layoffs_standardized (table)
            ├── dim_company (table)
            ├── dim_industry (table)
            ├── dim_location (table)
            ├── dim_date (table)
            ├── fact_layoffs (table)
            └── v_komparasi_indo_global (table)
```

![Gambar 3. dbt Lineage DAG — Directed Acyclic Graph](images/dbt_lineage_dag.png)

Modularitas arsitektur ini memungkinkan eksekusi selektif menggunakan *tag* (`dbt run -s "tag:gold"`) atau *model selector*, sehingga perubahan pada lapisan *Silver* dapat dipropagasi secara otomatis ke seluruh model hilir tanpa intervensi manual.

### 4.2 Validasi dan Integritas Data

Validasi integritas data dilakukan melalui dua mekanisme:

**a) Pengujian Deklaratif dbt (24 tes):**  
File `schema.yml` di direktori `models/gold/` mendefinisikan 24 pengujian otomatis yang mencakup:
- **Uniqueness:** `fact_id`, `company_id`, `industry_id`, `location_id`, `date_id` pada tabel masing-masing.
- **Not NULL:** Seluruh *primary key* dan kolom kritis (`company_name`, `industry_name`, `full_date`, `year`, `quarter`, `industry`, `region_scope`).
- **Referential Integrity (Relationships):** `fact_layoffs.company_id` → `dim_company.company_id`, `fact_layoffs.industry_id` → `dim_industry.industry_id`, `fact_layoffs.location_id` → `dim_location.location_id`, `fact_layoffs.date_id` → `dim_date.date_id`.

Seluruh 24 pengujian LULUS (*PASS*) dalam waktu ~0.85 detik, mengonfirmasi tidak adanya pelanggaran integritas referensial.

![Gambar 4. Hasil Pengujian dbt — 24/24 PASS](images/dbt_test_results.png)

**b) Pengujian Singular — Validasi Koordinat Geospasial (`assert_valid_coordinates.sql`):**  
Sebuah *custom test* SQL memvalidasi bahwa seluruh koordinat pada `dim_location` berada dalam rentang geografis yang valid: *latitude* antara -90° hingga 90° dan *longitude* antara -180° hingga 180°. Pengujian ini mengembalikan 0 baris (*PASS*), mengonfirmasi tidak adanya koordinat anomali.

**c) Verifikasi Row Parity:**  
Perbandingan jumlah baris antar lapisan mengkonfirmasi konsistensi *pipeline*:

| Tabel | Jumlah Baris |
|---|---|
| `public.bronze_tech_layoffs` | 1.745 |
| `public_gold.fact_layoffs` | 1.745 |
| `public_gold.dim_company` | 1.374 |
| `public_gold.dim_industry` | 58 |
| `public_gold.dim_location` | 184 |
| `public_gold.dim_date` | 741 |
| `public_gold.v_komparasi_indo_global` | 396 |

Identiknya jumlah baris antara `bronze_tech_layoffs` dan `fact_layoffs` (1.745) mengonfirmasi bahwa *zero row inflation* tercapai—tidak terjadi *Cartesian product* maupun kehilangan data selama proses transformasi 3 lapis.

### 4.3 Output Analitik Sektoral dan Spasial

*View* `v_komparasi_indo_global` berhasil menghasilkan 396 baris agregasi komparatif yang siap dikonsumsi langsung oleh Looker Studio. Berikut contoh representasi parsial dari data output (*sample*):

| year | quarter | industry | region_scope | total_companies_impacted | total_layoffs |
|---|---|---|---|---|---|
| 2020 | 2 | Transportation | Indonesia | 1 | 430 |
| 2022 | 4 | Fintech | Indonesia | 2 | 137 |
| 2022 | 4 | Retail | Indonesia | 3 | 494 |
| 2022 | 4 | Transportation | Indonesia | 1 | 1.300 |
| 2023 | 1 | Transportation | Indonesia | 1 | 360 |
| 2020 | 2 | Transportation | Global | 20 | 12.424 |
| 2022 | 4 | Fintech | Global | 34 | 4.197 |
| 2023 | 1 | Retail | Global | 19 | 20.234 |

![Gambar 5. Tabel Lengkap Hasil View Komparasi Indonesia vs Global](images/tabel_komparasi.png)

Berdasarkan data ini, dapat diidentifikasi sejumlah temuan analitis:

1. **Konsentrasi Sektoral Indonesia:** Perusahaan Indonesia yang terdampak *layoffs* terkonsentrasi pada 3 sektor: *Transportation* (Gojek Q2 2020, GoTo Group Q4 2022, LinkAja Q1 2023), *Fintech* (KoinWorks, Ajaib Q4 2022), dan *Retail* (Bukalapak, Tokopedia, JD.ID Q4 2022). Temuan ini kontras dengan *landscape* global yang melibatkan 58 sektor industri.

2. **Asimetri Skala:** Pada kuartal puncak Q4 2022, sektor *Fintech* Indonesia mencatat 137 *layoffs* dari 2 perusahaan, sedangkan tren global pada kuartal yang sama menunjukkan 4.197 *layoffs* dari 34 perusahaan. Rasio ini menunjukkan keterlibatan Indonesia sebesar ~3.3% dari total *layoffs* sektor *Fintech* global.

3. **Temporal Lag:** *Layoffs* sektor *Transportation* Indonesia dimulai pada Q2 2020 (Gojek), satu kuartal lebih lambat dibandingkan gelombang awal di pasar global (Q1 2020), mengindikasikan adanya *lag* transmisi dampak ekonomi. 

Selain itu, skrip utilitas `export_buat_temen.py` berhasil mengekspor dua dataset final ke format CSV untuk keperluan Looker Studio:
- **`data_komparasi_looker.csv`:** 396 baris dari `v_komparasi_indo_global`
- **`data_peta_looker.csv`:** 1.745 baris untuk visualisasi peta geospasial, menggabungkan tabel fakta dengan seluruh dimensi melalui `LEFT JOIN` kueri SQL.

![Gambar 6. Dashboard Looker Studio](images/looker_dashboard.png)

---

## Bab 5 — Kesimpulan & Saran

### 5.1 Kesimpulan

Penelitian ini berhasil merancang dan mengimplementasikan *Data Warehouse* berbasis Arsitektur Medallion dan *Data Build Tool* (dbt) pada PostgreSQL 15 (PostGIS) untuk menstandarisasi serta mentransformasi dataset publik *tech layoffs* global (2020–2025). Hasil implementasi menunjukkan bahwa:

1. **Pipeline ELT End-to-End Teruji:** Orkestrasi skrip `run_pipeline.py` berhasil mengeksekusi seluruh alur dari ingesti Bronze (1.745 baris), transformasi Silver (normalisasi industri dan *funding stage*), hingga pemodelan Gold (*Star Schema* dengan 1 tabel fakta, 4 tabel dimensi) secara konsisten dan otomatis. Durasi total eksekusi adalah ~3 detik untuk keseluruhan proses.

2. **Integritas Data Terjamin:** Seluruh 24 pengujian kualitas dbt—mencakup *uniqueness*, *not_null*, *referential integrity*, dan validasi koordinat geospasial—LULUS tanpa kegagalan. *Row parity* antara lapisan Bronze (1.745 baris) dan tabel fakta Gold (1.745 baris) mengkonfirmasi eliminasi total *Cartesian product* melalui strategi *compound spatial join key* 4-kolom.

3. **Penyediaan Data Analitik Komparatif:** *View* `v_komparasi_indo_global` (396 baris) berhasil menyajikan agregasi komparatif Indonesia vs Global berdasarkan Tahun, Kuartal, dan Industri. Data ini siap dikonsumsi langsung oleh Looker Studio tanpa komputasi tambahan, mengurangi beban *dashboard query*.

4. **Temuan Komparatif:** Analisis menunjukkan bahwa ekosistem *startup* Indonesia terkonsentrasi pada 3 sektor (*Transportation*, *Fintech*, *Retail*), kontras dengan diversitas 58 sektor di tingkat global. Kontribusi Indonesia terhadap *tech layoffs* global relatif kecil (~3.3% di sektor *Fintech* Q4 2022) namun berdampak signifikan pada skala ekosistem teknologi domestik.

### 5.2 Saran

Untuk pengembangan lebih lanjut, direkomendasikan:

1. **Otomatisasi Penjadwalan Pipeline:** Mengintegrasikan skrip `run_pipeline.py` ke dalam *workflow orchestrator* seperti Apache Airflow sebagai DAG terjadwal (*scheduled batch processing*) untuk mendukung pembaruan data berkala secara otomatis.

2. **Pengayaan Dimensi Makroekonomi:** Menambahkan tabel dimensi yang memuat indikator makroekonomi (kurs mata uang, suku bunga Bank Indonesia, indeks pendanaan ventura regional) untuk memperkuat analisis kausalitas antara kondisi ekonomi makro dan intensitas *layoffs* di tingkat sektoral.

3. **Penerapan Incremental Materialization dbt:** Mengubah materialisasi model Silver dan Gold dari `table` ke `incremental` untuk mengoptimalkan kinerja *pipeline* saat volume data sumber bertumbuh secara signifikan di masa depan.

4. **Skalabilitas Infrastruktur:** Mengevaluasi migrasi dari PostgreSQL lokal (Docker) ke layanan *managed cloud* seperti Google Cloud SQL atau Supabase untuk mendukung akses multi-pengguna dan ketersediaan tinggi (*high availability*).

---

## Daftar Pustaka

*(Akan dilengkapi sesuai format jurnal SINTA yang dituju)*

---

## Lampiran

### Lampiran A: Struktur Direktori Proyek

```
.
├── docker-compose.yml              # Konfigurasi kontainer PostgreSQL
├── requirements.txt                # Dependensi Python (pandas, sqlalchemy, dbt)
├── PythonIngestion.py              # Skrip ingesti Bronze Layer
├── run_pipeline.py                 # Orkestrator pipeline utama
├── export_buat_temen.py            # Ekspor CSV untuk Looker Studio
├── dbt_layoffs/                    # Proyek dbt
│   ├── dbt_project.yml             # Konfigurasi dbt (skema, materialisasi)
│   ├── profiles.yml                # Koneksi database (port 5433)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml         # Deklarasi sumber (bronze_tech_layoffs)
│   │   │   └── stg_layoffs.sql     # Standardisasi nama kolom
│   │   ├── silver/
│   │   │   └── layoffs_standardized.sql  # Normalisasi industri + stage
│   │   └── gold/
│   │       ├── dim_company.sql
│   │       ├── dim_industry.sql
│   │       ├── dim_location.sql
│   │       ├── dim_date.sql
│   │       ├── fact_layoffs.sql
│   │       ├── v_komparasi_indo_global.sql
│   │       └── schema.yml          # 24 pengujian kualitas data
│   └── tests/
│       └── assert_valid_coordinates.sql  # Validasi batas koordinat
├── Cleaned_tech_layoffs.csv        # Dataset utama (1.745 baris)
└── layoffs_location_with_coordinates.csv  # Dataset koordinat (243 lokasi)
```
