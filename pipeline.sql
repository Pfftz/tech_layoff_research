-- Step 0: Ensure schemas exist
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Phase 1: Data Audit & Silver Layer
DROP TABLE IF EXISTS silver.layoffs_standardized CASCADE;

CREATE TABLE silver.layoffs_standardized AS
SELECT 
    "Company",
    "Location_HQ",
    "Country",
    "Continent",
    "Laid_Off",
    "Date_layoffs"::DATE AS Date_layoffs,
    "Percentage",
    "Company_Size_before_Layoffs",
    "Company_Size_after_layoffs",
    "Industry",
    "Stage",
    "Money_Raised_in__mil",
    "Year",
    "latitude",
    "longitude",
    -- Industry Normalization
    CASE 
        WHEN "Industry" ILIKE '%Finance%' OR "Industry" = 'Fintech' THEN 'Fintech'
        WHEN "Industry" IN ('Transportation', 'Transportion') THEN 'Transportation'
        ELSE "Industry" 
    END AS industry_clean,
    -- Funding Stage Categorization
    CASE 
        WHEN "Stage" IN ('Seed', 'Series A') THEN 'Early Stage'
        WHEN "Stage" IN ('Series B', 'Series C', 'Series D') THEN 'Growth Stage'
        WHEN "Stage" IN ('Series E', 'Series F', 'Series G', 'Series H', 'Series I', 'Series J') THEN 'Late Stage'
        WHEN "Stage" IN ('Post-IPO', 'Acquired', 'Private Equity', 'Subsidiary') THEN 'Mature/Public'
        WHEN "Stage" IS NULL OR "Stage" = 'Unknown' THEN 'Unknown'
        ELSE 'Other'
    END AS stage_group
FROM public.bronze_tech_layoffs;

-- Phase 2: Dimensional Modeling (Gold Layer)

-- 1. dim_company
DROP TABLE IF EXISTS gold.dim_company CASCADE;
CREATE TABLE gold.dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    stage_group VARCHAR(100)
);

INSERT INTO gold.dim_company (company_name, stage_group)
SELECT DISTINCT "Company", stage_group 
FROM silver.layoffs_standardized 
WHERE "Company" IS NOT NULL;

-- 2. dim_industry
DROP TABLE IF EXISTS gold.dim_industry CASCADE;
CREATE TABLE gold.dim_industry (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(255)
);

INSERT INTO gold.dim_industry (industry_name)
SELECT DISTINCT industry_clean 
FROM silver.layoffs_standardized 
WHERE industry_clean IS NOT NULL;

-- 3. dim_location
DROP TABLE IF EXISTS gold.dim_location CASCADE;
CREATE TABLE gold.dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    country VARCHAR(255),
    continent VARCHAR(255),
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO gold.dim_location (city, country, continent, latitude, longitude)
SELECT DISTINCT "Location_HQ", "Country", "Continent", 
       CAST("latitude" AS NUMERIC), CAST("longitude" AS NUMERIC) 
FROM silver.layoffs_standardized 
WHERE "Location_HQ" IS NOT NULL;

-- 4. dim_date
DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);

INSERT INTO gold.dim_date (full_date, day, month, quarter, year)
SELECT DISTINCT 
    Date_layoffs,
    EXTRACT(DAY FROM Date_layoffs),
    EXTRACT(MONTH FROM Date_layoffs),
    EXTRACT(QUARTER FROM Date_layoffs),
    EXTRACT(YEAR FROM Date_layoffs)
FROM silver.layoffs_standardized
WHERE Date_layoffs IS NOT NULL;

-- 5. fact_layoffs
DROP TABLE IF EXISTS gold.fact_layoffs CASCADE;
CREATE TABLE gold.fact_layoffs (
    fact_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES gold.dim_company(company_id),
    industry_id INT REFERENCES gold.dim_industry(industry_id),
    location_id INT REFERENCES gold.dim_location(location_id),
    date_id INT REFERENCES gold.dim_date(date_id),
    total_laid_off NUMERIC,
    percentage NUMERIC,
    funds_raised NUMERIC
);

INSERT INTO gold.fact_layoffs (company_id, industry_id, location_id, date_id, total_laid_off, percentage, funds_raised)
SELECT 
    c.company_id,
    i.industry_id,
    l.location_id,
    d.date_id,
    COALESCE(s."Laid_Off", 0) AS total_laid_off,  -- Handle Nulls treating as 0
    s."Percentage",
    s."Money_Raised_in__mil" AS funds_raised
FROM silver.layoffs_standardized s
LEFT JOIN gold.dim_company c ON c.company_name = s."Company" AND c.stage_group = s.stage_group
LEFT JOIN gold.dim_industry i ON i.industry_name = s.industry_clean
LEFT JOIN gold.dim_location l 
    ON l.city = s."Location_HQ" 
    AND l.country = s."Country"
    AND l.latitude = CAST(s."latitude" AS NUMERIC)
    AND l.longitude = CAST(s."longitude" AS NUMERIC)
LEFT JOIN gold.dim_date d ON d.full_date = s.Date_layoffs;

-- Phase 3: Validation Check
-- Output comparison for the Validation phase
-- This will be checked in python later
