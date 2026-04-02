
  
    

  create  table "tech_layoffs_dw"."public_gold"."fact_layoffs__dbt_tmp"
  
  
    as
  
  (
    WITH standardized AS (
    SELECT * FROM "tech_layoffs_dw"."public_silver"."layoffs_standardized"
),
dim_company AS (
    SELECT * FROM "tech_layoffs_dw"."public_gold"."dim_company"
),
dim_industry AS (
    SELECT * FROM "tech_layoffs_dw"."public_gold"."dim_industry"
),
dim_location AS (
    SELECT * FROM "tech_layoffs_dw"."public_gold"."dim_location"
),
dim_date AS (
    SELECT * FROM "tech_layoffs_dw"."public_gold"."dim_date"
)

SELECT
    ROW_NUMBER() OVER() AS fact_id,
    c.company_id,
    i.industry_id,
    l.location_id,
    d.date_id,
    COALESCE(s.laid_off_count, 0) AS total_laid_off,
    s.layoff_percentage AS percentage,
    s.funds_raised_mil AS funds_raised
FROM standardized s
LEFT JOIN dim_company c ON c.company_name = s.company_name AND c.stage_group = s.stage_group
LEFT JOIN dim_industry i ON i.industry_name = s.industry_clean
LEFT JOIN dim_location l
    ON l.city = s.location_hq
    AND l.country = s.country
    AND l.latitude = CAST(s.latitude AS NUMERIC)
    AND l.longitude = CAST(s.longitude AS NUMERIC)
LEFT JOIN dim_date d ON d.full_date = s.date_layoffs
  );
  