
  
    

  create  table "tech_layoffs_dw"."public_gold"."v_komparasi_indo_global__dbt_tmp"
  
  
    as
  
  (
    

WITH standardized AS (
    SELECT *
    FROM "tech_layoffs_dw"."public_silver"."layoffs_standardized"
),
base AS (
    SELECT
        company_name,
        industry_clean AS industry,
        EXTRACT(YEAR FROM date_layoffs)::INT AS year,
        EXTRACT(QUARTER FROM date_layoffs)::INT AS quarter,
        CASE
            WHEN country = 'Indonesia' THEN 'Indonesia'
            ELSE 'Global'
        END AS region_scope,
        COALESCE(laid_off_count, 0) AS laid_off_count
    FROM standardized
    WHERE date_layoffs IS NOT NULL
      AND industry_clean IS NOT NULL
)

SELECT
    year,
    quarter,
    industry,
    region_scope,
    COUNT(DISTINCT company_name) AS total_companies_impacted,
    SUM(laid_off_count) AS total_layoffs
FROM base
GROUP BY
    year,
    quarter,
    industry,
    region_scope
  );
  