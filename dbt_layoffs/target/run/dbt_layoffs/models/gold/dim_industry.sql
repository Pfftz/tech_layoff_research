
  
    

  create  table "tech_layoffs_dw"."public_gold"."dim_industry__dbt_tmp"
  
  
    as
  
  (
    WITH src AS (
    SELECT DISTINCT industry_clean
    FROM "tech_layoffs_dw"."public_silver"."layoffs_standardized"
    WHERE industry_clean IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY industry_clean) AS industry_id,
    industry_clean AS industry_name
FROM src
  );
  