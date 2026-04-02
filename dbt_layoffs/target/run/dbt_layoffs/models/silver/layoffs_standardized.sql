
  
    

  create  table "tech_layoffs_dw"."public_silver"."layoffs_standardized__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT *
    FROM "tech_layoffs_dw"."public"."bronze_tech_layoffs"
)

SELECT 
    "Company" AS company_name,
    "Location_HQ" AS location_hq,
    "Country" AS country,
    "Continent" AS continent,
    "Laid_Off" AS laid_off_count,
    "Date_layoffs"::DATE AS date_layoffs,
    "Percentage" AS layoff_percentage,
    "Company_Size_before_Layoffs" AS size_before,
    "Company_Size_after_layoffs" AS size_after,
    "Industry" AS industry,
    "Stage" AS funding_stage,
    "Money_Raised_in__mil" AS funds_raised_mil,
    "Year" AS layoff_year,
    "latitude" AS latitude,
    "longitude" AS longitude,
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
FROM source_data
  );
  