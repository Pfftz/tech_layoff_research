

WITH raw_data AS (
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
    "longitude" AS longitude
FROM raw_data