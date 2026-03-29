{{ config(
    materialized='table'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_layoffs') }}
)

SELECT 
    company_name,
    location_hq,
    country,
    continent,
    laid_off_count,
    date_layoffs,
    layoff_percentage,
    size_before,
    size_after,
    industry,
    funding_stage,
    funds_raised_mil,
    layoff_year,
    latitude,
    longitude,
    -- Industry Normalization
    CASE 
        WHEN industry ILIKE '%Finance%' OR industry = 'Fintech' THEN 'Fintech'
        WHEN industry IN ('Transportation', 'Transportion') THEN 'Transportation'
        ELSE industry
    END AS industry_clean,
    -- Funding Stage Categorization
    CASE
        WHEN funding_stage IN ('Seed', 'Series A') THEN 'Early Stage'
        WHEN funding_stage IN ('Series B', 'Series C', 'Series D') THEN 'Growth Stage'
        WHEN funding_stage IN ('Series E', 'Series F', 'Series G', 'Series H', 'Series I', 'Series J') THEN 'Late Stage'
        WHEN funding_stage IN ('Post-IPO', 'Acquired', 'Private Equity', 'Subsidiary') THEN 'Mature/Public'
        WHEN funding_stage IS NULL OR funding_stage = 'Unknown' THEN 'Unknown'
        ELSE 'Other'
    END AS stage_group
FROM staged
