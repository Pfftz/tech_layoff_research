WITH src AS (
    SELECT DISTINCT 
        location_hq,
        country,
        continent,
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude
    FROM "tech_layoffs_dw"."public_silver"."layoffs_standardized"
    WHERE location_hq IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY location_hq, country) AS location_id,
    location_hq AS city,
    country AS country,
    continent AS continent,
    latitude,
    longitude
FROM src