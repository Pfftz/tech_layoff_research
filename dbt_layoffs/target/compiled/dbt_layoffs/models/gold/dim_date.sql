WITH src AS (
    SELECT DISTINCT date_layoffs
    FROM "tech_layoffs_dw"."public_silver"."layoffs_standardized"
    WHERE date_layoffs IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY date_layoffs) AS date_id,
    date_layoffs AS full_date,
    EXTRACT(DAY FROM date_layoffs) AS day,
    EXTRACT(MONTH FROM date_layoffs) AS month,
    EXTRACT(QUARTER FROM date_layoffs) AS quarter,
    EXTRACT(YEAR FROM date_layoffs) AS year
FROM src