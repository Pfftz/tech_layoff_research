WITH src AS (
    SELECT DISTINCT 
        company_name,
        stage_group
    FROM {{ ref('layoffs_standardized') }}
    WHERE company_name IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY company_name, stage_group) AS company_id,
    company_name,
    stage_group
FROM src
