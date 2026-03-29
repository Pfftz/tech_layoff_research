WITH src AS (
    SELECT DISTINCT 
        location_hq,
        country,
        continent,
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude
    FROM {{ ref('layoffs_standardized') }}
    WHERE location_hq IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY location_hq, country) AS location_id,
    location_hq AS city,
    country AS country,
    continent AS continent,
    latitude,
    longitude,
    -- PostGIS geospatial column geometry (Point, 4326)
    CASE 
        WHEN longitude IS NOT NULL AND latitude IS NOT NULL 
        THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        ELSE NULL
    END AS geom
FROM src
