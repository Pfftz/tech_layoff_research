-- Singular test to ensure latitude and longitude are within acceptable bounds
-- Latitude must be between -90 and 90
-- Longitude must be between -180 and 180

SELECT *
FROM "tech_layoffs_dw"."public_gold"."dim_location"
WHERE 
    (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
    OR 
    (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))