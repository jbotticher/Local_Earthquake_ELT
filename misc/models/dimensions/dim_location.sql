WITH locations AS (
    SELECT DISTINCT
        longitude,
        latitude,
        location
    FROM
        {{ ref('stg_dedup_raw') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY longitude, latitude) AS location_id,
    longitude,
    latitude,
    location
FROM
    locations
