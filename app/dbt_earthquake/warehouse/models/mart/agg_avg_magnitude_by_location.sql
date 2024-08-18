WITH monthly_earthquakes AS (
    SELECT
        l.location_id,
        t.time_id,
        COUNT(*) AS total_earthquakes,
        AVG(COALESCE(TRY_CAST(f.magnitude AS NUMBER), 0)) AS avg_magnitude
    FROM
        {{ ref('fact_earthquake') }} f
    JOIN {{ ref('dim_location') }} l
        ON f.location_id = l.location_id
    JOIN {{ ref('dim_time') }} t
        ON f.event_time::timestamp = t.event_timestamp 
    GROUP BY
        l.location_id,
        t.time_id
)

SELECT
    l.location_id,
    l.location,  
    t.time_id,
    t.event_timestamp AS month,
    m.total_earthquakes,
    m.avg_magnitude
FROM
    monthly_earthquakes m
JOIN {{ ref('dim_location') }} l
    ON m.location_id = l.location_id
JOIN {{ ref('dim_time') }} t
    ON m.time_id = t.time_id
