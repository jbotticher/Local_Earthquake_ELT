WITH depth_ranges AS (
    SELECT
        COALESCE(depth, 0) AS depth,
        CASE
            WHEN depth < 10 THEN 'Shallow'
            WHEN depth BETWEEN 10 AND 70 THEN 'Intermediate'
            ELSE 'Deep'
        END AS depth_range
    FROM
        {{ ref('fact_earthquake') }}
)

SELECT
    depth_range,
    COUNT(*) AS num_earthquakes
FROM
    depth_ranges
GROUP BY
    depth_range
ORDER BY
    depth_range
