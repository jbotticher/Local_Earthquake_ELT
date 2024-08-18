SELECT
    f.alert_level,
    COUNT(*) AS total_earthquakes
FROM
    {{ ref('fact_earthquake') }} f
GROUP BY
    f.alert_level
ORDER BY
    total_earthquakes DESC
