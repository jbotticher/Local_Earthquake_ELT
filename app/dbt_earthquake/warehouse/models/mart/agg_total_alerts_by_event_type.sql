SELECT
    f.event_type,
    COUNT(*) AS total_alerts
FROM
    {{ ref('fact_earthquake') }} f
WHERE
    f.alert_level IS NOT NULL
GROUP BY
    f.event_type
ORDER BY
    total_alerts DESC
