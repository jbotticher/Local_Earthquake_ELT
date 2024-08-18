SELECT *
FROM {{ ref('agg_total_earthquakes_by_alert_level') }}