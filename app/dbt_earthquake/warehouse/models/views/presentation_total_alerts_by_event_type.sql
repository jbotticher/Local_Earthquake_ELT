SELECT *
FROM {{ ref('agg_total_alerts_by_event_type') }}