WITH times AS (
    SELECT DISTINCT
        event_time::timestamp AS event_timestamp,
        updated_time::timestamp AS updated_timestamp,
    FROM
        {{ ref('stg_dedup_raw') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY event_timestamp) AS time_id,
    event_timestamp,
    updated_timestamp
FROM
    times
