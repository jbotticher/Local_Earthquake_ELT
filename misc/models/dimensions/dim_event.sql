WITH events AS (
    SELECT DISTINCT
        id,
        magnitude,
        depth,
        event_url,
        felt_reports,
        cdi,
        mmi,
        alert_level,
        status,
        tsunami,
        significance,
        network,
        code,
        num_stations,
        min_distance,
        rms,
        gap,
        mag_type,
        event_type,
        event_title
    FROM
        {{ ref('stg_dedup_raw') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY id) AS event_id,
    id,
    depth,
    magnitude,
    event_url,
    felt_reports,
    cdi,
    mmi,
    alert_level,
    status,
    tsunami,
    significance,
    network,
    code,
    num_stations,
    min_distance,
    rms,
    gap,
    mag_type,
    event_type,
    event_title
FROM
    events
