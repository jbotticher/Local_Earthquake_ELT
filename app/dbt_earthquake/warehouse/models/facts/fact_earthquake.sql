SELECT
    CONCAT(l.location_id,t.time_id, e.event_id) AS earthquake_id,
    l.location_id,
    t.time_id,
    e.event_id,
    e.depth,
    f.magnitude,
    f.felt_reports,
    f.cdi,
    f.mmi,
    f.alert_level,
    f.status,
    f.tsunami,
    f.significance,
    f.network,
    f.code,
    f.num_stations,
    f.min_distance,
    f.rms,
    f.gap,
    f.mag_type,
    f.event_type,
    f.event_title,
    f.load_id,
    f.load_timestamp,
    f.batch_id,
    t.updated_timestamp,
    f.event_time


FROM
    {{ ref('stg_dedup_raw') }} f
    JOIN {{ ref('dim_location') }} l
        ON f.longitude = l.longitude
        AND f.latitude = l.latitude
        AND f.location = l.location
    JOIN {{ ref('dim_time') }} t
        ON f.event_time::timestamp = t.event_timestamp
    JOIN {{ ref('dim_event') }} e
        ON f.id = e.id
