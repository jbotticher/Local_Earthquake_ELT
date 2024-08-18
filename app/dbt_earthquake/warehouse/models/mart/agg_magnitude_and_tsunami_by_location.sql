SELECT
    l.location_id,
    l.location,
    SUM(f.tsunami) AS tsunami_count,
    AVG(COALESCE(TRY_CAST(f.magnitude AS NUMBER), 0)) AS avg_magnitude
FROM
    EARTHQUAKE.DBT.FACT_EARTHQUAKE f
JOIN EARTHQUAKE.DBT.DIM_LOCATION l
    ON f.location_id = l.location_id
GROUP BY
    l.location_id,
    l.location
ORDER BY
    avg_magnitude DESC