WITH base AS (
    SELECT
        t.updated_timestamp::date AS date,
        COUNT(*) AS earthquake_count
    FROM
        {{ ref('fact_earthquake') }} f
    JOIN {{ ref('dim_time') }} t
        ON f.time_id = t.time_id
    GROUP BY
        t.updated_timestamp::date
)

SELECT
    date_trunc('month', date) AS month,
    SUM(earthquake_count) AS total_earthquake_count
FROM
    base
GROUP BY
    month
ORDER BY
    month
