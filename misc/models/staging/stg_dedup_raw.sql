WITH flatten AS (
SELECT *
FROM {{ ref('stg_flatten_raw') }}
)

, dedup AS (
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time DESC) AS rn
FROM flatten
WHERE id IS NOT NULL AND id != ''
)

SELECT *
FROM dedup
where rn = 1