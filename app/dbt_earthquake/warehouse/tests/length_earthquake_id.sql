select *
from {{ ref('fact_earthquake') }}
where length(time_id) > '18'
limit 1