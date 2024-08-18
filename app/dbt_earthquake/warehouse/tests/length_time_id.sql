select *
from {{ ref('dim_time') }}
where length(time_id) > '6'
limit 1