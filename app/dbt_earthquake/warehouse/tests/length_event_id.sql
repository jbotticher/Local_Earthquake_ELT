select *
from {{ ref('dim_event') }}
where length(event_id) > '6'
limit 1