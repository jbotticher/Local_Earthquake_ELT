select *
from {{ ref('dim_location') }}
where length(location_id) > '6'
limit 1