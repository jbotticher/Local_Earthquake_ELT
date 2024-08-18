select id, updated_time
from {{ ref('stg_dedup_raw') }}
group by id, updated_time
having count(id)>1
limit 1