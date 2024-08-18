select earthquake_id, count(*)
from {{ ref('fact_earthquake') }}
group by earthquake_id
having count(*) > 1
limit 1