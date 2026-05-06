-- tests/assert_no_orphan_tips.sql
-- Tips must have a matching business in Gold dim_business
-- using the point-in-time join condition.
select
    t.tip_id,
    t.business_id,
    t.tip_date,
    'orphan tip — no dim_business match at tip_date' as failure_reason
from {{ ref('fact_tips') }} t
left join {{ ref('dim_business') }} b
    on  t.business_id = b.business_id
    and t.tip_date   >= b.valid_from
    and t.tip_date    < b.valid_to
where b.business_id is null
