-- tests/assert_no_orphan_reviews.sql
-- Reviews must have a matching business in Gold dim_business
select
    r.review_id,
    r.business_id,
    'orphan review — no dim_business match' as failure_reason
from {{ ref('fact_reviews') }} r
left join {{ ref('dim_business') }} b
    on r.business_id = b.business_id
   and r.review_date >= b.valid_from
   and r.review_date  < b.valid_to
where b.business_id is null
