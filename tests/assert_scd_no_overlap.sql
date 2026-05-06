-- tests/assert_scd_no_overlap.sql
-- For SCD Type 2 correctness: at any point in time, a business_id
-- must have at most ONE active row (is_current = true).
-- If this query returns rows, the MERGE logic has a bug.
select
    business_id,
    count(*) as current_version_count,
    'multiple current rows for same business_id' as failure_reason
from {{ ref('dim_business') }}
where is_current = true
group by business_id
having count(*) > 1
