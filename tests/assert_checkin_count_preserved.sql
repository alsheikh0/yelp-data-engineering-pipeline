-- tests/assert_checkin_count_preserved.sql
-- After the checkin timestamp explode in Silver, every business's
-- total_checkins_for_business count should match the actual number of
-- rows in fact_checkins for that business.
-- A mismatch means the explode or OPTIMIZE dropped or duplicated rows.
select
    business_id,
    max(total_checkins_for_business) as expected_count,
    count(*) as actual_count,
    'checkin count mismatch after explode' as failure_reason
from {{ ref('fact_checkins') }}
group by business_id
having max(total_checkins_for_business) != count(*)
