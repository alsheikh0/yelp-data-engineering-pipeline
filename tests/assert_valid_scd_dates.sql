-- tests/assert_valid_scd_dates.sql
-- For every SCD Type 2 row in dim_business:
-- valid_from must be strictly less than valid_to.
-- A row where valid_from >= valid_to would be a MERGE bug.
select
    business_key,
    business_id,
    valid_from,
    valid_to,
    'invalid SCD window: valid_from >= valid_to' as failure_reason
from {{ ref('dim_business') }}
where valid_from >= valid_to
