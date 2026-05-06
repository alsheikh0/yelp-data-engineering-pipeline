-- models/staging/stg_silver_checkins.sql
{{ config(materialized='view', schema='staging') }}

select
    business_id,
    checkin_at,
    checkin_date,
    checkin_year,
    checkin_month,
    checkin_hour,
    checkin_dow,
    is_weekend,
    time_of_day,
    is_peak_hour,
    total_checkins_for_business,
    loaded_at
from {{ source('silver', 'checkins') }}
