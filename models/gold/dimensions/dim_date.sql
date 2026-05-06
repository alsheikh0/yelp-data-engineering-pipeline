-- models/gold/dimensions/dim_date.sql
-- Calendar dimension covering Yelp dataset years (2004–2022).

{{ config(materialized='table', schema='gold', file_format='delta') }}

with date_spine as (
    select explode(
        sequence(
            to_date('2004-01-01'),
            to_date('2022-12-31'),
            interval 1 day
        )
    ) as calendar_date
),

enriched as (
    select
        cast(date_format(calendar_date, 'yyyyMMdd') as int)  as date_key,
        calendar_date,
        year(calendar_date)                                  as year,
        quarter(calendar_date)                               as quarter_num,
        concat('Q', quarter(calendar_date))                  as quarter_label,
        month(calendar_date)                                 as month_num,
        date_format(calendar_date, 'MMMM')                  as month_name,
        date_format(calendar_date, 'MMM')                    as month_abbr,
        weekofyear(calendar_date)                            as week_of_year,
        date_format(calendar_date, 'EEEE')                  as day_name,
        date_format(calendar_date, 'EEE')                    as day_abbr,
        dayofweek(calendar_date)                             as day_of_week,
        dayofmonth(calendar_date)                            as day_of_month,
        dayofyear(calendar_date)                             as day_of_year,
        case when dayofweek(calendar_date) in (1,7)
             then true else false end                        as is_weekend,
        last_day(calendar_date)                              as last_day_of_month,
        date_trunc('week',    calendar_date)                 as week_start_date,
        date_trunc('month',   calendar_date)                 as month_start_date,
        date_trunc('quarter', calendar_date)                 as quarter_start_date
    from date_spine
)

select * from enriched
order by calendar_date
