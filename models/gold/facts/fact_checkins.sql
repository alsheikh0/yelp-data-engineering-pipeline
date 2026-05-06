-- models/gold/facts/fact_checkins.sql
-- ─────────────────────────────────────────────────────────────
-- Checkin event-grain fact table.
-- One row = one individual checkin event at one business.
-- The Silver layer already exploded the comma-separated timestamps,
-- so this is mostly a join to dim_business for enrichment.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'table',
        schema           = 'gold',
        file_format      = 'delta',
        partition_by     = 'checkin_year',
        on_schema_change = 'sync_all_columns'
    )
}}

with checkins as (
    select * from {{ ref('stg_silver_checkins') }}
),

dim_biz as (
    select
        business_id, business_key, business_name,
        city, state, price_tier, status,
        valid_from, valid_to
    from {{ ref('dim_business') }}
),

checkins_enriched as (
    select
        c.*,
        b.business_key,
        b.business_name,
        b.city,
        b.state,
        b.price_tier,
        b.status as business_status

    from checkins c
    left join dim_biz b
        on  c.business_id  = b.business_id
        and c.checkin_at  >= b.valid_from
        and c.checkin_at   < b.valid_to
)

select
    {{ dbt_utils.generate_surrogate_key(
        ['business_id', 'checkin_at']
    ) }}                                as checkin_id,

    business_key,
    business_id,
    business_name,
    city,
    state,
    price_tier,
    business_status,

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
    current_timestamp() as gold_loaded_at

from checkins_enriched
