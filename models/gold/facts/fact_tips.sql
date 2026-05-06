-- models/gold/facts/fact_tips.sql
-- ─────────────────────────────────────────────────────────────
-- Tip event-grain fact table. One row = one tip left by a user
-- on a business. Tips are shorter, suggestion-style texts —
-- distinct from reviews. They carry compliment counts as a
-- proxy for how useful other users found the tip.
--
-- Uses SCD Type 2 point-in-time joins so a tip from 2018
-- reflects the 2018 state of the business and user, not current.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized        = 'table',
        schema              = 'gold',
        file_format         = 'delta',
        partition_by        = 'tip_year',
        on_schema_change    = 'sync_all_columns'
    )
}}

with tips as (
    select * from {{ ref('stg_silver_tips') }}
),

dim_biz as (
    select
        business_id,
        business_key,
        business_name,
        city,
        state,
        price_tier,
        rating_tier,
        status,
        valid_from,
        valid_to
    from {{ ref('dim_business') }}
),

dim_usr as (
    select
        user_id,
        user_key,
        user_tier,
        is_currently_elite,
        valid_from,
        valid_to
    from {{ ref('dim_user') }}
),

-- SCD Type 2 point-in-time join to business
tips_with_biz as (
    select
        t.*,
        b.business_key,
        b.business_name,
        b.city,
        b.state,
        b.price_tier,
        b.rating_tier,
        b.status as business_status_at_tip_time
    from tips t
    left join dim_biz b
        on  t.business_id  = b.business_id
        and t.tip_date    >= b.valid_from
        and t.tip_date     < b.valid_to
),

-- SCD Type 2 point-in-time join to user
tips_enriched as (
    select
        tb.*,
        u.user_key,
        u.user_tier           as user_tier_at_tip_time,
        u.is_currently_elite  as user_was_elite_at_tip_time
    from tips_with_biz tb
    left join dim_usr u
        on  tb.user_id    = u.user_id
        and tb.tip_date  >= u.valid_from
        and tb.tip_date   < u.valid_to
)

select
    {{ dbt_utils.generate_surrogate_key(
        ['business_id', 'user_id', 'tip_date']
    ) }}                                            as tip_id,

    business_key,
    user_key,

    business_id,
    business_name,
    city,
    state,
    price_tier,
    rating_tier,
    business_status_at_tip_time,

    user_id,
    user_tier_at_tip_time,
    user_was_elite_at_tip_time,

    tip_date,
    tip_year,
    tip_month,

    compliment_count,
    text_char_len,
    text_word_count,
    is_popular_tip,

    -- Tip engagement tier
    case
        when compliment_count >= 10 then 'viral'
        when compliment_count >= 5  then 'popular'
        when compliment_count >= 1  then 'engaged'
        else 'no_engagement'
    end                                             as tip_engagement_tier,

    -- Brevity signal — tips are meant to be short
    case
        when text_word_count <= 10  then 'very_brief'
        when text_word_count <= 30  then 'brief'
        when text_word_count <= 60  then 'detailed'
        else 'very_detailed'
    end                                             as tip_length_tier,

    loaded_at,
    current_timestamp()                             as gold_loaded_at

from tips_enriched
