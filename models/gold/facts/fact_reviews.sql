-- models/gold/facts/fact_reviews.sql
-- ─────────────────────────────────────────────────────────────
-- Review-grain fact table. One row = one review.
-- Joins to dim_business and dim_user using SCD Type 2 point-in-time logic:
-- we pick the dimension version that was CURRENT at the time of the review.
-- This is the correct way to handle SCD Type 2 in a fact table —
-- a 2018 review should see the 2018 version of a business, not the 2022 version.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized        = 'table',
        schema              = 'gold',
        file_format         = 'delta',
        partition_by        = 'review_year',
        on_schema_change    = 'sync_all_columns'
    )
}}

with reviews as (
    select * from {{ ref('stg_silver_reviews') }}
),

-- Point-in-time join to business: pick the business version
-- that was active (valid_from <= review_date <= valid_to) when the review happened.
-- For businesses with only one version (most), this is just is_current=true.
dim_biz as (
    select * from {{ ref('dim_business') }}
),

dim_usr as (
    select * from {{ ref('dim_user') }}
),

-- SCD Type 2 point-in-time lookup for business
reviews_with_biz as (
    select
        r.*,
        b.business_key,
        b.business_name,
        b.city,
        b.state,
        b.stars             as business_stars_at_review_time,
        b.price_tier,
        b.status            as business_status_at_review_time,
        b.category_count

    from reviews r
    left join dim_biz b
        on  r.business_id      = b.business_id
        and r.review_date     >= b.valid_from
        and r.review_date      < b.valid_to
),

-- SCD Type 2 point-in-time lookup for user
reviews_with_usr as (
    select
        rb.*,
        u.user_key,
        u.user_tier         as user_tier_at_review_time,
        u.is_currently_elite as user_was_elite_at_review_time,
        u.review_count      as user_total_reviews_at_review_time,
        u.engagement_score  as user_engagement_at_review_time

    from reviews_with_biz rb
    left join dim_usr u
        on  rb.user_id         = u.user_id
        and rb.review_date    >= u.valid_from
        and rb.review_date     < u.valid_to
),

final as (
    select
        review_id,
        business_key,
        user_key,

        review_date,
        review_year,
        review_month,

        stars,
        useful,
        funny,
        cool,
        total_votes,

        text_char_len,
        text_word_count,
        sentiment_proxy,
        is_high_quality,

        business_id,
        business_name,
        city,
        state,
        business_stars_at_review_time,
        price_tier,
        business_status_at_review_time,
        category_count,

        user_id,
        user_tier_at_review_time,
        user_was_elite_at_review_time,
        user_total_reviews_at_review_time,
        user_engagement_at_review_time,

        case
            when stars >= 4 and text_word_count >= 50 then 'detailed_positive'
            when stars >= 4 and text_word_count <  50 then 'brief_positive'
            when stars <= 2 and text_word_count >= 50 then 'detailed_negative'
            when stars <= 2 and text_word_count <  50 then 'brief_negative'
            else 'neutral'
        end                                             as review_type,

        case
            when user_was_elite_at_review_time then 'elite_reviewer'
            when user_tier_at_review_time = 'veteran' then 'veteran_reviewer'
            else 'standard_reviewer'
        end                                             as reviewer_segment,

        loaded_at,
        current_timestamp()                             as gold_loaded_at

    from reviews_with_usr
)

select * from final
