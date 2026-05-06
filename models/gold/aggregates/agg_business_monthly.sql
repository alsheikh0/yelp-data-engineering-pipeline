-- models/gold/aggregates/agg_business_monthly.sql
-- ─────────────────────────────────────────────────────────────
-- Monthly review + tip performance per business.
-- Window functions: MoM change, 3-month rolling avg, city rank.
-- Primary table for "which businesses improved/declined?" analysis.
-- Combines both review signals and tip engagement signals.
-- ─────────────────────────────────────────────────────────────

{{ config(materialized='table', schema='gold', file_format='delta') }}

with reviews as (
    select * from {{ ref('fact_reviews') }}
),

-- Tip signals aggregated to month grain for joining
tips_monthly as (
    select
        business_id,
        tip_year,
        tip_month,
        count(*)                                        as tip_count,
        sum(compliment_count)                           as total_tip_compliments,
        count(case when is_popular_tip then 1 end)      as popular_tip_count
    from {{ ref('fact_tips') }}
    group by business_id, tip_year, tip_month
),

monthly_base as (
    select
        r.business_id,
        r.business_name,
        r.city,
        r.state,
        r.review_year,
        r.review_month,

        count(*)                                        as review_count,
        round(avg(r.stars), 2)                          as avg_stars,
        round(avg(r.text_word_count), 0)                as avg_review_length,
        sum(r.total_votes)                              as total_votes,
        sum(r.useful)                                   as useful_votes,

        count(case when r.sentiment_proxy = 'positive'
              then 1 end)                               as positive_reviews,
        count(case when r.sentiment_proxy = 'negative'
              then 1 end)                               as negative_reviews,
        count(case when r.is_high_quality
              then 1 end)                               as high_quality_reviews,
        count(case when r.reviewer_segment = 'elite_reviewer'
              then 1 end)                               as elite_reviewer_count,

        round(100.0 * count(case when r.sentiment_proxy = 'positive'
              then 1 end) / nullif(count(*), 0), 1)    as positive_pct,

        -- Tip signals joined in
        coalesce(t.tip_count, 0)                        as tip_count,
        coalesce(t.total_tip_compliments, 0)            as total_tip_compliments,
        coalesce(t.popular_tip_count, 0)                as popular_tip_count,

        -- Combined engagement score (reviews + tips)
        count(*) + coalesce(t.tip_count, 0)             as total_engagement

    from reviews r
    left join tips_monthly t
        on  r.business_id   = t.business_id
        and r.review_year   = t.tip_year
        and r.review_month  = t.tip_month

    group by
        r.business_id, r.business_name, r.city, r.state,
        r.review_year, r.review_month,
        t.tip_count, t.total_tip_compliments, t.popular_tip_count
),

with_windows as (
    select
        *,

        -- Month-over-month review volume change
        lag(review_count, 1) over (
            partition by business_id
            order by review_year, review_month
        )                                               as prior_month_reviews,

        round(100.0 * (
            review_count - lag(review_count, 1) over (
                partition by business_id
                order by review_year, review_month
            )
        ) / nullif(lag(review_count, 1) over (
            partition by business_id
            order by review_year, review_month
        ), 0), 1)                                       as review_count_mom_pct,

        -- 3-month rolling average stars
        round(avg(avg_stars) over (
            partition by business_id
            order by review_year, review_month
            rows between 2 preceding and current row
        ), 2)                                           as rolling_3m_avg_stars,

        -- Prior month stars for MoM delta
        lag(avg_stars, 1) over (
            partition by business_id
            order by review_year, review_month
        )                                               as prior_month_avg_stars,

        -- Running cumulative review total
        sum(review_count) over (
            partition by business_id
            order by review_year, review_month
            rows between unbounded preceding and current row
        )                                               as cumulative_reviews,

        -- City rank by review volume this month
        rank() over (
            partition by city, review_year, review_month
            order by review_count desc
        )                                               as city_monthly_rank,

        -- Tip momentum: rolling 3-month tip count
        sum(tip_count) over (
            partition by business_id
            order by review_year, review_month
            rows between 2 preceding and current row
        )                                               as rolling_3m_tip_count

    from monthly_base
)

select
    *,
    round(avg_stars - prior_month_avg_stars, 2)         as stars_mom_delta,
    case
        when avg_stars > prior_month_avg_stars + 0.1 then 'improving'
        when avg_stars < prior_month_avg_stars - 0.1 then 'declining'
        else 'stable'
    end                                                 as rating_trend,
    current_timestamp()                                 as aggregated_at
from with_windows
