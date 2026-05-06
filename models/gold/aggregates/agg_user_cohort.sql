-- models/gold/aggregates/agg_user_cohort.sql
-- ─────────────────────────────────────────────────────────────
-- Aggregate: user cohort analysis by member join year.
-- Answers: "Do users who joined in 2015 write more reviews
--           per year than those who joined in 2019?"
--          "Which cohort has the highest elite conversion rate?"
--          "How does review quality evolve as users age on platform?"
--
-- Uses a cohort grain: (join_year, years_since_joining)
-- This lets you plot retention / engagement curves per cohort.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        schema       = 'gold',
        file_format  = 'delta'
    )
}}

with reviews as (
    select * from {{ ref('fact_reviews') }}
),

dim_usr as (
    select
        user_id,
        user_key,
        member_since,
        member_join_year,
        user_tier,
        is_currently_elite,
        total_elite_years,
        years_on_platform,
        engagement_score,
        review_count    as total_lifetime_reviews,
        average_stars   as current_avg_stars,
        fans            as current_fans
    from {{ ref('dim_user') }}
    where is_current = true   -- only current snapshot of each user
),

-- Join reviews to user dim to get cohort (join year)
reviews_with_cohort as (
    select
        r.review_id,
        r.review_year,
        r.stars,
        r.is_high_quality,
        r.sentiment_proxy,
        r.text_word_count,
        r.total_votes,
        u.member_join_year              as cohort_year,
        u.user_id,
        u.user_tier,
        u.is_currently_elite,
        (r.review_year - u.member_join_year) as years_since_joining

    from reviews r
    inner join dim_usr u
        on r.user_id = u.user_id

    where u.member_join_year is not null
      and r.review_year >= u.member_join_year
),

-- Cohort × activity year aggregation
cohort_yearly as (
    select
        cohort_year,
        review_year,
        years_since_joining,

        count(distinct user_id)                         as active_users,
        count(distinct review_id)                       as total_reviews,
        round(count(distinct review_id) * 1.0
              / nullif(count(distinct user_id), 0), 2)  as reviews_per_active_user,

        round(avg(stars), 2)                            as avg_stars,
        round(avg(text_word_count), 0)                  as avg_review_length,

        count(case when is_high_quality then 1 end)     as high_quality_reviews,
        round(100.0 * count(case when is_high_quality
              then 1 end) / nullif(count(*), 0), 1)     as high_quality_pct,

        count(case when sentiment_proxy = 'positive'
              then 1 end)                               as positive_reviews,
        round(100.0 * count(case when sentiment_proxy = 'positive'
              then 1 end) / nullif(count(*), 0), 1)     as positive_pct,

        count(case when user_tier = 'elite'
              then 1 end)                               as elite_user_reviews,
        round(100.0 * count(case when user_tier = 'elite'
              then 1 end) / nullif(count(distinct user_id), 0), 1)
                                                        as elite_user_pct

    from reviews_with_cohort
    group by cohort_year, review_year, years_since_joining
),

-- Cohort-level summary stats (first year baseline)
cohort_baseline as (
    select
        cohort_year,
        first_value(active_users) over (
            partition by cohort_year
            order by years_since_joining
        )                                               as cohort_size_year_0,
        first_value(avg_stars) over (
            partition by cohort_year
            order by years_since_joining
        )                                               as baseline_avg_stars

    from cohort_yearly
    where years_since_joining = 0
),

with_retention as (
    select
        cy.*,
        cb.cohort_size_year_0,
        cb.baseline_avg_stars,

        round(100.0 * cy.active_users
              / nullif(cb.cohort_size_year_0, 0), 1)   as retention_pct,

        round(cy.avg_stars - cb.baseline_avg_stars, 2)  as stars_vs_baseline

    from cohort_yearly cy
    left join cohort_baseline cb using (cohort_year)
)

select
    *,
    current_timestamp()                                 as aggregated_at
from with_retention
order by cohort_year, years_since_joining
