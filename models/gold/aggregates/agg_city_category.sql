-- models/gold/aggregates/agg_city_category.sql
-- ─────────────────────────────────────────────────────────────
-- Aggregate: review performance per city × category per year.
-- Answers: "Which city-category combos are growing fastest?"
--          "Where are Italian restaurants thriving vs declining?"
-- Uses the business_categories bridge table from Silver to get
-- one row per (business, category) before joining to reviews.
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

biz_categories as (
    -- Silver bridge table: one row per (business_id, category)
    select * from {{ source('silver', 'business_categories') }}
),

-- Join reviews to the category bridge
reviews_with_category as (
    select
        r.review_year,
        r.city,
        r.state,
        bc.category,
        r.stars,
        r.sentiment_proxy,
        r.is_high_quality,
        r.total_votes,
        r.review_id

    from reviews r
    inner join biz_categories bc
        on r.business_id = bc.business_id

    where bc.category is not null
      and bc.category != ''
),

city_cat_yearly as (
    select
        review_year,
        city,
        state,
        category,

        count(distinct review_id)                       as review_count,
        round(avg(stars), 2)                            as avg_stars,
        round(sum(total_votes), 0)                      as total_votes,

        count(case when sentiment_proxy = 'positive'
              then 1 end)                               as positive_reviews,
        count(case when sentiment_proxy = 'negative'
              then 1 end)                               as negative_reviews,
        count(case when is_high_quality
              then 1 end)                               as high_quality_reviews,

        round(100.0 * count(case when sentiment_proxy = 'positive'
              then 1 end) / nullif(count(*), 0), 1)    as positive_pct

    from reviews_with_category
    group by review_year, city, state, category
),

with_yoy as (
    select
        *,

        -- Year-over-year review volume growth
        lag(review_count, 1) over (
            partition by city, state, category
            order by review_year
        )                                               as prior_year_reviews,

        round(100.0 * (
            review_count - lag(review_count, 1) over (
                partition by city, state, category
                order by review_year
            )
        ) / nullif(lag(review_count, 1) over (
            partition by city, state, category
            order by review_year
        ), 0), 1)                                       as review_count_yoy_pct,

        -- Year-over-year rating change
        round(
            avg_stars - lag(avg_stars, 1) over (
                partition by city, state, category
                order by review_year
            ), 2
        )                                               as stars_yoy_delta,

        -- Rank of category in each city for each year
        rank() over (
            partition by review_year, city
            order by review_count desc
        )                                               as category_rank_in_city,

        -- Cumulative reviews in this city-category ever
        sum(review_count) over (
            partition by city, state, category
            order by review_year
            rows between unbounded preceding and current row
        )                                               as cumulative_reviews

    from city_cat_yearly
)

select
    *,
    case
        when review_count_yoy_pct >  20 then 'fast_growing'
        when review_count_yoy_pct >   5 then 'growing'
        when review_count_yoy_pct <  -5 then 'declining'
        when review_count_yoy_pct < -20 then 'fast_declining'
        else 'stable'
    end                                                 as growth_label,

    current_timestamp()                                 as aggregated_at
from with_yoy
order by review_year, city, review_count desc
