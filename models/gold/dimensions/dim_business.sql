-- models/gold/dimensions/dim_business.sql
-- ─────────────────────────────────────────────────────────────
-- SCD Type 2 business dimension. One row per (business_id, version).
-- Every time a business changes rating, opens/closes, relocates, or
-- changes category — a new row is inserted with a new valid_from,
-- and the old row's valid_to is set. All handled by the MERGE in Silver.
-- This model shapes Silver into the canonical Gold dimension.
--
-- KEY POINT — surrogate key:
--   business_key = hash(business_id + valid_from)
--   This is UNIQUE per version, not per business.
--   Fact tables reference business_key (not business_id) to pin a review
--   to the exact version of the business that existed at review time.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized        = 'table',
        schema              = 'gold',
        file_format         = 'delta',
        on_schema_change    = 'sync_all_columns'
    )
}}

with silver as (
    select * from {{ ref('stg_silver_business') }}
),

enriched as (
    select
        -- ── surrogate key (unique per version) ────────────────────────────
        {{ dbt_utils.generate_surrogate_key(
            ['business_id', 'valid_from']
        ) }}                                        as business_key,

        -- ── natural key ───────────────────────────────────────────────────
        business_id,

        -- ── core descriptive attributes ───────────────────────────────────
        business_name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,

        -- ── SCD-tracked attributes (change triggers new version) ──────────
        stars,
        review_count,
        is_open,
        categories,
        category_count,
        price_range,

        -- ── flattened attribute fields ────────────────────────────────────
        wifi,
        outdoor_seating,
        has_tv,
        delivery,
        takeout,
        reservations,
        good_for_kids,
        alcohol,
        noise_level,
        attire,

        -- ── operating hours (flattened from hours struct) ─────────────────
        mon_open,  mon_close,
        tue_open,  tue_close,
        wed_open,  wed_close,
        thu_open,  thu_close,
        fri_open,  fri_close,
        sat_open,  sat_close,
        sun_open,  sun_close,

        -- ── derived classification columns ────────────────────────────────
        case
            when is_open then 'open'
            else 'closed'
        end                                         as status,

        case
            when price_range = 1 then 'budget'
            when price_range = 2 then 'moderate'
            when price_range = 3 then 'upscale'
            when price_range = 4 then 'fine_dining'
            else 'unknown'
        end                                         as price_tier,

        -- Weekend hours coverage flag
        case
            when (sat_open is not null or sun_open is not null)
            then true else false
        end                                         as is_open_weekends,

        -- Full-service flag (delivery + takeout + reservations)
        case
            when (delivery = true and takeout = true and reservations = true)
            then true else false
        end                                         as is_full_service,

        -- Rating tier for easy dashboard bucketing
        case
            when stars >= 4.5 then 'exceptional'
            when stars >= 4.0 then 'great'
            when stars >= 3.5 then 'good'
            when stars >= 3.0 then 'average'
            else 'below_average'
        end                                         as rating_tier,

        -- Size indicator based on review volume
        case
            when review_count >= 1000 then 'very_popular'
            when review_count >= 200  then 'popular'
            when review_count >= 50   then 'established'
            when review_count >= 10   then 'growing'
            else 'new'
        end                                         as popularity_tier,

        -- ── SCD Type 2 metadata ───────────────────────────────────────────
        valid_from,
        valid_to,
        is_current,
        change_hash,
        loaded_at

    from silver
)

select * from enriched
