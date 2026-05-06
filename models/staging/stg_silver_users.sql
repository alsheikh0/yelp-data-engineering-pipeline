-- models/staging/stg_silver_users.sql
-- ─────────────────────────────────────────────────────────────
-- Staging view on Silver users Delta table.
-- Exposes all columns needed by dim_user.
-- ─────────────────────────────────────────────────────────────

{{ config(materialized='view', schema='staging') }}

select
    user_id,
    user_name,
    member_since,

    -- engagement metrics (SCD-tracked)
    review_count,
    average_stars,
    fans,
    friend_count,
    total_compliments,
    votes_useful_given,
    votes_funny_given,
    votes_cool_given,
    engagement_score,

    -- derived tier (SCD-tracked)
    user_tier,
    is_currently_elite,
    total_elite_years,
    years_on_platform,

    -- SCD metadata
    valid_from,
    valid_to,
    is_current,
    change_hash,
    loaded_at

from {{ source('silver', 'users') }}
