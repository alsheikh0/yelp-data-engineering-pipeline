-- models/staging/stg_silver_tips.sql
-- ─────────────────────────────────────────────────────────────
-- Staging view on Silver tips Delta table.
-- Used by Gold aggregates that incorporate tip signals.
-- ─────────────────────────────────────────────────────────────

{{ config(materialized='view', schema='staging') }}

select
    business_id,
    user_id,
    tip_date,
    tip_year,
    tip_month,
    compliment_count,
    text_char_len,
    text_word_count,
    is_popular_tip,
    loaded_at

from {{ source('silver', 'tips') }}
