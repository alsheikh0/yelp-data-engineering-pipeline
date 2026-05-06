-- models/staging/stg_silver_reviews.sql
-- View on Silver reviews — aliases raw column names, no compute cost.

{{ config(materialized='view', schema='staging') }}

select
    review_id,
    business_id,
    user_id,
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
    loaded_at
from {{ source('silver', 'reviews') }}
