-- models/staging/stg_silver_business.sql
-- ─────────────────────────────────────────────────────────────
-- Staging view on the Silver business Delta table.
-- Zero compute cost (view only). Aliases raw column names and
-- documents which columns are SCD-tracked vs static.
-- dim_business reads from this staging view, not directly from Silver.
-- ─────────────────────────────────────────────────────────────

{{ config(materialized='view', schema='staging') }}

select
    -- natural key
    business_id,
    business_name,

    -- location (static — changes rarely, tracked in SCD)
    address,
    city,
    state,
    postal_code,
    latitude,
    longitude,

    -- SCD-tracked attributes
    stars,
    review_count,
    is_open,
    categories,
    category_count,
    price_range,

    -- flattened attributes
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

    -- flattened hours
    mon_open,  mon_close,
    tue_open,  tue_close,
    wed_open,  wed_close,
    thu_open,  thu_close,
    fri_open,  fri_close,
    sat_open,  sat_close,
    sun_open,  sun_close,

    -- SCD metadata
    valid_from,
    valid_to,
    is_current,
    change_hash,
    loaded_at

from {{ source('silver', 'business') }}
