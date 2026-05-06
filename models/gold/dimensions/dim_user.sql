-- ═══════════════════════════════════════════════════════════════
-- models/gold/dimensions/dim_user.sql
-- SCD Type 2 user dimension.
-- Tracks user_tier changes over time via valid_from/valid_to.
-- ═══════════════════════════════════════════════════════════════

{{ config(
    materialized     = 'table',
    schema           = 'gold',
    file_format      = 'delta',
    on_schema_change = 'sync_all_columns'
) }}

with silver as (
    select * from {{ ref('stg_silver_users') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['user_id', 'valid_from']
        ) }}                                    as user_key,

        user_id,
        user_name,
        member_since,
        year(member_since)                      as member_join_year,

        review_count,
        average_stars,
        fans,
        friend_count,
        total_compliments,
        votes_useful_given,
        votes_funny_given,
        votes_cool_given,
        engagement_score,
        years_on_platform,

        user_tier,
        is_currently_elite,
        total_elite_years,

        case
            when user_tier = 'elite'    then 4
            when user_tier = 'veteran'  then 3
            when user_tier = 'active'   then 2
            else 1
        end                                     as tier_rank,

        valid_from,
        valid_to,
        is_current,
        change_hash,
        loaded_at

    from silver
)

select * from enriched
