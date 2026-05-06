-- macros/generate_scd_hash.sql
-- ─────────────────────────────────────────────────────────────
-- Generates a SHA-256 hash from a list of column names.
-- Used in PySpark SCD MERGE notebooks to detect row changes.
-- Included here as documentation of the hashing convention —
-- must match the Python logic in the Databricks notebooks.
--
-- Usage in dbt (for validation queries):
--   {{ generate_scd_hash(['stars', 'review_count', 'is_open']) }}
-- ─────────────────────────────────────────────────────────────

{% macro generate_scd_hash(columns) %}
    sha2(concat_ws('|', {% for col in columns %}cast({{ col }} as string)
        {%- if not loop.last %}, {% endif %}
    {% endfor %}), 256)
{% endmacro %}


-- macros/point_in_time_join.sql
-- ─────────────────────────────────────────────────────────────
-- Documents the SCD Type 2 point-in-time join pattern used
-- across fact models. The fact's event_date must fall within
-- the dimension's valid_from ≤ event_date < valid_to window.
--
-- This is the critical correctness constraint for SCD Type 2:
-- a 2018 review must join to the 2018 version of dim_business,
-- not the current 2022 version.
--
-- Usage:
--   {{ point_in_time_join('r.review_date', 'b.valid_from', 'b.valid_to') }}
-- ─────────────────────────────────────────────────────────────

{% macro point_in_time_join(event_ts, valid_from, valid_to) %}
    {{ event_ts }} >= {{ valid_from }}
    and {{ event_ts }} < {{ valid_to }}
{% endmacro %}
