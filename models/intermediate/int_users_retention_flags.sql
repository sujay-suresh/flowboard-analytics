{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Retention flags — pre-computed via scripts/compute_retention.py
    due to 2M event × 8K user aggregation performance.

    Grain: user_id × retention_day (1, 7, 14, 30, 60, 90).
    is_retained = 1 if user had at least one event in the retention window.

    Wraps the pre-computed table for dbt lineage visibility.
*/

select * from {{ source('app', 'int_users_retention_flags') }}
